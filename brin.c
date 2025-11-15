#include <sqlite3ext.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <time.h>   // Needed for struct tm and time conversion

SQLITE_EXTENSION_INIT1

typedef enum {
    BRIN_TYPE_INTEGER,
    BRIN_TYPE_REAL,
    BRIN_TYPE_TEXT
} BrinAffinity;


// A generic representation of a BRIN block summary
typedef struct BrinRange {

    BrinAffinity type;       // INTEGER / REAL / TEXT

    union {
        struct {             // numeric (INTEGER or REAL)
            double min;
            double max;
        } num;

        struct {             // text (lexicographic min/max)
            char *min;
            char *max;
        } txt;

    } u;

    sqlite3_int64 start_rowid;
    sqlite3_int64 end_rowid;

} BrinRange;


const char* get_affinity(const char *declared_type) {
    if (!declared_type) return "BLOB";

    char type[64];
    snprintf(type, sizeof(type), "%s", declared_type);
    for (int i = 0; type[i]; i++) {
        type[i] = toupper(type[i]);
    }

    if (strstr(type, "INT")) return "INTEGER";
    if (strstr(type, "CHAR") || strstr(type, "CLOB")
                             || strstr(type, "TEXT")) return "TEXT";
    if (strstr(type, "REAL") || strstr(type, "FLOA")
                             || strstr(type, "DOUB")) return "REAL";
    if (strstr(type, "NUMERIC") || strstr(type, "DECIMAL")
                                || strstr(type, "BOOLEAN")
                                || strstr(type, "DATE")
                                || strstr(type, "DATETIME")) return "NUMERIC";

    return "BLOB"; // default
}


/* --- Structure representing the BRIN virtual table --- */
typedef struct {
  sqlite3_vtab base;   // Base class - required by SQLite
  char *table;         // Physical table name (the one we index)
  char *column;        // Column name being indexed
  int block_size;      // Number of rows per block
  BrinAffinity affinity;
  BrinRange *ranges;      // dynamically built index ranges
  int total_blocks;
  int index_ready;
  sqlite3 *db;         // Pointer to the sqlite3 DB handle (stored at xConnect)
} BrinVtab;


/* --- Cursor structure to iterate over BRIN ranges --- */
typedef struct {
  sqlite3_vtab_cursor base;  // Base class - required by SQLite
  int current_block;         // Current block index while scanning
  sqlite3_int64 current_rowid;
  int eof;
} BrinCursor;


/* --- xConnect / xCreate --- */
static int brinConnect(
  sqlite3 *db,             // Pointer to the database connection
  void *pAux,              // pClientData passed during module registration
  int argc,                // Argument count
  const char *const*argv,  // Argument vector
  sqlite3_vtab **ppVtab,   // Pointer for the virtual table structure instance
  char **pzErr             // Output pointer for the error message (char [])
){
  (void)pAux; (void)pzErr;
  if (argc < 5) {
    fprintf(stderr, "brinConnect: not enough args (argc=%d)\n", argc);
    return SQLITE_ERROR;
  }

  fprintf(stdout, "-> Connect / xCreate\n");

  BrinVtab *v = (BrinVtab*)sqlite3_malloc(sizeof(BrinVtab));
  if (v == NULL) return SQLITE_NOMEM;
  memset(v,0,sizeof(BrinVtab));

  v->table      = sqlite3_mprintf("%s",argv[3]);
  v->column     = sqlite3_mprintf("%s",argv[4]);
  v->block_size = atoi(argv[5]);
  v->db         = db;

  // Get column affinity
  const char *dataType, *collation;
  int notNull, isPK, isAuto;

  // Get column metadata

  int rc = 1;

  rc = sqlite3_table_column_metadata(
        db,
        "main",
        v->table,
        v->column,
        &dataType,
        &collation,
        &notNull,
        &isPK,
        &isAuto
  );

  if (rc != SQLITE_OK) {
    fprintf(stderr, "Error retrieving metadata: %s\n", sqlite3_errmsg(db));
    sqlite3_free(v->table);
    sqlite3_free(v->column);
    sqlite3_free(v);
    sqlite3_close(db);
    return rc;
  }

  // ToDo: We should add a test to validate this
  const char *affinity = get_affinity(dataType);

  printf("Affinity %s\n", affinity);

  if ( strcmp(affinity, "INTEGER") == 0 ) {
    rc = sqlite3_declare_vtab(db,
           "CREATE TABLE x(min INTEGER, max INTEGER, start_rowid INT, end_rowid INT)"
    );
    v->affinity = BRIN_TYPE_INTEGER;
  }
  else if ( strcmp(affinity, "REAL") == 0 ) {
    rc = sqlite3_declare_vtab(db,
           "CREATE TABLE x(min REAL, max REAL, start_rowid INT, end_rowid INT)"
    );
    v->affinity = BRIN_TYPE_REAL;
  }
  else if ( strcmp(affinity, "TEXT") == 0 ) {
    rc = sqlite3_declare_vtab(db,
           "CREATE TABLE x(min TEXT, max TEXT, start_rowid INT, end_rowid INT)"
    );
    v->affinity = BRIN_TYPE_TEXT;
  }
  else {
    fprintf(stderr,
             "NOT SUPPORTED: %s\n", affinity);

    sqlite3_free(v->table);
    sqlite3_free(v->column);
    sqlite3_free(v);

    return SQLITE_ERROR;

  }

  if (rc != SQLITE_OK){
    fprintf(stderr,
             "brinConnect: declare_vtab failed: %s\n",
             sqlite3_errmsg(db));

    sqlite3_free(v->table);
    sqlite3_free(v->column);
    sqlite3_free(v);

    return rc;
  }

  *ppVtab = (sqlite3_vtab*)v;
  return SQLITE_OK;
}


/* ---------------------------
 * brinBuildIndex:
 * Build the in-memory BRIN ranges by scanning the base table in rowid order.
 * Assumes monotonic increasing values (so min is first in block, max is last).
 * --------------------------- */
static int brinBuildIndex(BrinVtab *v) {
    if (!v || !v->db) return SQLITE_ERROR;

    printf("[BRIN] brinBuildIndex: building index for %s.%s (block_size=%d)\n",
           v->table, v->column, v->block_size);

    /* free any previous ranges */
    if (v->ranges) {
        if (v->affinity == BRIN_TYPE_TEXT) {
            for (int i=0;i<v->total_blocks;i++){
                free(v->ranges[i].u.txt.min);
                free(v->ranges[i].u.txt.max);
            }
        }
        free(v->ranges);
        v->ranges = NULL;
        v->total_blocks = 0;
    }

    char sql[1024];
    snprintf(sql, sizeof(sql),
             "SELECT rowid, %s FROM %s ORDER BY rowid;", v->column, v->table);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("[BRIN] brinBuildIndex: prepare failed: %s\n", sqlite3_errmsg(v->db));
        return rc;
    }

    int cap = 256;
    int count = 0;
    v->ranges = (BrinRange*)calloc(cap, sizeof(BrinRange));
    if (!v->ranges) {
        sqlite3_finalize(stmt);
        return SQLITE_NOMEM;
    }

    int block_pos = 0;
    BrinRange current;
    memset(&current, 0, sizeof(current));
    current.type = v->affinity;

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        sqlite3_int64 rowid = sqlite3_column_int64(stmt, 0);

        /* start new block */
        if (block_pos == 0) {
            current.start_rowid = rowid;
            current.end_rowid = rowid;
            current.type = v->affinity;
            if (v->affinity == BRIN_TYPE_TEXT) {
                const unsigned char *txt = sqlite3_column_text(stmt, 1);
                const char *s = txt ? (const char*)txt : "";
                current.u.txt.min = strdup(s);
                current.u.txt.max = strdup(s);
            } else {
                double val = sqlite3_column_double(stmt, 1);
                current.u.num.min = val;
                current.u.num.max = val;
            }
        } else {
            /* update end_rowid and max */
            current.end_rowid = rowid;
            if (v->affinity == BRIN_TYPE_TEXT) {
                const unsigned char *txt = sqlite3_column_text(stmt, 1);
                const char *s = txt ? (const char*)txt : "";
                /* since monotonic increasing, last seen is >= previous, but keep safe */
                free(current.u.txt.max);
                current.u.txt.max = strdup(s);
            } else {
                double val = sqlite3_column_double(stmt, 1);
                if (val > current.u.num.max) current.u.num.max = val;
            }
        }

        block_pos++;

        if (block_pos >= v->block_size) {
            /* push current */
            if (count >= cap) {
                cap *= 2;
                BrinRange *tmp = realloc(v->ranges, cap * sizeof(BrinRange));
                if (!tmp) {
                    sqlite3_finalize(stmt);
                    return SQLITE_NOMEM;
                }
                v->ranges = tmp;
            }
            v->ranges[count++] = current;
            /* reset current */
            memset(&current, 0, sizeof(BrinRange));
            current.type = v->affinity;
            block_pos = 0;
        }
    }

    if (block_pos > 0) {
        /* push last partial block */
        if (count >= cap) {
            cap *= 2;
            BrinRange *tmp = realloc(v->ranges, cap * sizeof(BrinRange));
            if (!tmp) {
                sqlite3_finalize(stmt);
                return SQLITE_NOMEM;
            }
            v->ranges = tmp;
        }
        v->ranges[count++] = current;
    }

    sqlite3_finalize(stmt);

    v->total_blocks = count;
    v->index_ready = 1;
    printf("[BRIN] brinBuildIndex: built %d blocks\n", v->total_blocks);
    return SQLITE_OK;
}

/* ---------------------------
 * xBestIndex
 * - inspects available constraints and tells sqlite which ones we'll use.
 * - we use idxNum bit0 => lower bound (>=), bit1 => upper bound (<=).
 * - set aConstraintUsage[].argvIndex and .omit where appropriate.
 * - set estimatedCost / estimatedRows to influence planner to use BRIN.
 * --------------------------- */
static int brinBestIndex(sqlite3_vtab *pVTab, sqlite3_index_info *pInfo)
{
    BrinVtab *v = (BrinVtab*)pVTab;
    printf("[BRIN] xBestIndex invoked\n");

    int idxNum = 0;
    int nextArg = 1; /* argvIndex starts at 1 */

    for (int i = 0; i < pInfo->nConstraint; i++) {
        const struct sqlite3_index_constraint *c = &pInfo->aConstraint[i];
        if (!c->usable) continue;

        /* We only index the single virtual column exposed (col 0 = min, col1=max - but vtable has columns min,max,...)
           The optimizer presents constraints referencing the vtable columns; we want constraints on the real column,
           but for our simple virtual table we assume users will constrain the first visible columns. */
        /* Common pattern: expect constraint on column 0 (min) or 1 (max) */
        /* We'll accept any constraint that is EQ, GE, LE on column 0 or 1 for range searches. */
        if (c->iColumn == 0 || c->iColumn == 1) {
            if (c->op == SQLITE_INDEX_CONSTRAINT_GE) {
                /* treat as lower bound */
                pInfo->aConstraintUsage[i].argvIndex = nextArg++;
                pInfo->aConstraintUsage[i].omit = 1;
                idxNum |= 1; /* lower bound */
                printf("[BRIN]  using constraint GE on column %d\n", c->iColumn);
            } else if (c->op == SQLITE_INDEX_CONSTRAINT_LE) {
                /* treat as upper bound */
                pInfo->aConstraintUsage[i].argvIndex = nextArg++;
                pInfo->aConstraintUsage[i].omit = 1;
                idxNum |= 2; /* upper bound */
                printf("[BRIN]  using constraint LE on column %d\n", c->iColumn);
            } else if (c->op == SQLITE_INDEX_CONSTRAINT_EQ) {
                /* equality -> use as both bounds (we will receive single arg) */
                pInfo->aConstraintUsage[i].argvIndex = nextArg++;
                pInfo->aConstraintUsage[i].omit = 1;
                idxNum |= 3;
                printf("[BRIN]  using constraint EQ on column %d\n", c->iColumn);
            }
        }
    }

    pInfo->idxNum = idxNum;

    /* Estimated cost: small relative to table scan. We use number of blocks as rough cost. */
    double cost = (v->total_blocks > 0) ? (double)(v->total_blocks) : 10.0;
    pInfo->estimatedCost = cost;
    pInfo->estimatedRows = (sqlite3_int64)((v->total_blocks > 0) ? v->total_blocks : 25);

    printf("[BRIN] xBestIndex chosen idxNum=%d estimatedCost=%.2f blocks=%d\n",
           idxNum, pInfo->estimatedCost, v->total_blocks);

    return SQLITE_OK;
}

/* ---------------------------
 * xOpen / xClose
 * --------------------------- */
static int brinOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
    BrinCursor *c = (BrinCursor*)sqlite3_malloc(sizeof(BrinCursor));
    if (!c) return SQLITE_NOMEM;
    memset(c,0,sizeof(BrinCursor));
    c->current_block = 0;
    c->current_rowid = 0;
    c->eof = 0;
    *ppCursor = (sqlite3_vtab_cursor*)c;
    printf("[BRIN] xOpen\n");
    return SQLITE_OK;
}

static int brinClose(sqlite3_vtab_cursor *cur){
    BrinCursor *c = (BrinCursor*)cur;
    printf("[BRIN] xClose\n");
    sqlite3_free(c);
    return SQLITE_OK;
}

/* ---------------------------
 * xFilter
 * - Build index lazily if not yet built.
 * - idxNum bits: bit0(lower), bit1(upper)
 * - argc will contain the number of argv values passed (in order of idx usage)
 * --------------------------- */
static int brinFilter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                      int argc, sqlite3_value **argv)
{
    BrinCursor *cur = (BrinCursor*)pCursor;
    BrinVtab *v = (BrinVtab*)pCursor->pVtab;

    printf("[BRIN] xFilter invoked idxNum=%d argc=%d\n", idxNum, argc);

    /* Build the index if needed (lazy) */
    if (!v->index_ready) {
        int rc = brinBuildIndex(v);
        if (rc != SQLITE_OK) return rc;
    }

    /* Default search bounds (wide) */
    double qmin = -1e308, qmax = 1e308;
    const char *qmin_txt = NULL, *qmax_txt = NULL;

    int arg_pos = 0;
    if (idxNum & 1) { /* has lower bound */
        if (arg_pos < argc) {
            sqlite3_value *val = argv[arg_pos++];
            if (v->affinity == BRIN_TYPE_TEXT) qmin_txt = (const char*)sqlite3_value_text(val);
            else qmin = sqlite3_value_double(val);
        }
    }
    if (idxNum & 2) { /* has upper bound */
        if (arg_pos < argc) {
            sqlite3_value *val = argv[arg_pos++];
            if (v->affinity == BRIN_TYPE_TEXT) qmax_txt = (const char*)sqlite3_value_text(val);
            else qmax = sqlite3_value_double(val);
        }
    }

    /* If equality (both bits set but single arg), handle case where only 1 argv supplied */
    if ((idxNum == 3) && argc == 1) {
        /* treat argv[0] as exact value */
        if (v->affinity == BRIN_TYPE_TEXT) {
            qmin_txt = qmax_txt = (const char*)sqlite3_value_text(argv[0]);
        } else {
            qmin = qmax = sqlite3_value_double(argv[0]);
        }
    }

    printf("[BRIN] search bounds: ");
    if (v->affinity == BRIN_TYPE_TEXT)
        printf("TEXT [%s .. %s]\n", qmin_txt?qmin_txt:"(null)", qmax_txt?qmax_txt:"(null)");
    else
        printf("NUM [%.6g .. %.6g]\n", qmin, qmax);

    /* Find the first block overlapping the query range */
    int start_block = -1;
    for (int i = 0; i < v->total_blocks; i++) {
        BrinRange *r = &v->ranges[i];
        int overlap = 0;
        if (v->affinity == BRIN_TYPE_TEXT) {
            const char *minv = r->u.txt.min ? r->u.txt.min : "";
            const char *maxv = r->u.txt.max ? r->u.txt.max : "";
            const char *qmin_s = qmin_txt ? qmin_txt : "";
            const char *qmax_s = qmax_txt ? qmax_txt : "\xff\xff\xff\xff";
            if ((strcmp(maxv, qmin_s) >= 0) && (strcmp(minv, qmax_s) <= 0)) overlap = 1;
        } else {
            if ((r->u.num.max >= qmin) && (r->u.num.min <= qmax)) overlap = 1;
        }
        if (overlap) { start_block = i; break; }
    }

    if (start_block < 0) {
        /* no matching blocks */
        cur->eof = 1;
        cur->current_block = 0;
        printf("[BRIN] xFilter: no matching blocks\n");
        return SQLITE_OK;
    }

    /* position cursor at first matching block */
    cur->current_block = start_block;
    cur->current_rowid = v->ranges[start_block].start_rowid;
    cur->eof = 0;
    printf("[BRIN] xFilter: starting at block %d (rowid=%lld)\n",
           start_block, (long long)cur->current_rowid);

    return SQLITE_OK;
}

/* ---------------------------
 * xNext / xEof / xColumn / xRowid
 * --------------------------- */
static int brinNext(sqlite3_vtab_cursor *cur){
    BrinCursor *c = (BrinCursor*)cur;
    BrinVtab *v = (BrinVtab*)cur->pVtab;
    c->current_block++;
    if (c->current_block >= v->total_blocks) c->eof = 1;
    printf("[BRIN] xNext -> block=%d eof=%d\n", c->current_block, c->eof);
    return SQLITE_OK;
}

static int brinEof(sqlite3_vtab_cursor *cur){
    BrinCursor *c = (BrinCursor*)cur;
    return c->eof;
}

static int brinColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
    BrinCursor *c = (BrinCursor*)cur;
    BrinVtab *v = (BrinVtab*)cur->pVtab;
    if (c->current_block < 0 || c->current_block >= v->total_blocks) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    BrinRange *r = &v->ranges[c->current_block];

    printf("[BRIN] xColumn block=%d col=%d\n", c->current_block, i);

    switch (i) {
        case 0: /* min */
            if (v->affinity == BRIN_TYPE_TEXT)
                sqlite3_result_text(ctx, r->u.txt.min, -1, SQLITE_TRANSIENT);
            else
                sqlite3_result_double(ctx, r->u.num.min);
            break;
        case 1: /* max */
            if (v->affinity == BRIN_TYPE_TEXT)
                sqlite3_result_text(ctx, r->u.txt.max, -1, SQLITE_TRANSIENT);
            else
                sqlite3_result_double(ctx, r->u.num.max);
            break;
        case 2: /* start_rowid */
            sqlite3_result_int64(ctx, r->start_rowid);
            break;
        case 3: /* end_rowid */
            sqlite3_result_int64(ctx, r->end_rowid);
            break;
        default:
            sqlite3_result_null(ctx);
    }
    return SQLITE_OK;
}

static int brinRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
    BrinCursor *c = (BrinCursor*)cur;
    *pRowid = (sqlite_int64)c->current_block + 1; /* simple unique rowid per block */
    return SQLITE_OK;
}

/* ---------------------------
 * xDisconnect / xDestroy
 * --------------------------- */
static int brinDisconnect(sqlite3_vtab *pVTab){
    BrinVtab *v = (BrinVtab*)pVTab;
    printf("[BRIN] xDisconnect\n");

    if (v) {
        if (v->ranges) {
            if (v->affinity == BRIN_TYPE_TEXT) {
                for (int i=0;i<v->total_blocks;i++){
                    free(v->ranges[i].u.txt.min);
                    free(v->ranges[i].u.txt.max);
                }
            }
            free(v->ranges);
        }
        sqlite3_free(v->table);
        sqlite3_free(v->column);
        sqlite3_free(v);
    }
    return SQLITE_OK;
}

static int brinDestroy(sqlite3_vtab *pVTab){
    printf("[BRIN] xDestroy\n");
    return brinDisconnect(pVTab);
}

/* --- Module registration structure --- */
static sqlite3_module BrinModule = {
  2,                /* iVersion */
  brinConnect,      /* xCreate */
  brinConnect,      /* xConnect*/
  brinBestIndex,    /* xBestIndex */
  brinDisconnect,   /* xDisconnect */
  brinDisconnect,   /* xDestroy */
  brinOpen,         /* xOpen */
  brinClose,        /* xClose */
  brinFilter,       /* xFilter */
  brinNext,         /* xNext */
  brinEof,          /* xEoF */
  brinColumn,       /* xColumn */
  brinRowid,        /* xRowid */
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0
};


/* --- Entry point for .load --- */
int sqlite3_brin_init(sqlite3 *db, char **pzErrMsg,
                            const sqlite3_api_routines *pApi) {
  SQLITE_EXTENSION_INIT2(pApi);

  int rc = sqlite3_create_module(db, "brin", &BrinModule, 0);

  if( rc != 0 ) {
    printf("The module could not be created.\n");
  }

  return rc;
}

