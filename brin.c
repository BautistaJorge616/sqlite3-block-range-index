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

    BrinAffinity type;

    union {
        struct {             // INTEGER / REAL
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
    if (!declared_type) return NULL;

    char type[64];
    snprintf(type, sizeof(type), "%s", declared_type);
    for (int i = 0; type[i]; i++) {
        type[i] = toupper(type[i]);
    }

    fprintf(stdout, "->type %s\n", type);

    if (strstr(type, "INT")) return "INTEGER";
    if (strstr(type, "CHAR") || strstr(type, "CLOB")
                             || strstr(type, "TEXT")) return "TEXT";
    if (strstr(type, "REAL") || strstr(type, "FLOA")
                             || strstr(type, "DOUB")) return "REAL";
    if (strstr(type, "DATE")
                             || strstr(type, "DATETIME")) return "TEXT";

    return NULL;
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
    sqlite3_vtab_cursor base;

    int useBrin;                 /* ¿usar BRIN o no? */
    int current_block;           /* bloque actual */
    int nBlocks;                 /* total de bloques */

    sqlite3_int64 low;           /* límite inferior */
    sqlite3_int64 high;          /* límite superior */

    BrinRange *blocks;           /* rangos BRIN */

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

  if ( !affinity ) {
    fprintf(stderr,
             "NOT SUPPORTED: %s\n", dataType);

    sqlite3_free(v->table);
    sqlite3_free(v->column);
    sqlite3_free(v);

    return SQLITE_ERROR;
  }

  if ( strcmp(affinity, "INTEGER") == 0 ) {
    rc = sqlite3_declare_vtab(db,
           "CREATE TABLE x( min INTEGER, max INTEGER, "
                             "start_rowid INT, end_rowid INT)"
    );
    v->affinity = BRIN_TYPE_INTEGER;
  }
  if ( strcmp(affinity, "REAL") == 0 ) {
    rc = sqlite3_declare_vtab(db,
           "CREATE TABLE x( min REAL, max REAL, "
                            "start_rowid INT, end_rowid INT)"
    );
    v->affinity = BRIN_TYPE_REAL;
  }
  if ( strcmp(affinity, "TEXT") == 0 ) {
    rc = sqlite3_declare_vtab(db,
           "CREATE TABLE x( min TEXT, max TEXT, "
                            "start_rowid INT, end_rowid INT)"
    );
    v->affinity = BRIN_TYPE_TEXT;
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


static void brinDumpRange(const BrinRange *r, int idx) {
    if (!r) return;

    fprintf(stderr,
        "  [block %d] rowid [%lld - %lld] (%lld rows) ",
        idx,
        (long long)r->start_rowid,
        (long long)r->end_rowid,
        (long long)(r->end_rowid - r->start_rowid + 1)
    );

    if (r->type == BRIN_TYPE_INTEGER || r->type == BRIN_TYPE_REAL) {
        fprintf(stderr, "min=%.3f max=%.3f\n",
                r->u.num.min, r->u.num.max);
    } else if (r->type == BRIN_TYPE_TEXT) {
        fprintf(stderr, "min='%s' max='%s'\n",
                r->u.txt.min, r->u.txt.max);
    }
}



/* ---------------------------
 * brinBuildIndex:
 * Build the in-memory BRIN ranges by scanning the base table in rowid order.
 * Assumes monotonic increasing values (so min is first in block, max is last).
 * --------------------------- */
static int brinBuildIndex(BrinVtab *v) {
    if (!v || !v->db) return SQLITE_ERROR;

    printf("Building BRIN index\n");
    printf("Table      : %s\n", v->table);
    printf("Column     : %s\n", v->column);
    printf("Block size : %d\n", v->block_size);
    printf("Affinity   : %d\n", v->affinity);


    /* free any previous ranges */
    if (v->ranges) {
        printf("Freeing previous BRIN ranges (%d blocks)", v->total_blocks);
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
        printf("[BRIN] brinBuildIndex: prepare failed: %s\n",
                sqlite3_errmsg(v->db));
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
            printf("Starting new block at rowid=%lld \n", (long long)rowid);
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
        printf("Finalizing block %d\n", count);
        brinDumpRange(&current, count);
        v->ranges[count++] = current;
    }

    sqlite3_finalize(stmt);

    v->total_blocks = count;

    printf("Finished building BRIN index\n");
    printf("Total blocks: %d\n", v->total_blocks);

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
static int brinBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pIdxInfo) {
    BrinVtab *v = (BrinVtab*)pVtab;

    printf("[BRIN] xBestIndex invoked\n");

    int minTerm = -1;
    int maxTerm = -1;

    /* scan constraints */
    for (int i = 0; i < pIdxInfo->nConstraint; i++) {
        const struct sqlite3_index_constraint *c =
            &pIdxInfo->aConstraint[i];

        if (!c->usable)
            continue;

        /* column 0 = min, column 1 = max */
        if (c->iColumn == 0 && c->op == SQLITE_INDEX_CONSTRAINT_LE) {
            minTerm = i;
            printf("[BRIN]  using constraint LE on column min\n");
        }
        else if (c->iColumn == 1 && c->op == SQLITE_INDEX_CONSTRAINT_GE) {
            maxTerm = i;
            printf("[BRIN]  using constraint GE on column max\n");
        }
    }

    int argvIdx = 1;

    if (minTerm >= 0) {
        pIdxInfo->aConstraintUsage[minTerm].argvIndex = argvIdx++;
        pIdxInfo->aConstraintUsage[minTerm].omit = 1;
    }

    if (maxTerm >= 0) {
        pIdxInfo->aConstraintUsage[maxTerm].argvIndex = argvIdx++;
        pIdxInfo->aConstraintUsage[maxTerm].omit = 1;
    }

    /* tell xFilter what we recognized */
    if (minTerm >= 0 && maxTerm >= 0) {
        pIdxInfo->idxNum = 3; /* min + max */
    } else if (minTerm >= 0) {
        pIdxInfo->idxNum = 1; /* min only */
    } else if (maxTerm >= 0) {
        pIdxInfo->idxNum = 2; /* max only */
    } else {
        pIdxInfo->idxNum = 0; /* full scan */
    }

    /* cost model: fewer blocks = cheaper */
    int blocks = v->total_blocks > 0 ? v->total_blocks : 1000;

    pIdxInfo->estimatedRows = blocks;
    pIdxInfo->estimatedCost = (double)blocks;

    printf("[BRIN] xBestIndex chosen idxNum=%d estimatedCost=%.2f blocks=%d\n",
           pIdxInfo->idxNum, pIdxInfo->estimatedCost, blocks);

    return SQLITE_OK;
}



/* ---------------------------
 * xOpen / xClose
 * --------------------------- */
static int brinOpen(
    sqlite3_vtab *pVtab,
    sqlite3_vtab_cursor **ppCursor
){
    BrinCursor *c = (BrinCursor*)calloc(1, sizeof(BrinCursor));
    if (!c) return SQLITE_NOMEM;

    c->useBrin = 0;
    c->current_block = 0;
    c->eof = 0;

    *ppCursor = &c->base;

    printf("[BRIN] xOpen\n");
    return SQLITE_OK;
}


static int brinClose(sqlite3_vtab_cursor *cur){
    BrinCursor *c = (BrinCursor*)cur;
    printf("[BRIN] xClose\n");
    free(c);
    return SQLITE_OK;
}


/* ---------------------------
 * xFilter
 * - Build index lazily if not yet built.
 * - idxNum bits: bit0(lower), bit1(upper)
 * - argc will contain the number of argv values passed (in order of idx usage)
 * --------------------------- */
static int brinFilter(
    sqlite3_vtab_cursor *cur,
    int idxNum,
    const char *idxStr,
    int argc,
    sqlite3_value **argv
){
    BrinCursor *c = (BrinCursor*)cur;
    BrinVtab   *v = (BrinVtab*)cur->pVtab;

    printf("[BRIN] xFilter invoked idxNum=%d argc=%d\n", idxNum, argc);

    /* Default: do not use BRIN */
    c->useBrin = 0;
    c->eof = 1;

    /* We only support the (min <= ? AND max >= ?) plan */
    if (idxNum != 3 || argc != 2) {
        printf("[BRIN] xFilter: BRIN not used\n");
        return SQLITE_OK;
    }

    /* Lazy index build */
    if (!v->index_ready) {
        printf("[BRIN] building BRIN index (lazy)\n");
        brinBuildIndex(v);
    }

    c->blocks  = v->ranges;
    c->nBlocks = v->total_blocks;
    c->current_block = 0;

    /* =========================
     * INTEGER / REAL
     * ========================= */
    if (v->affinity == BRIN_TYPE_INTEGER ||
        v->affinity == BRIN_TYPE_REAL) {

        sqlite3_int64 a = sqlite3_value_int64(argv[0]); /* min <= a  => HIGH */
        sqlite3_int64 b = sqlite3_value_int64(argv[1]); /* max >= b  => LOW  */

        /* Normalize */
        c->low  = (a < b) ? a : b;
        c->high = (a < b) ? b : a;

        printf("[BRIN] search bounds NUM [%lld .. %lld]\n",
               c->low, c->high);

        /* Find first matching block */
        while (c->current_block < c->nBlocks) {
            BrinRange *r = &c->blocks[c->current_block];

            if (r->u.num.max >= c->low &&
                r->u.num.min <= c->high) {
                c->useBrin = 1;
                c->eof = 0;
                printf("[BRIN] first matching block=%d\n",
                       c->current_block);
                return SQLITE_OK;
            }

            c->current_block++;
        }
    }

    /* =========================
     * TEXT (ISO timestamps, logs)
     * ========================= */
    else if (v->affinity == BRIN_TYPE_TEXT) {

        const char *a = (const char*)sqlite3_value_text(argv[0]);
        const char *b = (const char*)sqlite3_value_text(argv[1]);

        if (!a || !b) {
            printf("[BRIN] NULL text bound\n");
            return SQLITE_OK;
        }

        /* Normalize lexicographically */
        const char *low  = (strcmp(a, b) < 0) ? a : b;
        const char *high = (strcmp(a, b) < 0) ? b : a;

        printf("[BRIN] search bounds TEXT [%s .. %s]\n", low, high);

        /* Find first matching block */
        while (c->current_block < c->nBlocks) {
            BrinRange *r = &c->blocks[c->current_block];

            /*
             * overlap test:
             *   r.max >= low  AND  r.min <= high
             */
            if (strcmp(r->u.txt.max, low) >= 0 &&
                strcmp(r->u.txt.min, high) <= 0) {

                c->useBrin = 1;
                c->eof = 0;
                printf("[BRIN] first matching block=%d\n",
                       c->current_block);
                return SQLITE_OK;
            }

            c->current_block++;
        }
    }

    printf("[BRIN] xFilter: no matching blocks\n");
    return SQLITE_OK;
}


/* ---------------------------
 * xNext / xEof / xColumn / xRowid
 * --------------------------- */
static int brinNext(sqlite3_vtab_cursor *cur){
    BrinCursor *c = (BrinCursor*)cur;

    if (c->eof) return SQLITE_OK;

    c->current_block++;

    while (c->current_block < c->nBlocks) {
        BrinRange *r = &c->blocks[c->current_block];

        /* INTEGER / REAL */
        if (r->type == BRIN_TYPE_INTEGER ||
            r->type == BRIN_TYPE_REAL) {

            if (r->u.num.max >= c->low &&
                r->u.num.min <= c->high) {
                return SQLITE_OK;
            }
        }
        /* TEXT */
        else if (r->type == BRIN_TYPE_TEXT) {
            /* already filtered in xFilter, just accept */
            return SQLITE_OK;
        }

        c->current_block++;
    }

    /* IMPORTANT: mark EOF immediately */
    c->eof = 1;
    return SQLITE_OK;
}


static int brinEof(sqlite3_vtab_cursor *cur) {
    BrinCursor *c = (BrinCursor*)cur;
    return c->eof;
}


static int brinColumn(
    sqlite3_vtab_cursor *cur,
    sqlite3_context *ctx,
    int col
){
    BrinCursor *c = (BrinCursor*)cur;

    if (c->eof || c->current_block >= c->nBlocks) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    BrinRange *r = &c->blocks[c->current_block];

    switch (col) {
        case 0:
            sqlite3_result_text(ctx, r->u.txt.min, -1, SQLITE_TRANSIENT);
            break;
        case 1:
            sqlite3_result_text(ctx, r->u.txt.max, -1, SQLITE_TRANSIENT);
            break;
        case 2:
            sqlite3_result_int64(ctx, r->start_rowid);
            break;
        case 3:
            sqlite3_result_int64(ctx, r->end_rowid);
            break;
    }

    return SQLITE_OK;
}


static int brinRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *pRowid) {
    BrinCursor *c = (BrinCursor*)cur;
    *pRowid = c->current_block;
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

