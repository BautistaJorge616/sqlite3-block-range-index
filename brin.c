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

  sqlite3_int64 last_indexed_rowid; // for updates
  int last_block_size;               // Rows currently stored in last block

  int index_ready;

  sqlite3 *db;         // Pointer to the sqlite3 DB handle (stored at xConnect)
} BrinVtab;


/* --- Cursor structure to iterate over BRIN ranges --- */
typedef struct {
    sqlite3_vtab_cursor base;

    int useBrin;
    int current_block;
    int nBlocks;

    sqlite3_int64 low;
    sqlite3_int64 high;

    BrinRange *blocks;

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
    //fprintf(stderr, "brinConnect: not enough args (argc=%d)\n", argc);
    return SQLITE_ERROR;
  }

  //fprintf(stdout, "[BRIN] Connect / xCreate\n");

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

  brinBuildIndex(v);

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


/* -------------------------------------------
 * get_max_rowid:
 * Returns the maximum rowid in the base table.
 * If table is empty, returns 0.
 * ------------------------------------------- */
static sqlite3_int64 get_max_rowid(BrinVtab *v)
{
    sqlite3_stmt *stmt = NULL;
    sqlite3_int64 max_rowid = 0;

    char sql[256];

    /* Build safe SQL */
    snprintf(sql, sizeof(sql),
             "SELECT MAX(rowid) FROM %s;", v->table);

    int rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("[BRIN] get_max_rowid: prepare failed: %s\n",
               sqlite3_errmsg(v->db));
        return v->last_indexed_rowid;  /* fallback safely */
    }

    rc = sqlite3_step(stmt);

    if (rc == SQLITE_ROW) {
        if (sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
            max_rowid = sqlite3_column_int64(stmt, 0);
        } else {
            max_rowid = 0; /* table empty */
        }
    }

    sqlite3_finalize(stmt);

    return max_rowid;
}


/* --------------------------------------------------------
 * brinBuildIndex
 * Builds full in-memory BRIN index by scanning base table.
 * Assumes monotonic increasing values (logs).
 * -------------------------------------------------------- */
static int brinBuildIndex(BrinVtab *v)
{
    if (!v || !v->db)
        return SQLITE_ERROR;

    printf("\n[BRIN] ===== FULL BUILD START =====\n");
    //printf("[BRIN] Table      : %s\n", v->table);
    //printf("[BRIN] Column     : %s\n", v->column);
    //printf("[BRIN] Block size : %d\n", v->block_size);
    //printf("[BRIN] Affinity   : %d\n\n", v->affinity);

    /* ------------------------------------------
     * Free previous index if exists
     * ------------------------------------------ */
    if (v->ranges)
    {
        printf("[BRIN] Cleaning previous ranges...\n");

        if (v->affinity == BRIN_TYPE_TEXT)
        {
            for (int i = 0; i < v->total_blocks; i++)
            {
                free(v->ranges[i].u.txt.min);
                free(v->ranges[i].u.txt.max);
            }
        }

        free(v->ranges);
        v->ranges = NULL;
        v->total_blocks = 0;
    }

    /* ------------------------------------------
     * Prepare scan query
     * ------------------------------------------ */
    char sql[1024];
    snprintf(sql, sizeof(sql),
             "SELECT rowid, %s FROM %s ORDER BY rowid;",
             v->column, v->table);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, NULL);

    if (rc != SQLITE_OK)
    {
        printf("[BRIN] ERROR preparing scan: %s\n",
               sqlite3_errmsg(v->db));
        return rc;
    }

    int capacity = 128; // number of available ranges - This can grow
    int block_pos = 0;
    sqlite3_int64 last_rowid_seen = 0; // for updates

    // allocate space for the ranges
    v->ranges = calloc(capacity, sizeof(BrinRange));
    if (!v->ranges)
    {
        sqlite3_finalize(stmt);
        return SQLITE_NOMEM;
    }

    BrinRange current;

    // fill the space with 0
    memset(&current, 0, sizeof(BrinRange));
    current.type = v->affinity;

    //printf("[BRIN] Scanning table...\n");

    /* ------------------------------------------
     * Main scan loop
     * ------------------------------------------ */

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {

        // sqlite3_step get 1 row per time

        // extract result at column 0
        sqlite3_int64 rowid = sqlite3_column_int64(stmt, 0);
        last_rowid_seen = rowid;

        if (block_pos == 0)
        {
            //printf("[BRIN] Starting new block at rowid=%lld\n", rowid);

            current.start_rowid = rowid;
            current.end_rowid = rowid;
            current.type = v->affinity;

            if (v->affinity == BRIN_TYPE_TEXT)
            {
                const char *txt =
                    (const char*)sqlite3_column_text(stmt, 1);

                current.u.txt.min = strdup(txt ? txt : "");
                current.u.txt.max = strdup(txt ? txt : "");
            }
            else
            {
                double val = sqlite3_column_double(stmt, 1);
                current.u.num.min = val;
                current.u.num.max = val;
            }
        }
        else
        {
            current.end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT)
            {
                const char *txt =
                    (const char*)sqlite3_column_text(stmt, 1);

                free(current.u.txt.max);
                current.u.txt.max = strdup(txt ? txt : "");
            }
            else
            {
                double val = sqlite3_column_double(stmt, 1);
                current.u.num.max = val;
            }
        }

        // internal pos at the block -> inside the range
        block_pos++;

        /* ------------------------------------------
         * Block full → store it
         * ------------------------------------------ */
        if (block_pos >= v->block_size)
        {

            // increase number of ranges
            if (v->total_blocks >= capacity)
            {
                capacity *= 2;
                v->ranges = realloc(v->ranges,
                                    capacity * sizeof(BrinRange));
            }

            v->ranges[v->total_blocks++] = current;

            /*
            printf("[BRIN] Block %d stored "
                   "(rowid %lld - %lld)\n",
                   v->total_blocks - 1,
                   current.start_rowid,
                   current.end_rowid);
            */
            memset(&current, 0, sizeof(BrinRange));
            current.type = v->affinity;
            block_pos = 0;
        }
    }

    /* ------------------------------------------
     * Store partial block if exists
     * ------------------------------------------ */
    if (block_pos > 0)
    {
        if (v->total_blocks >= capacity)
        {
            capacity *= 2;
            v->ranges = realloc(v->ranges,
                                capacity * sizeof(BrinRange));
        }

        v->ranges[v->total_blocks++] = current;
/*
        printf("[BRIN] Partial block %d stored "
               "(rowid %lld - %lld, size=%d)\n",
               v->total_blocks - 1,
               current.start_rowid,
               current.end_rowid,
               block_pos); */
    }

    sqlite3_finalize(stmt);

    /* ------------------------------------------
     * IMPORTANT: Save incremental state
     * ------------------------------------------ */
    v->last_indexed_rowid = last_rowid_seen;
    v->last_block_size = block_pos;
    v->index_ready = 1;
/*
    printf("\n[BRIN] ===== FULL BUILD DONE =====\n");
    printf("[BRIN] Total blocks         : %d\n", v->total_blocks);
    printf("[BRIN] Last indexed rowid   : %lld\n",
           v->last_indexed_rowid);
    printf("[BRIN] Last block size      : %d\n",
           v->last_block_size);
    printf("[BRIN] =================================\n\n");
*/
    return SQLITE_OK;
}


/* --------------------------------------------------------
 * brinIncrementalUpdate
 * Updates BRIN index incrementally for new appended rows.
 * Assumes monotonic rowid growth.
 * -------------------------------------------------------- */
static int brinIncrementalUpdate(BrinVtab *v)
{
    if (!v || !v->db || !v->index_ready)
        return SQLITE_ERROR;

    printf("\n[BRIN] ===== INCREMENTAL UPDATE START =====\n");
    printf("[BRIN] Last indexed rowid : %lld\n", v->last_indexed_rowid);
    printf("[BRIN] Total blocks       : %d\n", v->total_blocks);
    printf("[BRIN] Last block size    : %d\n", v->last_block_size);

    char sql[1024];
    snprintf(sql, sizeof(sql),
             "SELECT rowid, %s FROM %s "
             "WHERE rowid > ? ORDER BY rowid;",
             v->column, v->table);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, NULL);

    if (rc != SQLITE_OK)
    {
        printf("[BRIN] ERROR preparing incremental scan: %s\n",
               sqlite3_errmsg(v->db));
        return rc;
    }

    sqlite3_bind_int64(stmt, 1, v->last_indexed_rowid);

    int new_rows = 0;

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        sqlite3_int64 rowid = sqlite3_column_int64(stmt, 0);
        new_rows++;

        printf("[BRIN] New row detected: rowid=%lld\n", rowid);

        BrinRange *lastBlock = &v->ranges[v->total_blocks - 1];

        /* ------------------------------------------
         * Case 1: Last block still has capacity
         * ------------------------------------------ */
        if (v->last_block_size < v->block_size)
        {
            printf("[BRIN] Extending block %d\n",
                   v->total_blocks - 1);

            lastBlock->end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT)
            {
                const char *txt =
                    (const char*)sqlite3_column_text(stmt, 1);

                free(lastBlock->u.txt.max);
                lastBlock->u.txt.max = strdup(txt ? txt : "");
            }
            else
            {
                double val = sqlite3_column_double(stmt, 1);
                lastBlock->u.num.max = val;
            }

            v->last_block_size++;
        }
        /* ------------------------------------------
         * Case 2: Need new block
         * ------------------------------------------ */
        else
        {
            printf("[BRIN] Creating new block at rowid=%lld\n",
                   rowid);

            BrinRange *tmp = realloc(
                v->ranges,
                (v->total_blocks + 1) * sizeof(BrinRange)
            );

            if (!tmp)
            {
                sqlite3_finalize(stmt);
                return SQLITE_NOMEM;
            }

            v->ranges = tmp;

            BrinRange *newBlock =
                &v->ranges[v->total_blocks];

            memset(newBlock, 0, sizeof(BrinRange));
            newBlock->type = v->affinity;
            newBlock->start_rowid = rowid;
            newBlock->end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT)
            {
                const char *txt =
                    (const char*)sqlite3_column_text(stmt, 1);

                newBlock->u.txt.min = strdup(txt ? txt : "");
                newBlock->u.txt.max = strdup(txt ? txt : "");
            }
            else
            {
                double val = sqlite3_column_double(stmt, 1);
                newBlock->u.num.min = val;
                newBlock->u.num.max = val;
            }

            v->total_blocks++;
            v->last_block_size = 1;
        }

        v->last_indexed_rowid = rowid;
    }

    sqlite3_finalize(stmt);

    if (new_rows == 0)
    {
        printf("[BRIN] No new rows detected.\n");
    }
    else
    {
        printf("[BRIN] Incremental update processed %d new rows.\n",
               new_rows);
    }

    printf("[BRIN] Updated last indexed rowid : %lld\n",
           v->last_indexed_rowid);
    printf("[BRIN] Updated total blocks       : %d\n",
           v->total_blocks);
    printf("[BRIN] Updated last block size    : %d\n",
           v->last_block_size);

    printf("[BRIN] ===== INCREMENTAL UPDATE END =====\n\n");

    return SQLITE_OK;
}


/* --------------------------------------------------
 * xBestIndex - Planner Integration
 * -------------------------------------------------- */
static int brinBestIndex(sqlite3_vtab *pVtab,
                         sqlite3_index_info *pIdxInfo)
{
    BrinVtab *v = (BrinVtab*)pVtab;

    printf("\n[BRIN] ===== xBestIndex START =====\n");

    int minTerm = -1;
    int maxTerm = -1;

    /* Scan available constraints */
    for (int i = 0; i < pIdxInfo->nConstraint; i++) {

        const struct sqlite3_index_constraint *c =
            &pIdxInfo->aConstraint[i];

        printf("[BRIN] Constraint %d usable=%d column=%d op=%d\n",
               i, c->usable, c->iColumn, c->op);

        if (!c->usable)
            continue;

        /* Column 0 = min, Column 1 = max */
        if (c->iColumn == 0 &&
            c->op == SQLITE_INDEX_CONSTRAINT_LE)
        {
            minTerm = i;
            printf("[BRIN] -> Using min <= ? constraint\n");
        }
        else if (c->iColumn == 1 &&
                 c->op == SQLITE_INDEX_CONSTRAINT_GE)
        {
            maxTerm = i;
            printf("[BRIN] -> Using max >= ? constraint\n");
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

    /* Encode strategy */
    if (minTerm >= 0 && maxTerm >= 0)
        pIdxInfo->idxNum = 3;
    else if (minTerm >= 0)
        pIdxInfo->idxNum = 1;
    else if (maxTerm >= 0)
        pIdxInfo->idxNum = 2;
    else
        pIdxInfo->idxNum = 0;

    /* ================================
       COST MODEL (THIS IS CRITICAL)
       ================================ */

    int blocks = v->total_blocks > 0 ? v->total_blocks : 100;

    /*
       If both bounds exist, this is selective.
       We estimate only ~5% of blocks touched.
    */
    if (pIdxInfo->idxNum == 3) {

        int estimated_blocks = blocks / 20;
        if (estimated_blocks < 1)
            estimated_blocks = 1;

        pIdxInfo->estimatedRows = estimated_blocks;

        /* Make cost very attractive */
        pIdxInfo->estimatedCost = (double)estimated_blocks;

        printf("[BRIN] Strategy: FULL RANGE\n");
        printf("[BRIN] Estimated blocks touched: %d\n",
               estimated_blocks);
    }
    else {
        /* Worst case full scan */
        pIdxInfo->estimatedRows = blocks;
        pIdxInfo->estimatedCost = (double)(blocks * 10);

        printf("[BRIN] Strategy: FULL SCAN\n");
    }

    /*
       Encourage planner to choose this as outer loop.
       Lower cost => higher priority.
    */
    if (pIdxInfo->estimatedCost < 10)
        pIdxInfo->estimatedCost = 1.0;

    /* Optional: hint that ordering is preserved */
    pIdxInfo->orderByConsumed = 0;

    printf("[BRIN] idxNum=%d cost=%.2f rows=%lld\n",
           pIdxInfo->idxNum,
           pIdxInfo->estimatedCost,
           pIdxInfo->estimatedRows);

    printf("[BRIN] ===== xBestIndex END =====\n\n");

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

    //printf("[BRIN] xFilter invoked idxNum=%d argc=%d\n", idxNum, argc);

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
      printf("[BRIN] xFilter: BRIN build\n");
      brinBuildIndex(v);
    }
/*
    else {
      printf("[BRIN] xFilter: BRIN update\n");

      sqlite3_int64 current_max = get_max_rowid(v);

      if (current_max > v->last_indexed_rowid) {
        printf("[BRIN] New rows detected. Updating...\n");
        brinIncrementalUpdate(v);
      } else {
        printf("[BRIN] No new rows. Skipping update.\n");
      }


    }
*/

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

        //printf("[BRIN] search bounds NUM [%lld .. %lld]\n",
        //       c->low, c->high);

        /* Find first matching block */
        while (c->current_block < c->nBlocks) {
            BrinRange *r = &c->blocks[c->current_block];

            if (r->u.num.max >= c->low &&
                r->u.num.min <= c->high) {
                c->useBrin = 1;
                c->eof = 0;
                //printf("[BRIN] first matching block=%d\n",
                //       c->current_block);
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

