#include <sqlite3ext.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <time.h>   // Needed for struct tm and time conversion

#ifdef DEBUG
    #define DEBUG_PRINT(...) printf(__VA_ARGS__)
#else
    #define DEBUG_PRINT(...)
#endif

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

    BrinVtab *v;

    sqlite3_int64 low;
    sqlite3_int64 high;

    int start_block;
    int end_block;
    int current_block;

    int eof;
} BrinCursor;


const char* get_affinity(const char *declared_type) {
    if (!declared_type) return NULL;

    DEBUG_PRINT("[BRIN] get_affinity()\n");

    char type[64];
    snprintf(type, sizeof(type), "%s", declared_type);
    for (int i = 0; type[i]; i++) {
        type[i] = toupper(type[i]);
    }

    DEBUG_PRINT("type:%s\n", type);

    if (strstr(type, "INT")) return "INTEGER";
    if (strstr(type, "CHAR") || strstr(type, "CLOB")
                             || strstr(type, "TEXT")) return "TEXT";
    if (strstr(type, "REAL") || strstr(type, "FLOA")
                             || strstr(type, "DOUB")) return "REAL";
    if (strstr(type, "DATE")
                             || strstr(type, "DATETIME")) return "TEXT";

    return NULL;
}


/* -------------------------------------------
 * get_max_rowid:
 * Returns the maximum rowid in the base table.
 * If table is empty, returns 0.
 * ------------------------------------------- */
static sqlite3_int64 get_max_rowid(BrinVtab *v)
{

    DEBUG_PRINT("[BRIN] get_max_rowid()\n");

    sqlite3_stmt *stmt = NULL;
    sqlite3_int64 max_rowid = 0;

    char sql[256];

    /* Build safe SQL */
    snprintf(sql, sizeof(sql),
             "SELECT MAX(rowid) FROM %s;", v->table);

    int rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("get_max_rowid: prepare failed: %s\n",
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

    DEBUG_PRINT("max_rowid:%lld\n",max_rowid);

    return max_rowid;
}


/* --------------------------------------------------------
 * brinIncrementalUpdate
 * Updates BRIN index incrementally for new appended rows.
 * Assumes monotonic rowid growth.
 * -------------------------------------------------------- */
static int brinIncrementalUpdate(BrinVtab *v)
{
    if (!v || !v->db || !v->index_ready)
        return SQLITE_OK;

    DEBUG_PRINT("[BRIN] brinIncrementalUpdate()\n");

    char sql[512];

    snprintf(sql, sizeof(sql),
        "SELECT rowid, %s FROM %s "
        "WHERE rowid > ?;",
        v->column, v->table);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK)
        return rc;

    sqlite3_bind_int64(stmt, 1, v->last_indexed_rowid);

    int found_new_rows = 0;

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        found_new_rows = 1;

        sqlite3_int64 rowid =
            sqlite3_column_int64(stmt, 0);

        BrinRange *lastBlock =
            &v->ranges[v->total_blocks - 1];

        /* Extend current block */
        if (v->last_block_size < v->block_size)
        {
            lastBlock->end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT) {
                const char *txt =
                  (const char*)sqlite3_column_text(stmt,1);
                free(lastBlock->u.txt.max);
                lastBlock->u.txt.max = strdup(txt?txt:"");
            } else {
                lastBlock->u.num.max =
                    sqlite3_column_double(stmt,1);
            }

            v->last_block_size++;
        }
        else
        {
            /* Allocate new block */
            BrinRange *tmp =
                realloc(v->ranges,
                        (v->total_blocks+1)*sizeof(BrinRange));
            if (!tmp) break;

            v->ranges = tmp;

            BrinRange *newBlock =
                &v->ranges[v->total_blocks];

            memset(newBlock,0,sizeof(BrinRange));
            newBlock->type = v->affinity;
            newBlock->start_rowid = rowid;
            newBlock->end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT) {
                const char *txt =
                  (const char*)sqlite3_column_text(stmt,1);
                newBlock->u.txt.min = strdup(txt?txt:"");
                newBlock->u.txt.max = strdup(txt?txt:"");
            } else {
                double val =
                    sqlite3_column_double(stmt,1);
                newBlock->u.num.min = val;
                newBlock->u.num.max = val;
            }

            v->total_blocks++;
            v->last_block_size = 1;
        }

        v->last_indexed_rowid = rowid;
    }

    sqlite3_finalize(stmt);

    DEBUG_PRINT("found_new_rows:%d\n",found_new_rows);

    /* If no rows found → O(1) fast exit */
    if (!found_new_rows)
        return SQLITE_OK;

    return SQLITE_OK;
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


    DEBUG_PRINT("[BRIN] brinBuildIndex() \n");
    DEBUG_PRINT("Table      : %s\n", v->table);
    DEBUG_PRINT("Column     : %s\n", v->column);
    DEBUG_PRINT("Block size : %d\n", v->block_size);
    DEBUG_PRINT("Affinity   : %d\n\n", v->affinity);

    /* ------------------------------------------
     * Free previous index if exists
     * ------------------------------------------ */
    if (v->ranges)
    {
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
             "SELECT rowid, %s FROM %s;",
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
            DEBUG_PRINT("new block at rowid=%lld\n", rowid);

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

                   DEBUG_PRINT("Block %d stored "
                   "(rowid %lld - %lld)\n",
                   v->total_blocks - 1,
                   current.start_rowid,
                   current.end_rowid);

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

        DEBUG_PRINT("Partial block %d stored "
               "(rowid %lld - %lld, size=%d)\n",
               v->total_blocks - 1,
               current.start_rowid,
               current.end_rowid,
               block_pos);
    }

    sqlite3_finalize(stmt);

    /* ------------------------------------------
     * IMPORTANT: Save incremental state
     * ------------------------------------------ */
    v->last_indexed_rowid = last_rowid_seen;
    v->last_block_size = block_pos;
    v->index_ready = 1;

    DEBUG_PRINT("Total blocks         : %d\n", v->total_blocks);
    DEBUG_PRINT("Last indexed rowid   : %lld\n",
           v->last_indexed_rowid);
    DEBUG_PRINT("Last block size      : %d\n",
           v->last_block_size);

    return SQLITE_OK;
}


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

  DEBUG_PRINT("[BRIN] brinConnect()\n");

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

  /* FULL BUILD at creation */
  rc = brinBuildIndex(v);
  if (rc != SQLITE_OK)
      return rc;

  return SQLITE_OK;
}


/* --------------------------------------------------
 * xBestIndex - Planner Integration (Instrumented)
 *
 * This function is called during the PLANNING phase.
 * No real data is accessed here.
 *
 * SQLite asks:
 *   "Given these WHERE constraints,
 *    how can you optimize this scan,
 *    and how expensive will it be?"
 *
 * We must:
 *   1. Inspect available constraints
 *   2. Decide which ones we can use
 *   3. Assign argvIndex so xFilter receives values
 *   4. Estimate cost and cardinality
 *   5. Encode strategy in idxNum
 *
 * IMPORTANT:
 *   This function does NOT execute the query.
 *   It only negotiates with the query planner.
 * -------------------------------------------------- */
static int brinBestIndex(sqlite3_vtab *pVtab,
                         sqlite3_index_info *pIdxInfo)
{
    BrinVtab *v = (BrinVtab*)pVtab;

    DEBUG_PRINT("[BRIN] brinBestIndex()\n");
    DEBUG_PRINT("total_blocks currently known: %d\n", v->total_blocks);

    int minTerm = -1;
    int maxTerm = -1;

    /* --------------------------------------------------
     * STEP 1 — Inspect all available constraints
     *
     * SQLite extracted every possible WHERE/ON condition
     * applicable to this virtual table.
     *
     * We scan them and detect:
     *   column 0 (min)  with <=
     *   column 1 (max)  with >=
     * -------------------------------------------------- */
    for (int i = 0; i < pIdxInfo->nConstraint; i++) {

        const struct sqlite3_index_constraint *c =
            &pIdxInfo->aConstraint[i];

        DEBUG_PRINT("Constraint %d usable=%d column=%d op=%d\n",
                     i, c->usable, c->iColumn, c->op);

        if (!c->usable) {
            DEBUG_PRINT("Skipping (not usable in this join position)\n");
            continue;
        }

        /* Detect lower/upper bound pattern */
        if (c->iColumn == 0 &&
            c->op == SQLITE_INDEX_CONSTRAINT_LE)
        {
            minTerm = i;
            DEBUG_PRINT("Candidate: min <= ? detected\n");
        }
        else if (c->iColumn == 1 &&
                 c->op == SQLITE_INDEX_CONSTRAINT_GE)
        {
            maxTerm = i;
            DEBUG_PRINT("Candidate: max >= ? detected\n");
        }
    }

    /* --------------------------------------------------
     * STEP 2 — Tell SQLite which constraints we use
     *
     * If we assign argvIndex:
     *   SQLite will pass the RHS value to xFilter().
     *
     * If we set omit=1:
     *   SQLite will NOT re-evaluate this predicate later.
     * -------------------------------------------------- */
    int argvIdx = 1;

    if (minTerm >= 0) {
        pIdxInfo->aConstraintUsage[minTerm].argvIndex = argvIdx++;
        pIdxInfo->aConstraintUsage[minTerm].omit = 1;
        DEBUG_PRINT("Using min <= ? in index\n");
    }

    if (maxTerm >= 0) {
        pIdxInfo->aConstraintUsage[maxTerm].argvIndex = argvIdx++;
        pIdxInfo->aConstraintUsage[maxTerm].omit = 1;
        DEBUG_PRINT("Using max >= ? in index\n");
    }

    /* --------------------------------------------------
     * STEP 3 — Encode strategy in idxNum
     *
     * idxNum is INTERNAL to our virtual table.
     * SQLite does not interpret it.
     *
     * We define:
     *   3 = full range (min + max)
     *   1 = only min
     *   2 = only max
     *   0 = full scan
     * -------------------------------------------------- */
    if (minTerm >= 0 && maxTerm >= 0) {
        pIdxInfo->idxNum = 3;
        DEBUG_PRINT("Strategy selected: FULL RANGE\n");
    }
    else if (minTerm >= 0) {
        pIdxInfo->idxNum = 1;
        DEBUG_PRINT("Strategy selected: LOWER BOUND only\n");
    }
    else if (maxTerm >= 0) {
        pIdxInfo->idxNum = 2;
        DEBUG_PRINT("Strategy selected: UPPER BOUND only\n");
    }
    else {
        pIdxInfo->idxNum = 0;
        DEBUG_PRINT("Strategy selected: FULL SCAN\n");
    }

    /* --------------------------------------------------
     * STEP 4 — Cost Model
     *
     * The planner will compare our estimatedCost
     * and estimatedRows with other tables in the join.
     *
     * Lower cost → more likely to become OUTER loop.
     *
     * IMPORTANT:
     *   These are heuristics.
     *   No real data access happens here.
     * -------------------------------------------------- */

    int blocks = v->total_blocks;

    if (blocks == 0) {
        /* Edge case: empty index */
        pIdxInfo->estimatedRows = 0;
        pIdxInfo->estimatedCost = 0;
        DEBUG_PRINT("No blocks available\n");
    }
    else if (pIdxInfo->idxNum == 3) {
        /* Full range predicate: assume selective */

        int estimated_blocks = blocks / 20;  /* 5% heuristic */
        if (estimated_blocks < 1)
            estimated_blocks = 1;

        pIdxInfo->estimatedRows = estimated_blocks;
        pIdxInfo->estimatedCost = (double)estimated_blocks;

        DEBUG_PRINT("Estimated blocks touched: %d of %d\n",
                     estimated_blocks, blocks);
    }
    else {
        /* Non-selective or unsupported pattern */
        pIdxInfo->estimatedRows = blocks;
        pIdxInfo->estimatedCost = (double)(blocks * 10);

        DEBUG_PRINT("Non-selective → high cost full scan\n");
    }

    /* --------------------------------------------------
     * STEP 5 — ORDER BY handling
     *
     * Our blocks are naturally ordered by start_rowid.
     * If the planner requests ORDER BY start_rowid ASC,
     * we can satisfy it without extra sorting.
     * -------------------------------------------------- */
    pIdxInfo->orderByConsumed = 0;

    if (pIdxInfo->nOrderBy == 1) {

        if (pIdxInfo->aOrderBy[0].iColumn == 2 &&   /* start_rowid */
            pIdxInfo->aOrderBy[0].desc == 0)        /* ASC */
        {
            pIdxInfo->orderByConsumed = 1;
            DEBUG_PRINT("ORDER BY start_rowid ASC satisfied by index\n");
        }
    }

    DEBUG_PRINT("Final decision: idxNum=%d cost=%.2f rows=%lld\n",
                 pIdxInfo->idxNum,
                 pIdxInfo->estimatedCost,
                 pIdxInfo->estimatedRows);


    return SQLITE_OK;
}


/* ---------------------------
 * xOpen / xClose
 * --------------------------- */
static int brinOpen(
    sqlite3_vtab *pVtab,
    sqlite3_vtab_cursor **ppCursor)
{

    DEBUG_PRINT("[BRIN] brinOpen()\n");

    BrinCursor *c = calloc(1, sizeof(BrinCursor));
    if (!c) return SQLITE_NOMEM;

    c->v = (BrinVtab*)pVtab;
    c->eof = 1;

    *ppCursor = &c->base;
    return SQLITE_OK;
}


static int brinClose(sqlite3_vtab_cursor *cur)
{

    DEBUG_PRINT("[BRIN] brinClose()\n");

    free(cur);
    return SQLITE_OK;
}


static int brinFilter(
    sqlite3_vtab_cursor *cur,
    int idxNum,
    const char *idxStr,
    int argc,
    sqlite3_value **argv)
{

    DEBUG_PRINT("[BRIN] brinFilter()\n");

    BrinCursor *c = (BrinCursor*)cur;
    BrinVtab   *v = c->v;

    c->eof = 1;

    /* Only support full range plan */
    if (idxNum != 3 || argc != 2)
        return SQLITE_OK;

    /* Incremental update (cheap if no new rows) */
    brinIncrementalUpdate(v);

    if (v->total_blocks == 0)
        return SQLITE_OK;

    sqlite3_int64 a = sqlite3_value_int64(argv[0]);
    sqlite3_int64 b = sqlite3_value_int64(argv[1]);

    c->low  = (a < b) ? a : b;
    c->high = (a < b) ? b : a;

    int left, right, mid;

    /* --------------------------------------------
       Binary search for FIRST block where
          r->max >= low
       -------------------------------------------- */
    int start = v->total_blocks;

    left = 0;
    right = v->total_blocks - 1;

    while (left <= right) {
        mid = (left + right) / 2;

        BrinRange *r = &v->ranges[mid];

        if (r->u.num.max >= c->low) {
            start = mid;
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    if (start == v->total_blocks)
        return SQLITE_OK;  /* no possible match */

    /* --------------------------------------------
       Binary search for LAST block where
          r->min <= high
       -------------------------------------------- */
    int end = -1;

    left = 0;
    right = v->total_blocks - 1;

    while (left <= right) {
        mid = (left + right) / 2;

        BrinRange *r = &v->ranges[mid];

        if (r->u.num.min <= c->high) {
            end = mid;
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    if (end < start)
        return SQLITE_OK;

    c->start_block   = start;
    c->end_block     = end;
    c->current_block = start;
    c->eof = 0;

    return SQLITE_OK;
}


/* ---------------------------
 * xNext / xEof / xColumn / xRowid
 * --------------------------- */
static int brinNext(sqlite3_vtab_cursor *cur)
{

    DEBUG_PRINT("[BRIN] brinNext()\n");

    BrinCursor *c = (BrinCursor*)cur;

    c->current_block++;

    if (c->current_block > c->end_block)
        c->eof = 1;

    return SQLITE_OK;
}


static int brinEof(sqlite3_vtab_cursor *cur) {
    DEBUG_PRINT("[BRIN] brinEof()\n");
    BrinCursor *c = (BrinCursor*)cur;
    return c->eof;
}


static int brinColumn(
    sqlite3_vtab_cursor *cur,
    sqlite3_context *ctx,
    int col)
{

    DEBUG_PRINT("[BRIN] brinColumn()\n");

    BrinCursor *c = (BrinCursor*)cur;

    if (c->eof) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    BrinRange *r = &c->v->ranges[c->current_block];

    switch (col)
    {
        case 0:
            sqlite3_result_double(ctx, r->u.num.min);
            break;
        case 1:
            sqlite3_result_double(ctx, r->u.num.max);
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

static int brinRowid(
    sqlite3_vtab_cursor *cur,
    sqlite3_int64 *pRowid)
{
    DEBUG_PRINT("[BRIN] brinRowid()\n");
    BrinCursor *c = (BrinCursor*)cur;
    *pRowid = c->current_block;
    return SQLITE_OK;
}


/* ---------------------------
 * xDisconnect / xDestroy
 * --------------------------- */
static int brinDisconnect(sqlite3_vtab *pVTab){
    BrinVtab *v = (BrinVtab*)pVTab;
    DEBUG_PRINT("[BRIN] brinDisconnect()\n");

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
    DEBUG_PRINT("[BRIN] brinDestroy()\n");
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

