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


/* --------------------------------------------------
 * brinTextCmp
 *
 * PURPOSE
 * -------
 * Compare two TEXT values using simple lexicographic
 * order.
 *
 * THESIS ASSUMPTION
 * -----------------
 * This prototype only supports TEXT values that follow
 * ISO 8601 conventions, for example:
 *
 *   2026-04-11
 *   2026-04-11 13:45:20
 *
 * Under this assumption, lexicographic order matches
 * chronological order, so strcmp() is sufficient.
 *
 * NULL HANDLING
 * -------------
 * We treat NULL as an empty string to avoid crashes.
 * In the thesis prototype, ideally indexed TEXT values
 * should be non-NULL and globally ordered.
 *
 * RETURN VALUE
 * ------------
 * Same semantics as strcmp():
 *   < 0  if a < b
 *   = 0  if a == b
 *   > 0  if a > b
 * -------------------------------------------------- */
static int brinTextCmp(const char *a, const char *b)
{
    if (a == NULL) a = "";
    if (b == NULL) b = "";
    return strcmp(a, b);
}


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
 *
 * PURPOSE
 * -------
 * Update the in-memory BRIN index incrementally by scanning
 * only rows appended after the last indexed rowid.
 *
 * THESIS ASSUMPTIONS
 * ------------------
 * - The base table is append-only.
 * - No UPDATE operations are considered.
 * - No DELETE operations are considered.
 * - The indexed column remains globally ordered ascending.
 * - New appended rows always contain values greater than or
 *   equal to the previous indexed values.
 *
 * Because of these assumptions, incremental maintenance is
 * simple:
 *
 *   1. Read rows with rowid > last_indexed_rowid
 *   2. If the last BRIN block still has space:
 *        - extend that block
 *        - update end_rowid
 *        - update block max
 *   3. If the last block is full:
 *        - create a new BRIN block
 *        - initialize min/max with the new row value
 *
 * WHY THIS WORKS
 * --------------
 * Since values are globally ordered:
 * - The first value entering a block is its minimum
 * - The last  value seen in that block is its maximum
 *
 * This logic is valid for:
 * - INTEGER
 * - REAL
 * - TEXT following ISO 8601 lexical conventions
 *
 * OUTPUT
 * ------
 * Updates:
 *   - v->ranges
 *   - v->total_blocks
 *   - v->last_indexed_rowid
 *   - v->last_block_size
 *
 * RETURN VALUE
 * ------------
 * SQLITE_OK on success, or an SQLite error code.
 * -------------------------------------------------------- */
static int brinIncrementalUpdate(BrinVtab *v)
{
    /* Defensive validation */
    if (!v || !v->db || !v->index_ready)
        return SQLITE_OK;

    DEBUG_PRINT("[BRIN] brinIncrementalUpdate()\n");
    DEBUG_PRINT("last_indexed_rowid before update: %lld\n", v->last_indexed_rowid);
    DEBUG_PRINT("last_block_size before update   : %d\n", v->last_block_size);
    DEBUG_PRINT("total_blocks before update      : %d\n", v->total_blocks);

    /* ----------------------------------------------------
     * Prepare a query that fetches only new appended rows.
     *
     * Because the thesis assumes append-only inserts,
     * rowid > last_indexed_rowid is enough.
     * ---------------------------------------------------- */
    char sql[512];
    snprintf(sql, sizeof(sql),
        "SELECT rowid, %s FROM %s "
        "WHERE rowid > ?;",
        v->column, v->table);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        DEBUG_PRINT("brinIncrementalUpdate: prepare failed: %s\n",
                    sqlite3_errmsg(v->db));
        return rc;
    }

    sqlite3_bind_int64(stmt, 1, v->last_indexed_rowid);

    int found_new_rows = 0;

    /* ----------------------------------------------------
     * Process every newly appended row.
     * ---------------------------------------------------- */
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        found_new_rows = 1;

        sqlite3_int64 rowid = sqlite3_column_int64(stmt, 0);

        DEBUG_PRINT("Processing appended rowid=%lld\n", rowid);

        /* ------------------------------------------------
         * CASE 1
         * No BRIN blocks exist yet.
         *
         * This should normally not happen if brinBuildIndex()
         * already ran successfully, but we handle it
         * defensively anyway.
         * ------------------------------------------------ */
        if (v->total_blocks == 0)
        {
            DEBUG_PRINT("No existing BRIN blocks, creating first block\n");

            BrinRange *tmp = realloc(v->ranges,
                                     (v->total_blocks + 1) * sizeof(BrinRange));
            if (!tmp) {
                sqlite3_finalize(stmt);
                return SQLITE_NOMEM;
            }

            v->ranges = tmp;

            BrinRange *newBlock = &v->ranges[v->total_blocks];
            memset(newBlock, 0, sizeof(BrinRange));

            newBlock->type = v->affinity;
            newBlock->start_rowid = rowid;
            newBlock->end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT) {
                const char *txt = (const char*)sqlite3_column_text(stmt, 1);
                newBlock->u.txt.min = strdup(txt ? txt : "");
                newBlock->u.txt.max = strdup(txt ? txt : "");

                DEBUG_PRINT("Initialized first TEXT block with value=%s\n",
                            txt ? txt : "(null)");
            } else {
                double val = sqlite3_column_double(stmt, 1);
                newBlock->u.num.min = val;
                newBlock->u.num.max = val;

                DEBUG_PRINT("Initialized first numeric block with value=%.6f\n", val);
            }

            v->total_blocks++;
            v->last_block_size = 1;
            v->last_indexed_rowid = rowid;

            continue;
        }

        /* ------------------------------------------------
         * CASE 2
         * There is already at least one block.
         * Get a pointer to the last block because append-only
         * maintenance only affects the tail of the BRIN.
         * ------------------------------------------------ */
        BrinRange *lastBlock = &v->ranges[v->total_blocks - 1];

        /* ------------------------------------------------
         * CASE 2A
         * The last block still has space.
         *
         * Since values are globally ordered ascending:
         * - min stays unchanged
         * - max becomes the new appended value
         * - end_rowid moves forward
         * ------------------------------------------------ */
        if (v->last_block_size < v->block_size)
        {
            lastBlock->end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT) {
                const char *txt = (const char*)sqlite3_column_text(stmt, 1);

                free(lastBlock->u.txt.max);
                lastBlock->u.txt.max = strdup(txt ? txt : "");

                if (!lastBlock->u.txt.max) {
                    sqlite3_finalize(stmt);
                    return SQLITE_NOMEM;
                }

                DEBUG_PRINT("Extended existing TEXT block\n");
                DEBUG_PRINT("New TEXT max=%s\n", txt ? txt : "(null)");
            } else {
                double val = sqlite3_column_double(stmt, 1);
                lastBlock->u.num.max = val;

                DEBUG_PRINT("Extended existing numeric block\n");
                DEBUG_PRINT("New numeric max=%.6f\n", val);
            }

            v->last_block_size++;
        }
        /* ------------------------------------------------
         * CASE 2B
         * The last block is full.
         *
         * Create a brand new BRIN block.
         * Since this is the first value entering the block:
         * - min = max = current value
         * - start_rowid = end_rowid = current rowid
         * ------------------------------------------------ */
        else
        {
            DEBUG_PRINT("Last block is full, creating a new block\n");

            BrinRange *tmp = realloc(v->ranges,
                                     (v->total_blocks + 1) * sizeof(BrinRange));
            if (!tmp) {
                sqlite3_finalize(stmt);
                return SQLITE_NOMEM;
            }

            v->ranges = tmp;

            BrinRange *newBlock = &v->ranges[v->total_blocks];
            memset(newBlock, 0, sizeof(BrinRange));

            newBlock->type = v->affinity;
            newBlock->start_rowid = rowid;
            newBlock->end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT) {
                const char *txt = (const char*)sqlite3_column_text(stmt, 1);
                newBlock->u.txt.min = strdup(txt ? txt : "");
                newBlock->u.txt.max = strdup(txt ? txt : "");

                if (!newBlock->u.txt.min || !newBlock->u.txt.max) {
                    free(newBlock->u.txt.min);
                    free(newBlock->u.txt.max);
                    sqlite3_finalize(stmt);
                    return SQLITE_NOMEM;
                }

                DEBUG_PRINT("Created new TEXT block with min=max=%s\n",
                            txt ? txt : "(null)");
            } else {
                double val = sqlite3_column_double(stmt, 1);
                newBlock->u.num.min = val;
                newBlock->u.num.max = val;

                DEBUG_PRINT("Created new numeric block with min=max=%.6f\n", val);
            }

            v->total_blocks++;
            v->last_block_size = 1;
        }

        /* ------------------------------------------------
         * Update global incremental state after each row.
         * ------------------------------------------------ */
        v->last_indexed_rowid = rowid;
    }

    /* sqlite3_step() must finish either at SQLITE_DONE
       or with an actual error. */
    if (rc != SQLITE_DONE) {
        DEBUG_PRINT("brinIncrementalUpdate: step failed: %s\n",
                    sqlite3_errmsg(v->db));
        sqlite3_finalize(stmt);
        return rc;
    }

    sqlite3_finalize(stmt);

    /* ----------------------------------------------------
     * Fast O(1) no-op behavior:
     * If no new rows were found, nothing changed.
     * ---------------------------------------------------- */
    if (!found_new_rows) {
        DEBUG_PRINT("No appended rows found, BRIN unchanged\n");
        return SQLITE_OK;
    }

    DEBUG_PRINT("Incremental update finished\n");
    DEBUG_PRINT("last_indexed_rowid after update: %lld\n", v->last_indexed_rowid);
    DEBUG_PRINT("last_block_size after update   : %d\n", v->last_block_size);
    DEBUG_PRINT("total_blocks after update      : %d\n", v->total_blocks);

    return SQLITE_OK;
}


/* --------------------------------------------------------
 * brinBuildIndex
 *
 * PURPOSE
 * -------
 * Build the full in-memory BRIN index by scanning the base
 * table once from beginning to end.
 *
 * THESIS ASSUMPTIONS
 * ------------------
 * - The indexed column is globally ordered ascending.
 * - Data is append-only.
 * - No UPDATE and no DELETE are considered.
 *
 * Because the base data is globally ordered, each block
 * summary can be built very efficiently:
 *
 *   - The first value seen in a block is the block MIN
 *   - The last  value seen in a block is the block MAX
 *
 * This is true for:
 *   - INTEGER
 *   - REAL
 *   - TEXT using ISO 8601 lexical conventions
 *
 * For TEXT, lexicographic order matches the intended
 * logical order of the prototype, so the first/last
 * values of the block also define min/max.
 *
 * OUTPUT
 * ------
 * v->ranges will contain one BrinRange per block.
 * Each BrinRange stores:
 *   - min / max value
 *   - start_rowid / end_rowid
 *
 * SIDE EFFECTS
 * ------------
 * Also updates:
 *   - v->total_blocks
 *   - v->last_indexed_rowid
 *   - v->last_block_size
 *   - v->index_ready
 * -------------------------------------------------------- */
static int brinBuildIndex(BrinVtab *v)
{
    if (!v || !v->db)
        return SQLITE_ERROR;

    DEBUG_PRINT("[BRIN] brinBuildIndex()\n");
    DEBUG_PRINT("Table      : %s\n", v->table);
    DEBUG_PRINT("Column     : %s\n", v->column);
    DEBUG_PRINT("Block size : %d\n", v->block_size);
    DEBUG_PRINT("Affinity   : %d\n\n", v->affinity);

    /* ------------------------------------------
     * STEP 0
     * Free previous in-memory index if it exists.
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
     * STEP 1
     * Prepare sequential scan over the base table.
     *
     * Since the thesis assumes append-only ordered data,
     * a plain forward scan is enough to construct the
     * BRIN summaries.
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

    /* ------------------------------------------
     * STEP 2
     * Allocate initial storage for block summaries.
     * ------------------------------------------ */
    int capacity = 128;
    int block_pos = 0;
    sqlite3_int64 last_rowid_seen = 0;

    v->ranges = calloc(capacity, sizeof(BrinRange));
    if (!v->ranges)
    {
        sqlite3_finalize(stmt);
        return SQLITE_NOMEM;
    }

    BrinRange current;
    memset(&current, 0, sizeof(BrinRange));
    current.type = v->affinity;

    /* ------------------------------------------
     * STEP 3
     * Main scan loop.
     *
     * We process rows in order and group them into
     * fixed-size BRIN blocks.
     * ------------------------------------------ */
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        sqlite3_int64 rowid = sqlite3_column_int64(stmt, 0);
        last_rowid_seen = rowid;

        /* --------------------------------------
         * First row of a new block
         *
         * Because data is ordered:
         *   this first value is also the block MIN
         * -------------------------------------- */
        if (block_pos == 0)
        {
            DEBUG_PRINT("Starting new block at rowid=%lld\n", rowid);

            current.start_rowid = rowid;
            current.end_rowid = rowid;
            current.type = v->affinity;

            if (v->affinity == BRIN_TYPE_TEXT)
            {
                const char *txt =
                    (const char*)sqlite3_column_text(stmt, 1);

                current.u.txt.min = strdup(txt ? txt : "");
                current.u.txt.max = strdup(txt ? txt : "");

                DEBUG_PRINT("Initial TEXT value for block: %s\n",
                            txt ? txt : "(null)");
            }
            else
            {
                double val = sqlite3_column_double(stmt, 1);
                current.u.num.min = val;
                current.u.num.max = val;

                DEBUG_PRINT("Initial numeric value for block: %.6f\n", val);
            }
        }
        else
        {
            /* ----------------------------------
             * Additional row inside current block
             *
             * Because data is ordered ascending:
             *   the newest value becomes the block MAX
             * ---------------------------------- */
            current.end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT)
            {
                const char *txt =
                    (const char*)sqlite3_column_text(stmt, 1);

                free(current.u.txt.max);
                current.u.txt.max = strdup(txt ? txt : "");

                DEBUG_PRINT("Updated block TEXT max to: %s\n",
                            txt ? txt : "(null)");
            }
            else
            {
                double val = sqlite3_column_double(stmt, 1);
                current.u.num.max = val;

                DEBUG_PRINT("Updated block numeric max to: %.6f\n", val);
            }
        }

        block_pos++;

        /* --------------------------------------
         * STEP 4
         * If block reached block_size, store it.
         * -------------------------------------- */
        if (block_pos >= v->block_size)
        {
            if (v->total_blocks >= capacity)
            {
                capacity *= 2;
                BrinRange *tmp = realloc(v->ranges,
                                         capacity * sizeof(BrinRange));
                if (!tmp) {
                    sqlite3_finalize(stmt);
                    return SQLITE_NOMEM;
                }
                v->ranges = tmp;
            }

            v->ranges[v->total_blocks++] = current;

            if (v->affinity == BRIN_TYPE_TEXT) {
                DEBUG_PRINT("Stored TEXT block %d: rowid [%lld, %lld], min=%s, max=%s\n",
                            v->total_blocks - 1,
                            current.start_rowid,
                            current.end_rowid,
                            current.u.txt.min ? current.u.txt.min : "(null)",
                            current.u.txt.max ? current.u.txt.max : "(null)");
            } else {
                DEBUG_PRINT("Stored numeric block %d: rowid [%lld, %lld], min=%.6f, max=%.6f\n",
                            v->total_blocks - 1,
                            current.start_rowid,
                            current.end_rowid,
                            current.u.num.min,
                            current.u.num.max);
            }

            memset(&current, 0, sizeof(BrinRange));
            current.type = v->affinity;
            block_pos = 0;
        }
    }

    /* ------------------------------------------
     * STEP 5
     * Store final partial block if needed.
     * ------------------------------------------ */
    if (block_pos > 0)
    {
        if (v->total_blocks >= capacity)
        {
            capacity *= 2;
            BrinRange *tmp = realloc(v->ranges,
                                     capacity * sizeof(BrinRange));
            if (!tmp) {
                sqlite3_finalize(stmt);
                return SQLITE_NOMEM;
            }
            v->ranges = tmp;
        }

        v->ranges[v->total_blocks++] = current;

        if (v->affinity == BRIN_TYPE_TEXT) {
            DEBUG_PRINT("Stored partial TEXT block %d: rowid [%lld, %lld], size=%d, min=%s, max=%s\n",
                        v->total_blocks - 1,
                        current.start_rowid,
                        current.end_rowid,
                        block_pos,
                        current.u.txt.min ? current.u.txt.min : "(null)",
                        current.u.txt.max ? current.u.txt.max : "(null)");
        } else {
            DEBUG_PRINT("Stored partial numeric block %d: rowid [%lld, %lld], size=%d, min=%.6f, max=%.6f\n",
                        v->total_blocks - 1,
                        current.start_rowid,
                        current.end_rowid,
                        block_pos,
                        current.u.num.min,
                        current.u.num.max);
        }
    }

    sqlite3_finalize(stmt);

    /* ------------------------------------------
     * STEP 6
     * Save incremental state for future append-only
     * maintenance.
     * ------------------------------------------ */
    v->last_indexed_rowid = last_rowid_seen;
    v->last_block_size = block_pos;
    v->index_ready = 1;

    DEBUG_PRINT("Total blocks         : %d\n", v->total_blocks);
    DEBUG_PRINT("Last indexed rowid   : %lld\n", v->last_indexed_rowid);
    DEBUG_PRINT("Last block size      : %d\n", v->last_block_size);

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


/* --------------------------------------------------
 * xBestIndex
 *
 * PURPOSE
 * -------
 * Called by SQLite during query planning.
 *
 * This method does NOT execute the query.
 * Its job is to tell SQLite:
 *   - which constraints we can use
 *   - how arguments should be passed to xFilter()
 *   - how many virtual rows we think we will return
 *   - how expensive we think the access path is
 *
 * THESIS MODEL
 * ------------
 * We support exactly one useful BRIN access pattern:
 *
 *   min <= high   AND   max >= low
 *
 * If both constraints exist and are usable:
 *   - we accept the BRIN plan
 *   - we map both RHS values into xFilter() arguments
 *   - if the RHS values are available during planning,
 *     we compute the exact number of candidate blocks
 *   - otherwise we use the thesis fallback:
 *
 *         estimatedRows = 2
 *         estimatedCost = 2.0
 *
 * SUPPORTED DATA TYPES
 * --------------------
 * - INTEGER
 * - REAL
 * - TEXT (ISO 8601 conventions only)
 *
 * IMPORTANT NOTE ABOUT sqlite3_vtab_rhs_value()
 * ---------------------------------------------
 * SQLite may expose RHS values during planning only
 * in some cases, typically when they are literals in
 * the SQL text.
 *
 * If the RHS is a parameter '?', an expression, or
 * another column, sqlite3_vtab_rhs_value() will often
 * fail with SQLITE_NOTFOUND.
 *
 * In that case, we simply fall back to the thesis
 * heuristic: 2 candidate blocks.
 * -------------------------------------------------- */
static int brinBestIndex(sqlite3_vtab *pVtab,
                         sqlite3_index_info *pIdxInfo)
{
    BrinVtab *v = (BrinVtab*)pVtab;

    DEBUG_PRINT("[BRIN] brinBestIndex()\n");
    DEBUG_PRINT("total_blocks currently known: %d\n", v->total_blocks);

    int minTerm = -1;   /* column 0: min <= ? */
    int maxTerm = -1;   /* column 1: max >= ? */

    /* Safe default outputs */
    pIdxInfo->idxNum = 0;
    pIdxInfo->idxStr = NULL;
    pIdxInfo->needToFreeIdxStr = 0;
    pIdxInfo->orderByConsumed = 0;

    /* --------------------------------------------
     * STEP 1
     * Search for the two BRIN constraints we need.
     *
     * We only support:
     *   column 0 (min) with <=
     *   column 1 (max) with >=
     * -------------------------------------------- */
    for (int i = 0; i < pIdxInfo->nConstraint; i++) {
        const struct sqlite3_index_constraint *c =
            &pIdxInfo->aConstraint[i];

        DEBUG_PRINT("Constraint %d usable=%d column=%d op=%d\n",
                    i, c->usable, c->iColumn, c->op);

        if (!c->usable) {
            DEBUG_PRINT("Skipping constraint %d: not usable\n", i);
            continue;
        }

        /* block_min <= high */
        if (c->iColumn == 0 &&
            c->op == SQLITE_INDEX_CONSTRAINT_LE)
        {
            minTerm = i;
            DEBUG_PRINT("Detected usable BRIN constraint: min <= ?\n");
        }
        /* block_max >= low */
        else if (c->iColumn == 1 &&
                 c->op == SQLITE_INDEX_CONSTRAINT_GE)
        {
            maxTerm = i;
            DEBUG_PRINT("Detected usable BRIN constraint: max >= ?\n");
        }
    }

    /* --------------------------------------------
     * STEP 2
     * Accept only the full BRIN range pattern.
     *
     * If both bounds exist:
     *   - pass them to xFilter()
     *   - try to compute exact candidate blocks
     *   - otherwise fallback to rows=2, cost=2.0
     *
     * If not:
     *   - discourage planner from using this path
     * -------------------------------------------- */
    if (minTerm >= 0 && maxTerm >= 0) {

        /* Pass RHS(min <= ?) as argv[0] */
        pIdxInfo->aConstraintUsage[minTerm].argvIndex = 1;
        pIdxInfo->aConstraintUsage[minTerm].omit = 1;

        /* Pass RHS(max >= ?) as argv[1] */
        pIdxInfo->aConstraintUsage[maxTerm].argvIndex = 2;
        pIdxInfo->aConstraintUsage[maxTerm].omit = 1;

        sqlite3_value *pHigh = NULL;  /* RHS of min <= high */
        sqlite3_value *pLow  = NULL;  /* RHS of max >= low  */

        int rcHigh = sqlite3_vtab_rhs_value(pIdxInfo, minTerm, &pHigh);
        int rcLow  = sqlite3_vtab_rhs_value(pIdxInfo, maxTerm, &pLow);

        if (rcHigh == SQLITE_OK &&
            rcLow  == SQLITE_OK &&
            pHigh != NULL &&
            pLow  != NULL &&
            v->total_blocks > 0)
        {
            DEBUG_PRINT("RHS values available in xBestIndex\n");

            /* ------------------------------------
             * TYPE-SPECIFIC exact candidate count
             * ------------------------------------ */
            if (v->affinity == BRIN_TYPE_INTEGER ||
                v->affinity == BRIN_TYPE_REAL)
            {
                double high = sqlite3_value_double(pHigh);
                double low  = sqlite3_value_double(pLow);

                if (low > high) {
                    double tmp = low;
                    low = high;
                    high = tmp;
                }

                DEBUG_PRINT("Planning numeric range normalized to [%.6f, %.6f]\n",
                            low, high);

                /* First block with block_max >= low */
                int left = 0;
                int right = v->total_blocks - 1;
                int mid;
                int start = v->total_blocks;

                while (left <= right) {
                    mid = (left + right) / 2;
                    BrinRange *r = &v->ranges[mid];

                    if (r->u.num.max >= low) {
                        start = mid;
                        right = mid - 1;
                    } else {
                        left = mid + 1;
                    }
                }

                /* Last block with block_min <= high */
                left = 0;
                right = v->total_blocks - 1;
                int end = -1;

                while (left <= right) {
                    mid = (left + right) / 2;
                    BrinRange *r = &v->ranges[mid];

                    if (r->u.num.min <= high) {
                        end = mid;
                        left = mid + 1;
                    } else {
                        right = mid - 1;
                    }
                }

                if (start != v->total_blocks && end >= start) {
                    int candidate_blocks = end - start + 1;
                    pIdxInfo->estimatedRows = candidate_blocks;
                    pIdxInfo->estimatedCost = (double)candidate_blocks;

                    DEBUG_PRINT("Exact numeric candidate block interval in xBestIndex: [%d, %d]\n",
                                start, end);
                    DEBUG_PRINT("Exact numeric candidate blocks in xBestIndex: %d\n",
                                candidate_blocks);
                } else {
                    pIdxInfo->estimatedRows = 1;
                    pIdxInfo->estimatedCost = 1.0;

                    DEBUG_PRINT("Numeric literal range produces no candidate blocks\n");
                    DEBUG_PRINT("Using minimal estimate rows=1 cost=1.0\n");
                }
            }
            else if (v->affinity == BRIN_TYPE_TEXT)
            {
                const char *high = (const char*)sqlite3_value_text(pHigh);
                const char *low  = (const char*)sqlite3_value_text(pLow);

                if (brinTextCmp(low, high) > 0) {
                    const char *tmp = low;
                    low = high;
                    high = tmp;
                }

                DEBUG_PRINT("Planning TEXT range normalized to [%s, %s]\n",
                            low ? low : "(null)",
                            high ? high : "(null)");

                /* First block with block_max >= low */
                int left = 0;
                int right = v->total_blocks - 1;
                int mid;
                int start = v->total_blocks;

                while (left <= right) {
                    mid = (left + right) / 2;
                    BrinRange *r = &v->ranges[mid];

                    if (brinTextCmp(r->u.txt.max, low) >= 0) {
                        start = mid;
                        right = mid - 1;
                    } else {
                        left = mid + 1;
                    }
                }

                /* Last block with block_min <= high */
                left = 0;
                right = v->total_blocks - 1;
                int end = -1;

                while (left <= right) {
                    mid = (left + right) / 2;
                    BrinRange *r = &v->ranges[mid];

                    if (brinTextCmp(r->u.txt.min, high) <= 0) {
                        end = mid;
                        left = mid + 1;
                    } else {
                        right = mid - 1;
                    }
                }

                if (start != v->total_blocks && end >= start) {
                    int candidate_blocks = end - start + 1;
                    pIdxInfo->estimatedRows = candidate_blocks;
                    pIdxInfo->estimatedCost = (double)candidate_blocks;

                    DEBUG_PRINT("Exact TEXT candidate block interval in xBestIndex: [%d, %d]\n",
                                start, end);
                    DEBUG_PRINT("Exact TEXT candidate blocks in xBestIndex: %d\n",
                                candidate_blocks);
                } else {
                    pIdxInfo->estimatedRows = 1;
                    pIdxInfo->estimatedCost = 1.0;

                    DEBUG_PRINT("TEXT literal range produces no candidate blocks\n");
                    DEBUG_PRINT("Using minimal estimate rows=1 cost=1.0\n");
                }
            }
            else {
                /* Defensive fallback */
                pIdxInfo->estimatedRows = 2;
                pIdxInfo->estimatedCost = 2.0;

                DEBUG_PRINT("Unknown affinity in xBestIndex, using fallback rows=2 cost=2.0\n");
            }
        } else {
            /* RHS values not available at planning time */
            pIdxInfo->estimatedRows = 2;
            pIdxInfo->estimatedCost = 2.0;

            DEBUG_PRINT("RHS values NOT available in xBestIndex\n");
            DEBUG_PRINT("Using thesis fallback estimatedRows=2 estimatedCost=2.0\n");
        }

        /* Natural BRIN order by start_rowid ASC */
        if (pIdxInfo->nOrderBy == 1 &&
            pIdxInfo->aOrderBy[0].iColumn == 2 &&
            pIdxInfo->aOrderBy[0].desc == 0)
        {
            pIdxInfo->orderByConsumed = 1;
            DEBUG_PRINT("ORDER BY start_rowid ASC consumed\n");
        }

    } else {
        int blocks = (v->total_blocks > 0) ? v->total_blocks : 1;

        pIdxInfo->estimatedRows = blocks;
        pIdxInfo->estimatedCost = 1000000.0;

        DEBUG_PRINT("BRIN plan rejected: both bounds are required\n");
        DEBUG_PRINT("Fallback estimatedRows=%lld estimatedCost=%.2f\n",
                    pIdxInfo->estimatedRows,
                    pIdxInfo->estimatedCost);
    }

    return SQLITE_OK;
}


/* --------------------------------------------------
 * xFilter
 *
 * PURPOSE
 * -------
 * Called during query execution.
 *
 * SQLite has already selected the access path, and now
 * asks the virtual table cursor to start scanning the
 * matching virtual rows.
 *
 * ARGUMENTS
 * ---------
 * By convention from xBestIndex():
 *
 *   argv[0] = high   from (min <= high)
 *   argv[1] = low    from (max >= low)
 *
 * WHAT THIS METHOD DOES
 * ---------------------
 * 1. Refresh BRIN incrementally for append-only inserts
 * 2. Find the first candidate block
 * 3. Find the last candidate block
 * 4. Initialize the cursor over that candidate interval
 *
 * IMPORTANT
 * ---------
 * The virtual table returns BRIN BLOCK SUMMARIES.
 * The final exact predicate over the base table must
 * still be applied by the SQL query after the JOIN.
 *
 * SUPPORTED DATA TYPES
 * --------------------
 * - INTEGER
 * - REAL
 * - TEXT (ISO 8601 conventions only)
 * -------------------------------------------------- */
static int brinFilter(
    sqlite3_vtab_cursor *cur,
    int idxNum,
    const char *idxStr,
    int argc,
    sqlite3_value **argv)
{
    BrinCursor *c = (BrinCursor*)cur;
    BrinVtab   *v = c->v;

    (void)idxNum;
    (void)idxStr;

    DEBUG_PRINT("[BRIN] brinFilter()\n");

    c->eof = 1;

    /* Our only supported BRIN plan requires exactly
       two arguments: high and low. */
    if (argc != 2) {
        DEBUG_PRINT("xFilter called without the required 2 arguments\n");
        return SQLITE_OK;
    }

    /* Keep BRIN synchronized with append-only inserts */
    brinIncrementalUpdate(v);

    if (v->total_blocks == 0) {
        DEBUG_PRINT("BRIN index is empty\n");
        return SQLITE_OK;
    }

    /* --------------------------------------------
     * NUMERIC branch
     * -------------------------------------------- */
    if (v->affinity == BRIN_TYPE_INTEGER ||
        v->affinity == BRIN_TYPE_REAL)
    {
        double high = sqlite3_value_double(argv[0]);
        double low  = sqlite3_value_double(argv[1]);

        if (low > high) {
            double tmp = low;
            low = high;
            high = tmp;
        }

        DEBUG_PRINT("Execution numeric range normalized to [%.6f, %.6f]\n",
                    low, high);

        c->low = (sqlite3_int64)low;
        c->high = (sqlite3_int64)high;

        /* First block with block_max >= low */
        int left = 0;
        int right = v->total_blocks - 1;
        int mid;
        int start = v->total_blocks;

        while (left <= right) {
            mid = (left + right) / 2;
            BrinRange *r = &v->ranges[mid];

            if (r->u.num.max >= low) {
                start = mid;
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        if (start == v->total_blocks) {
            DEBUG_PRINT("No numeric block satisfies block_max >= low\n");
            return SQLITE_OK;
        }

        /* Last block with block_min <= high */
        left = 0;
        right = v->total_blocks - 1;
        int end = -1;

        while (left <= right) {
            mid = (left + right) / 2;
            BrinRange *r = &v->ranges[mid];

            if (r->u.num.min <= high) {
                end = mid;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        if (end < start) {
            DEBUG_PRINT("No overlapping numeric candidate interval found\n");
            return SQLITE_OK;
        }

        c->start_block   = start;
        c->end_block     = end;
        c->current_block = start;
        c->eof = 0;

        int candidate_blocks = end - start + 1;

        DEBUG_PRINT("Numeric candidate block interval: [%d, %d]\n", start, end);
        DEBUG_PRINT("Numeric candidate blocks count: %d\n", candidate_blocks);
        DEBUG_PRINT("Total BRIN blocks: %d\n", v->total_blocks);
        DEBUG_PRINT("Numeric candidate block selectivity: %.4f%%\n",
                    100.0 * candidate_blocks / (double)v->total_blocks);

        return SQLITE_OK;
    }

    /* --------------------------------------------
     * TEXT branch
     *
     * Thesis assumption:
     * values follow ISO 8601 conventions, so
     * lexicographic order is meaningful.
     * -------------------------------------------- */
    if (v->affinity == BRIN_TYPE_TEXT)
    {
        const char *high = (const char*)sqlite3_value_text(argv[0]);
        const char *low  = (const char*)sqlite3_value_text(argv[1]);

        if (brinTextCmp(low, high) > 0) {
            const char *tmp = low;
            low = high;
            high = tmp;
        }

        DEBUG_PRINT("Execution TEXT range normalized to [%s, %s]\n",
                    low ? low : "(null)",
                    high ? high : "(null)");

        /* First block with block_max >= low */
        int left = 0;
        int right = v->total_blocks - 1;
        int mid;
        int start = v->total_blocks;

        while (left <= right) {
            mid = (left + right) / 2;
            BrinRange *r = &v->ranges[mid];

            if (brinTextCmp(r->u.txt.max, low) >= 0) {
                start = mid;
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        if (start == v->total_blocks) {
            DEBUG_PRINT("No TEXT block satisfies block_max >= low\n");
            return SQLITE_OK;
        }

        /* Last block with block_min <= high */
        left = 0;
        right = v->total_blocks - 1;
        int end = -1;

        while (left <= right) {
            mid = (left + right) / 2;
            BrinRange *r = &v->ranges[mid];

            if (brinTextCmp(r->u.txt.min, high) <= 0) {
                end = mid;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        if (end < start) {
            DEBUG_PRINT("No overlapping TEXT candidate interval found\n");
            return SQLITE_OK;
        }

        c->start_block   = start;
        c->end_block     = end;
        c->current_block = start;
        c->eof = 0;

        int candidate_blocks = end - start + 1;

        DEBUG_PRINT("TEXT candidate block interval: [%d, %d]\n", start, end);
        DEBUG_PRINT("TEXT candidate blocks count: %d\n", candidate_blocks);
        DEBUG_PRINT("Total BRIN blocks: %d\n", v->total_blocks);
        DEBUG_PRINT("TEXT candidate block selectivity: %.4f%%\n",
                    100.0 * candidate_blocks / (double)v->total_blocks);

        return SQLITE_OK;
    }

    /* Defensive fallback */
    DEBUG_PRINT("Unsupported affinity in xFilter\n");
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


/* --------------------------------------------------
 * xColumn
 *
 * PURPOSE
 * -------
 * Return the value of the requested column for the
 * current virtual row.
 *
 * VIRTUAL TABLE SCHEMA
 * --------------------
 * column 0 -> min
 * column 1 -> max
 * column 2 -> start_rowid
 * column 3 -> end_rowid
 *
 * Each virtual row represents one BRIN block summary.
 *
 * TYPE HANDLING
 * -------------
 * - INTEGER -> return int64
 * - REAL    -> return double
 * - TEXT    -> return UTF-8 string
 *
 * For the TEXT prototype used in this thesis, values
 * are assumed to follow ISO 8601 conventions, so the
 * stored min/max strings preserve the same lexicographic
 * order used by xFilter() and xBestIndex().
 * -------------------------------------------------- */
static int brinColumn(
    sqlite3_vtab_cursor *cur,
    sqlite3_context *ctx,
    int col)
{
    DEBUG_PRINT("[BRIN] brinColumn()\n");

    BrinCursor *c = (BrinCursor*)cur;

    /* If cursor is at EOF, return NULL defensively. */
    if (c->eof) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    BrinRange *r = &c->v->ranges[c->current_block];

    switch (col)
    {
        case 0: /* min */
            if (r->type == BRIN_TYPE_INTEGER) {
                sqlite3_result_int64(ctx, (sqlite3_int64)r->u.num.min);
            }
            else if (r->type == BRIN_TYPE_REAL) {
                sqlite3_result_double(ctx, r->u.num.min);
            }
            else if (r->type == BRIN_TYPE_TEXT) {
                sqlite3_result_text(ctx,
                                    r->u.txt.min ? r->u.txt.min : "",
                                    -1,
                                    SQLITE_TRANSIENT);
            }
            else {
                sqlite3_result_null(ctx);
            }
            break;

        case 1: /* max */
            if (r->type == BRIN_TYPE_INTEGER) {
                sqlite3_result_int64(ctx, (sqlite3_int64)r->u.num.max);
            }
            else if (r->type == BRIN_TYPE_REAL) {
                sqlite3_result_double(ctx, r->u.num.max);
            }
            else if (r->type == BRIN_TYPE_TEXT) {
                sqlite3_result_text(ctx,
                                    r->u.txt.max ? r->u.txt.max : "",
                                    -1,
                                    SQLITE_TRANSIENT);
            }
            else {
                sqlite3_result_null(ctx);
            }
            break;

        case 2: /* start_rowid */
            sqlite3_result_int64(ctx, r->start_rowid);
            break;

        case 3: /* end_rowid */
            sqlite3_result_int64(ctx, r->end_rowid);
            break;

        default:
            sqlite3_result_null(ctx);
            break;
    }

    return SQLITE_OK;
}


/* --------------------------------------------------
 * xRowid
 *
 * PURPOSE
 * -------
 * Return a unique rowid for the current virtual row.
 *
 * In this implementation, the block index itself is a
 * natural unique identifier for each BRIN summary row.
 * -------------------------------------------------- */
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

