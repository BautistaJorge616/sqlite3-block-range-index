#include <sqlite3ext.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <time.h>

#ifdef DEBUG
    #define DEBUG_PRINT(...) printf(__VA_ARGS__)
#else
    #define DEBUG_PRINT(...)
#endif

SQLITE_EXTENSION_INIT1

/* =========================================================
 * 1. Internal data structures
 * ========================================================= */

/* --------------------------------------------------
 * BrinAffinity
 *
 * PURPOSE
 * -------
 * Identify the logical type of the indexed column.
 *
 * The prototype supports:
 *   - INTEGER
 *   - REAL
 *   - TEXT
 *
 * TEXT support is intentionally restricted by thesis
 * assumptions to globally ordered values following
 * ISO 8601 lexical conventions.
 * -------------------------------------------------- */
typedef enum {
    BRIN_TYPE_INTEGER,
    BRIN_TYPE_REAL,
    BRIN_TYPE_TEXT
} BrinAffinity;


/* --------------------------------------------------
 * BrinRange
 *
 * PURPOSE
 * -------
 * Represent one BRIN block summary.
 *
 * Each BRIN block stores:
 *   - the minimum value in the block
 *   - the maximum value in the block
 *   - the first rowid covered by the block
 *   - the last  rowid covered by the block
 *
 * TYPE STORAGE
 * ------------
 * INTEGER and REAL are stored in the numeric branch.
 * TEXT is stored as dynamically allocated UTF-8 strings.
 *
 * THESIS MODEL
 * ------------
 * Since data is globally ordered ascending:
 *   - the first value entering a block becomes its min
 *   - the last  value entering a block becomes its max
 * -------------------------------------------------- */
typedef struct BrinRange {
    BrinAffinity type;

    union {
        struct {
            double min;
            double max;
        } num;

        struct {
            char *min;
            char *max;
        } txt;
    } u;

    sqlite3_int64 start_rowid;
    sqlite3_int64 end_rowid;
} BrinRange;


/* --------------------------------------------------
 * BrinVtab
 *
 * PURPOSE
 * -------
 * Represent the virtual table instance.
 *
 * This structure contains both:
 *   - SQLite-required virtual table state
 *   - internal BRIN metadata and in-memory summaries
 *
 * MAIN FIELDS
 * -----------
 * table, column:
 *   identify the physical base table and indexed column
 *
 * block_size:
 *   number of base-table rows summarized by one BRIN block
 *
 * ranges:
 *   dynamically allocated array of BRIN block summaries
 *
 * total_blocks:
 *   number of valid block summaries currently stored
 *
 * last_indexed_rowid:
 *   highest rowid already reflected in the BRIN structure
 *
 * last_block_size:
 *   how many rows are currently stored in the last block
 *
 * index_ready:
 *   indicates whether the BRIN structure has already been
 *   built and can be incrementally updated
 * -------------------------------------------------- */
typedef struct {
    sqlite3_vtab base;
    char *table;
    char *column;
    int block_size;
    BrinAffinity affinity;

    BrinRange *ranges;
    int total_blocks;

    sqlite3_int64 last_indexed_rowid;
    int last_block_size;

    int index_ready;

    sqlite3 *db;
} BrinVtab;


/* --------------------------------------------------
 * BrinCursor
 *
 * PURPOSE
 * -------
 * Represent one active scan over the BRIN virtual table.
 *
 * In this prototype, a cursor iterates over candidate
 * BRIN block summaries selected by xFilter().
 *
 * FIELDS
 * ------
 * start_block / end_block:
 *   candidate block interval selected for the current query
 *
 * current_block:
 *   current position inside that interval
 *
 * eof:
 *   indicates whether the scan has finished
 *
 * low / high:
 *   optional storage of the normalized query range,
 *   useful for debugging and possible future extensions
 * -------------------------------------------------- */
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


/* =========================================================
 * 2. Internal helper functions
 * ========================================================= */

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


/* --------------------------------------------------
 * get_affinity
 *
 * PURPOSE
 * -------
 * Infer a simplified logical affinity from the declared
 * type name of the indexed column.
 *
 * INPUT
 * -----
 * declared_type:
 *   declared SQLite type string obtained from metadata,
 *   for example "INTEGER", "TEXT", "REAL", "DATETIME"
 *
 * OUTPUT
 * ------
 * Returns one of these strings:
 *   - "INTEGER"
 *   - "REAL"
 *   - "TEXT"
 *
 * or NULL if the type is not supported by the prototype.
 *
 * DESIGN NOTE
 * -----------
 * The function normalizes the type name to uppercase and
 * then applies a simplified SQLite-style affinity mapping.
 *
 * THESIS-SPECIFIC CHOICE
 * ----------------------
 * DATE and DATETIME are treated as TEXT because the thesis
 * prototype only supports textual temporal values when they
 * are stored in ISO 8601 format.
 * -------------------------------------------------- */
static const char* get_affinity(const char *declared_type)
{
    if (!declared_type) return NULL;

    DEBUG_PRINT("[BRIN] get_affinity()\n");

    char type[64];
    snprintf(type, sizeof(type), "%s", declared_type);
    for (int i = 0; type[i]; i++) {
        type[i] = toupper((unsigned char)type[i]);
    }

    DEBUG_PRINT("type:%s\n", type);

    if (strstr(type, "INT")) return "INTEGER";
    if (strstr(type, "CHAR") || strstr(type, "CLOB") || strstr(type, "TEXT"))
        return "TEXT";
    if (strstr(type, "REAL") || strstr(type, "FLOA") || strstr(type, "DOUB"))
        return "REAL";
    if (strstr(type, "DATE") || strstr(type, "DATETIME"))
        return "TEXT";

    return NULL;
}


/* --------------------------------------------------
 * get_max_rowid
 *
 * PURPOSE
 * -------
 * Return the current maximum rowid from the base table.
 *
 * WHY THIS CAN BE USEFUL
 * ----------------------
 * This helper is not central to the current execution path,
 * but it is useful when debugging or validating whether the
 * BRIN structure is synchronized with the base table.
 *
 * RETURN VALUE
 * ------------
 * - maximum rowid if the table contains rows
 * - 0 if the table is empty
 * - last_indexed_rowid as a defensive fallback if query
 *   preparation fails
 * -------------------------------------------------- */
static sqlite3_int64 get_max_rowid(BrinVtab *v)
{
    DEBUG_PRINT("[BRIN] get_max_rowid()\n");

    sqlite3_stmt *stmt = NULL;
    sqlite3_int64 max_rowid = 0;
    char sql[256];

    snprintf(sql, sizeof(sql),
             "SELECT MAX(rowid) FROM %s;", v->table);

    int rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("get_max_rowid: prepare failed: %s\n",
               sqlite3_errmsg(v->db));
        return v->last_indexed_rowid;
    }

    rc = sqlite3_step(stmt);

    if (rc == SQLITE_ROW) {
        if (sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
            max_rowid = sqlite3_column_int64(stmt, 0);
        } else {
            max_rowid = 0;
        }
    }

    sqlite3_finalize(stmt);

    DEBUG_PRINT("max_rowid:%lld\n", max_rowid);

    return max_rowid;
}


/* =========================================================
 * 3. BRIN build and maintenance
 * ========================================================= */

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
    if (!v || !v->db || !v->index_ready)
        return SQLITE_OK;

    DEBUG_PRINT("[BRIN] brinIncrementalUpdate()\n");
    DEBUG_PRINT("last_indexed_rowid before update: %lld\n", v->last_indexed_rowid);
    DEBUG_PRINT("last_block_size before update   : %d\n", v->last_block_size);
    DEBUG_PRINT("total_blocks before update      : %d\n", v->total_blocks);

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

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        found_new_rows = 1;

        sqlite3_int64 rowid = sqlite3_column_int64(stmt, 0);

        DEBUG_PRINT("Processing appended rowid=%lld\n", rowid);

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

        BrinRange *lastBlock = &v->ranges[v->total_blocks - 1];

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

        v->last_indexed_rowid = rowid;
    }

    if (rc != SQLITE_DONE) {
        DEBUG_PRINT("brinIncrementalUpdate: step failed: %s\n",
                    sqlite3_errmsg(v->db));
        sqlite3_finalize(stmt);
        return rc;
    }

    sqlite3_finalize(stmt);

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

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        sqlite3_int64 rowid = sqlite3_column_int64(stmt, 0);
        last_rowid_seen = rowid;

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

    v->last_indexed_rowid = last_rowid_seen;
    v->last_block_size = block_pos;
    v->index_ready = 1;

    DEBUG_PRINT("Total blocks         : %d\n", v->total_blocks);
    DEBUG_PRINT("Last indexed rowid   : %lld\n", v->last_indexed_rowid);
    DEBUG_PRINT("Last block size      : %d\n", v->last_block_size);

    return SQLITE_OK;
}


/* =========================================================
 * 4. SQLite virtual table callbacks
 * ========================================================= */

/* --------------------------------------------------
 * brinConnect
 *
 * PURPOSE
 * -------
 * Create and initialize a virtual table instance.
 *
 * SQLite calls xCreate/xConnect when the module is
 * instantiated. In this prototype both callbacks share
 * the same implementation.
 *
 * RESPONSIBILITIES
 * ----------------
 * 1. Allocate the BrinVtab structure
 * 2. Read module arguments:
 *      - base table name
 *      - indexed column name
 *      - block size
 * 3. Inspect base column metadata
 * 4. Infer supported affinity
 * 5. Declare the virtual table schema visible to SQLite
 * 6. Build the initial in-memory BRIN structure
 *
 * MODULE ARGUMENTS
 * ----------------
 * Expected layout:
 *   argv[3] -> base table name
 *   argv[4] -> indexed column name
 *   argv[5] -> block size
 *
 * RETURN VALUE
 * ------------
 * SQLITE_OK on success, or an SQLite error code.
 * -------------------------------------------------- */
static int brinConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
    (void)pAux;
    (void)pzErr;

    if (argc < 6) {
        fprintf(stderr, "brinConnect: not enough args (argc=%d)\n", argc);
        return SQLITE_ERROR;
    }

    DEBUG_PRINT("[BRIN] brinConnect()\n");

    BrinVtab *v = (BrinVtab*)sqlite3_malloc(sizeof(BrinVtab));
    if (v == NULL) return SQLITE_NOMEM;
    memset(v, 0, sizeof(BrinVtab));

    v->table      = sqlite3_mprintf("%s", argv[3]);
    v->column     = sqlite3_mprintf("%s", argv[4]);
    v->block_size = atoi(argv[5]);
    v->db         = db;

    const char *dataType, *collation;
    int notNull, isPK, isAuto;
    int rc = SQLITE_OK;

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
        return rc;
    }

    const char *affinity = get_affinity(dataType);

    if (!affinity) {
        fprintf(stderr, "NOT SUPPORTED: %s\n", dataType);
        sqlite3_free(v->table);
        sqlite3_free(v->column);
        sqlite3_free(v);
        return SQLITE_ERROR;
    }

    if (strcmp(affinity, "INTEGER") == 0) {
        rc = sqlite3_declare_vtab(db,
            "CREATE TABLE x( min INTEGER, max INTEGER, "
            "start_rowid INT, end_rowid INT)"
        );
        v->affinity = BRIN_TYPE_INTEGER;
    }
    if (strcmp(affinity, "REAL") == 0) {
        rc = sqlite3_declare_vtab(db,
            "CREATE TABLE x( min REAL, max REAL, "
            "start_rowid INT, end_rowid INT)"
        );
        v->affinity = BRIN_TYPE_REAL;
    }
    if (strcmp(affinity, "TEXT") == 0) {
        rc = sqlite3_declare_vtab(db,
            "CREATE TABLE x( min TEXT, max TEXT, "
            "start_rowid INT, end_rowid INT)"
        );
        v->affinity = BRIN_TYPE_TEXT;
    }

    if (rc != SQLITE_OK) {
        fprintf(stderr,
                "brinConnect: declare_vtab failed: %s\n",
                sqlite3_errmsg(db));

        sqlite3_free(v->table);
        sqlite3_free(v->column);
        sqlite3_free(v);

        return rc;
    }

    *ppVtab = (sqlite3_vtab*)v;

    rc = brinBuildIndex(v);
    if (rc != SQLITE_OK)
        return rc;

    return SQLITE_OK;
}


/* --------------------------------------------------
 * xOpen
 *
 * PURPOSE
 * -------
 * Allocate and initialize a new cursor for this
 * virtual table scan.
 *
 * A cursor represents one active scan over the
 * virtual table.
 * -------------------------------------------------- */
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


/* --------------------------------------------------
 * xClose
 *
 * PURPOSE
 * -------
 * Destroy a cursor previously allocated by xOpen().
 *
 * SQLite calls this when the scan over the virtual table
 * is no longer needed.
 * -------------------------------------------------- */
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

    int minTerm = -1;
    int maxTerm = -1;

    pIdxInfo->idxNum = 0;
    pIdxInfo->idxStr = NULL;
    pIdxInfo->needToFreeIdxStr = 0;
    pIdxInfo->orderByConsumed = 0;

    for (int i = 0; i < pIdxInfo->nConstraint; i++) {
        const struct sqlite3_index_constraint *c =
            &pIdxInfo->aConstraint[i];

        DEBUG_PRINT("Constraint %d usable=%d column=%d op=%d\n",
                    i, c->usable, c->iColumn, c->op);

        if (!c->usable) {
            DEBUG_PRINT("Skipping constraint %d: not usable\n", i);
            continue;
        }

        if (c->iColumn == 0 &&
            c->op == SQLITE_INDEX_CONSTRAINT_LE)
        {
            minTerm = i;
            DEBUG_PRINT("Detected usable BRIN constraint: min <= ?\n");
        }
        else if (c->iColumn == 1 &&
                 c->op == SQLITE_INDEX_CONSTRAINT_GE)
        {
            maxTerm = i;
            DEBUG_PRINT("Detected usable BRIN constraint: max >= ?\n");
        }
    }

    if (minTerm >= 0 && maxTerm >= 0) {

        pIdxInfo->aConstraintUsage[minTerm].argvIndex = 1;
        pIdxInfo->aConstraintUsage[minTerm].omit = 1;

        pIdxInfo->aConstraintUsage[maxTerm].argvIndex = 2;
        pIdxInfo->aConstraintUsage[maxTerm].omit = 1;

        sqlite3_value *pHigh = NULL;
        sqlite3_value *pLow  = NULL;

        int rcHigh = sqlite3_vtab_rhs_value(pIdxInfo, minTerm, &pHigh);
        int rcLow  = sqlite3_vtab_rhs_value(pIdxInfo, maxTerm, &pLow);

        if (rcHigh == SQLITE_OK &&
            rcLow  == SQLITE_OK &&
            pHigh != NULL &&
            pLow  != NULL &&
            v->total_blocks > 0)
        {
            DEBUG_PRINT("RHS values available in xBestIndex\n");

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
                pIdxInfo->estimatedRows = 2;
                pIdxInfo->estimatedCost = 2.0;

                DEBUG_PRINT("Unknown affinity in xBestIndex, using fallback rows=2 cost=2.0\n");
            }
        } else {
            pIdxInfo->estimatedRows = 2;
            pIdxInfo->estimatedCost = 2.0;

            DEBUG_PRINT("RHS values NOT available in xBestIndex\n");
            DEBUG_PRINT("Using thesis fallback estimatedRows=2 estimatedCost=2.0\n");
        }

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

    if (argc != 2) {
        DEBUG_PRINT("xFilter called without the required 2 arguments\n");
        return SQLITE_OK;
    }

    brinIncrementalUpdate(v);

    if (v->total_blocks == 0) {
        DEBUG_PRINT("BRIN index is empty\n");
        return SQLITE_OK;
    }

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

    DEBUG_PRINT("Unsupported affinity in xFilter\n");
    return SQLITE_OK;
}


/* --------------------------------------------------
 * xNext
 *
 * PURPOSE
 * -------
 * Advance the cursor to the next virtual row.
 *
 * In this BRIN implementation, one virtual row
 * corresponds to one candidate BRIN block summary.
 * -------------------------------------------------- */
static int brinNext(sqlite3_vtab_cursor *cur)
{
    DEBUG_PRINT("[BRIN] brinNext()\n");

    BrinCursor *c = (BrinCursor*)cur;

    c->current_block++;

    if (c->current_block > c->end_block)
        c->eof = 1;

    return SQLITE_OK;
}


/* --------------------------------------------------
 * xEof
 *
 * PURPOSE
 * -------
 * Tell SQLite whether the cursor has finished.
 *
 * Return non-zero if the scan is done.
 * Return zero if a valid current row exists.
 * -------------------------------------------------- */
static int brinEof(sqlite3_vtab_cursor *cur)
{
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

    if (c->eof) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    BrinRange *r = &c->v->ranges[c->current_block];

    switch (col)
    {
        case 0:
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

        case 1:
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

        case 2:
            sqlite3_result_int64(ctx, r->start_rowid);
            break;

        case 3:
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


/* --------------------------------------------------
 * xDisconnect
 *
 * PURPOSE
 * -------
 * Release all resources associated with one virtual table
 * instance.
 *
 * RESPONSIBILITIES
 * ----------------
 * - free TEXT min/max strings inside BRIN blocks
 * - free the ranges array
 * - free copied table/column names
 * - free the BrinVtab structure itself
 *
 * SQLite calls xDisconnect when a connection stops using
 * the virtual table instance.
 * -------------------------------------------------- */
static int brinDisconnect(sqlite3_vtab *pVTab)
{
    BrinVtab *v = (BrinVtab*)pVTab;
    DEBUG_PRINT("[BRIN] brinDisconnect()\n");

    if (v) {
        if (v->ranges) {
            if (v->affinity == BRIN_TYPE_TEXT) {
                for (int i = 0; i < v->total_blocks; i++) {
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


/* --------------------------------------------------
 * xDestroy
 *
 * PURPOSE
 * -------
 * Destroy a virtual table instance.
 *
 * In this prototype, destruction requires the same
 * resource cleanup as xDisconnect(), so both callbacks
 * share the same implementation path.
 * -------------------------------------------------- */
static int brinDestroy(sqlite3_vtab *pVTab)
{
    DEBUG_PRINT("[BRIN] brinDestroy()\n");
    return brinDisconnect(pVTab);
}


/* =========================================================
 * 5. Module registration
 * ========================================================= */

/* --------------------------------------------------
 * BrinModule
 *
 * PURPOSE
 * -------
 * Describe the SQLite virtual table module by mapping
 * each required callback slot to the implementation
 * provided by this prototype.
 *
 * CALLBACK COVERAGE
 * -----------------
 * This prototype implements the core read-only behavior
 * required for:
 *   - connection/creation
 *   - query planning
 *   - scan execution
 *   - cursor navigation
 *   - cleanup
 *
 * Unused callbacks remain NULL.
 * -------------------------------------------------- */
static sqlite3_module BrinModule = {
  2,                /* iVersion */
  brinConnect,      /* xCreate */
  brinConnect,      /* xConnect */
  brinBestIndex,    /* xBestIndex */
  brinDisconnect,   /* xDisconnect */
  brinDestroy,      /* xDestroy */
  brinOpen,         /* xOpen */
  brinClose,        /* xClose */
  brinFilter,       /* xFilter */
  brinNext,         /* xNext */
  brinEof,          /* xEof */
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


/* --------------------------------------------------
 * sqlite3_brin_init
 *
 * PURPOSE
 * -------
 * Entry point called by SQLite when the shared library
 * is loaded with .load.
 *
 * RESPONSIBILITIES
 * ----------------
 * - initialize the SQLite extension API table
 * - register the virtual table module under the name
 *   "brin"
 *
 * USAGE
 * -----
 * Once loaded, the module can be instantiated with:
 *
 *   CREATE VIRTUAL TABLE ... USING brin(...)
 *
 * RETURN VALUE
 * ------------
 * SQLITE_OK on success, or the error code returned by
 * sqlite3_create_module().
 * -------------------------------------------------- */
int sqlite3_brin_init(sqlite3 *db, char **pzErrMsg,
                      const sqlite3_api_routines *pApi)
{
    (void)pzErrMsg;

    SQLITE_EXTENSION_INIT2(pApi);

    int rc = sqlite3_create_module(db, "brin", &BrinModule, 0);

    if (rc != SQLITE_OK) {
        printf("The module could not be created.\n");
    }

    return rc;
}
