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
 * BrinOutputRange
 *
 * PURPOSE
 * -------
 * Represent one row produced by the virtual table cursor
 * after applying range coalescing.
 *
 * A BrinOutputRange may represent:
 *
 *   - exactly one BRIN block
 *   - multiple contiguous BRIN blocks fused together
 *
 * WHY THIS EXISTS
 * ---------------
 * The base BRIN array v->ranges stores one summary per
 * physical/logical BRIN block.
 *
 * However, during query execution, if several candidate
 * blocks are contiguous and have the same recheck status,
 * we can expose them as a single virtual row.
 *
 * FIELDS
 * ------
 * start_block:
 *   First BRIN block included in this output segment.
 *
 * end_block:
 *   Last BRIN block included in this output segment.
 *
 * needs_recheck:
 *   0 -> all rows in this segment are guaranteed to satisfy
 *        the range predicate.
 *
 *   1 -> this segment is a boundary/partial segment and
 *        the base table predicate must still be checked.
 * -------------------------------------------------- */
typedef struct BrinOutputRange {
    int start_block;
    int end_block;
    int needs_recheck;
} BrinOutputRange;


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
 *
 * TEXT values are assumed to be ISO-8601 datetime
 * strings. Internally, they are converted to Unix epoch
 * seconds and stored as integers.
 *
 * This avoids per-range heap allocation for TEXT min/max
 * and allows numeric comparisons inside xBestIndex()
 * and xFilter().
 * -------------------------------------------------- */
typedef struct BrinRange {
    BrinAffinity type;

    union {
        struct {
            double min;
            double max;
        } num;

        struct {
            sqlite3_int64 min_epoch;
            sqlite3_int64 max_epoch;
        } txt;
    } u;

    sqlite3_int64 start_rowid;
    sqlite3_int64 end_rowid;
} BrinRange;

static double brinRangeMinAsDouble(const BrinRange *r);
static double brinRangeMaxAsDouble(const BrinRange *r);

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
 * BASE IDEA
 * ---------
 * xFilter() computes candidate BRIN blocks for a query
 * range [low, high].
 *
 * In the original implementation, the cursor returned one
 * virtual row per candidate block.
 *
 * In this version, xFilter() builds an output list of
 * coalesced ranges. Each output range may represent one
 * block or multiple contiguous blocks.
 *
 * This allows the virtual table to reduce:
 *
 *   - calls to xNext()
 *   - calls to xColumn()
 *   - outer-loop iterations in the join
 *
 * while keeping the result correct.
 * -------------------------------------------------- */
typedef struct {
    sqlite3_vtab_cursor base;

    BrinVtab *v;

    sqlite3_int64 low;
    sqlite3_int64 high;

    /*
     * Original candidate block interval.
     *
     * Kept for debug and for compatibility with the existing
     * mental model.
     */
    int start_block;
    int end_block;

    /*
     * The old implementation used current_block directly.
     *
     * With coalescing, the active cursor position is now
     * current_output.
     */
    int current_block;

    /*
     * Coalesced output segments.
     *
     * These are built in xFilter() and consumed by xNext(),
     * xColumn(), and xRowid().
     */
    BrinOutputRange *output_ranges;
    int output_count;
    int output_capacity;
    int current_output;

    /*
     * Optional filter derived from:
     *
     *   b.needs_recheck = 0
     *   b.needs_recheck = 1
     *
     * Values:
     *   -1 -> return all output ranges
     *    0 -> return only fully-covered ranges
     *    1 -> return only boundary/recheck ranges
     */
    int needs_recheck_filter;

    int eof;
} BrinCursor;


/* =========================================================
 * 2. Internal helper functions
 * ========================================================= */

/* --------------------------------------------------
 * brinResetOutputRanges
 *
 * PURPOSE
 * -------
 * Clear the coalesced output list owned by a cursor.
 *
 * WHEN USED
 * ---------
 * Called at the beginning of xFilter() before building a
 * new output list for the new query range.
 *
 * Also safe to call from xClose().
 * -------------------------------------------------- */
static void brinResetOutputRanges(BrinCursor *c)
{
    if (!c)
        return;

    if (c->output_ranges) {
        free(c->output_ranges);
        c->output_ranges = NULL;
    }

    c->output_count = 0;
    c->output_capacity = 0;
    c->current_output = 0;
}


/* --------------------------------------------------
 * brinBlockNeedsRecheck
 *
 * PURPOSE
 * -------
 * Decide whether one BRIN block needs row-level recheck
 * for the query range [low, high].
 *
 * RULE
 * ----
 * A block does NOT need recheck when it is fully covered:
 *
 *   block.min >= low
 *   block.max <= high
 *
 * In that case every value in the block is guaranteed to
 * be inside the query range.
 *
 * A block DOES need recheck when it intersects the query
 * range but is not fully covered. These are boundary blocks.
 *
 * RETURN VALUE
 * ------------
 * 0 -> no recheck needed
 * 1 -> recheck needed
 * -------------------------------------------------- */
static int brinBlockNeedsRecheck(
    BrinVtab *v,
    int block,
    double low,
    double high
){
    BrinRange *r;
    double block_min;
    double block_max;

    if (!v)
        return 1;

    if (block < 0)
        return 1;

    if (block >= v->total_blocks)
        return 1;

    r = &v->ranges[block];

    block_min = brinRangeMinAsDouble(r);
    block_max = brinRangeMaxAsDouble(r);

    if (block_min >= low) {
        if (block_max <= high) {
            return 0;
        }
    }

    return 1;
}


/* --------------------------------------------------
 * brinAppendOutputRange
 *
 * PURPOSE
 * -------
 * Append one output range to the cursor.
 *
 * COALESCING RULE
 * ---------------
 * If the new range is contiguous with the previous output
 * range and both have the same needs_recheck value, merge
 * them into a single output range.
 *
 * Example:
 *
 *   previous: blocks [10, 12], needs_recheck = 0
 *   new:      blocks [13, 14], needs_recheck = 0
 *
 * Result:
 *
 *   one range [10, 14], needs_recheck = 0
 *
 * This is the core of range coalescing.
 * -------------------------------------------------- */
static int brinAppendOutputRange(
    BrinCursor *c,
    int start_block,
    int end_block,
    int needs_recheck
){
    BrinOutputRange *last;
    BrinOutputRange *tmp;
    int new_capacity;

    if (!c)
        return SQLITE_ERROR;

    if (start_block > end_block)
        return SQLITE_OK;

    /*
     * Optional output filtering.
     *
     * If the query includes:
     *
     *   b.needs_recheck = 0
     *
     * then only fully-covered segments are returned.
     *
     * If the query includes:
     *
     *   b.needs_recheck = 1
     *
     * then only boundary/recheck segments are returned.
     */
    if (c->needs_recheck_filter != -1) {
        if (c->needs_recheck_filter != needs_recheck) {
            return SQLITE_OK;
        }
    }

    /*
     * If possible, merge into the previous output range.
     */
    if (c->output_count > 0) {
        last = &c->output_ranges[c->output_count - 1];

        if (last->needs_recheck == needs_recheck) {
            if (last->end_block + 1 == start_block) {
                last->end_block = end_block;
                return SQLITE_OK;
            }
        }
    }

    /*
     * Need a new output range.
     */
    if (c->output_count >= c->output_capacity) {
        if (c->output_capacity == 0) {
            new_capacity = 4;
        }
        else {
            new_capacity = c->output_capacity * 2;
        }

        tmp = realloc(
            c->output_ranges,
            (size_t)new_capacity * sizeof(BrinOutputRange)
        );

        if (!tmp)
            return SQLITE_NOMEM;

        c->output_ranges = tmp;
        c->output_capacity = new_capacity;
    }

    c->output_ranges[c->output_count].start_block = start_block;
    c->output_ranges[c->output_count].end_block = end_block;
    c->output_ranges[c->output_count].needs_recheck = needs_recheck;

    c->output_count++;

    return SQLITE_OK;
}


/* --------------------------------------------------
 * brinBuildOutputRanges
 *
 * PURPOSE
 * -------
 * Build the coalesced output range list for the current
 * query.
 *
 * INPUT
 * -----
 * start:
 *   First candidate BRIN block.
 *
 * end:
 *   Last candidate BRIN block.
 *
 * low/high:
 *   Query bounds in the normalized internal numeric format.
 *
 * BEHAVIOR
 * --------
 * For every candidate block:
 *
 *   1. Determine whether it needs row-level recheck.
 *   2. Append it to the cursor output list.
 *   3. Consecutive blocks with the same recheck status are
 *      automatically merged by brinAppendOutputRange().
 *
 * EXPECTED SHAPE FOR ORDERED DATA
 * -------------------------------
 * For one contiguous query range over ordered data, the
 * output is usually at most three segments:
 *
 *   left boundary     -> needs_recheck = 1
 *   fully-covered mid -> needs_recheck = 0
 *   right boundary    -> needs_recheck = 1
 * -------------------------------------------------- */
static int brinBuildOutputRanges(
    BrinCursor *c,
    int start,
    int end,
    double low,
    double high
){
    int rc;

    if (!c)
        return SQLITE_ERROR;

    if (start > end)
        return SQLITE_OK;

    for (int i = start; i <= end; i++) {
        int needs_recheck;

        needs_recheck =
            brinBlockNeedsRecheck(c->v, i, low, high);

        rc = brinAppendOutputRange(
            c,
            i,
            i,
            needs_recheck
        );

        if (rc != SQLITE_OK)
            return rc;
    }

    return SQLITE_OK;
}


/* --------------------------------------------------
 * brinEstimateOutputRangeCount
 *
 * PURPOSE
 * -------
 * Estimate how many virtual rows will be produced after
 * range coalescing.
 *
 * This mirrors brinBuildOutputRanges(), but does not
 * allocate memory.
 *
 * WHY THIS EXISTS
 * ---------------
 * xBestIndex() runs during query planning. It should be
 * cheap and should not build cursor state.
 *
 * This function gives the planner a better estimate of
 * how many virtual rows the vtab will output.
 * -------------------------------------------------- */
static int brinEstimateOutputRangeCount(
    BrinVtab *v,
    int start,
    int end,
    double low,
    double high,
    int needs_recheck_filter
){
    int count = 0;
    int have_previous = 0;
    int previous_needs_recheck = -1;
    int previous_block = -1;

    if (!v)
        return 0;

    if (start > end)
        return 0;

    for (int i = start; i <= end; i++) {
        int needs_recheck;
        int include_block = 1;

        needs_recheck =
            brinBlockNeedsRecheck(v, i, low, high);

        if (needs_recheck_filter != -1) {
            if (needs_recheck_filter != needs_recheck) {
                include_block = 0;
            }
        }

        if (include_block) {
            if (!have_previous) {
                count++;
                have_previous = 1;
                previous_needs_recheck = needs_recheck;
                previous_block = i;
            }
            else {
                int is_contiguous = 0;
                int same_recheck = 0;

                if (previous_block + 1 == i) {
                    is_contiguous = 1;
                }

                if (previous_needs_recheck == needs_recheck) {
                    same_recheck = 1;
                }

                if (is_contiguous && same_recheck) {
                    previous_block = i;
                }
                else {
                    count++;
                    previous_needs_recheck = needs_recheck;
                    previous_block = i;
                }
            }
        }
    }

    return count;
}


/*
 * Fixed datetime format accepted by the BRIN prototype.
 *
 * Only this exact format is supported:
 *
 *   YYYY-MM-DD HH:MM:SS
 *
 * Example:
 *
 *   3474-07-06 23:30:00
 *
 * The length is 19 characters plus the null terminator.
 */
#define BRIN_DATETIME_LEN 19
#define BRIN_DATETIME_BUFSZ 20

/* --------------------------------------------------
 * brinDaysFromCivil
 *
 * PURPOSE
 * -------
 * Convert a Gregorian calendar date to the number of
 * days since Unix epoch:
 *
 *   1970-01-01
 *
 * The calculation is deterministic and timezone-free.
 *
 * WHY NOT mktime()
 * ----------------
 * mktime() uses local time and depends on timezone/DST.
 * This prototype needs deterministic UTC-like behavior,
 * especially for reproducible benchmarks.
 * -------------------------------------------------- */
static sqlite3_int64 brinDaysFromCivil(int y, int m, int d)
{
    y -= m <= 2;

    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);
    const unsigned mp = (unsigned)(m + (m > 2 ? -3 : 9));

    const unsigned doy =
        (153 * mp + 2) / 5 + (unsigned)d - 1;

    const unsigned doe =
        yoe * 365 + yoe / 4 - yoe / 100 + doy;

    return (sqlite3_int64)era * 146097 +
           (sqlite3_int64)doe - 719468;
}


/* --------------------------------------------------
 * brinParseFixedDateTimeToEpoch
 *
 * PURPOSE
 * -------
 * Fast conversion from the benchmark datetime format
 * into Unix epoch seconds.
 *
 * ACCEPTED FORMAT
 * ---------------
 * This function assumes the input is always exactly:
 *
 *   YYYY-MM-DD HH:MM:SS
 *
 * Example:
 *
 *   3474-07-06 23:30:00
 *
 * IMPORTANT
 * ---------
 * This function intentionally does not validate the date.
 *
 * The benchmark dataset is generated by controlled code, so
 * all values are assumed to be valid and correctly formatted.
 *
 * Because of that, this parser reads fixed positions directly
 * instead of checking separators, length, month/day validity,
 * or digit validity.
 *
 * WHY THIS IS FASTER
 * ------------------
 * - no strlen()
 * - no sscanf()
 * - no mktime()
 * - no timezone handling
 * - no calendar validation
 *
 * RETURN VALUE
 * ------------
 * SQLITE_OK on success.
 * SQLITE_ERROR only if input pointers are NULL.
 * -------------------------------------------------- */
static int brinParseFixedDateTimeToEpoch(
    const char *s,
    sqlite3_int64 *out_epoch
){
    int y;
    int mo;
    int d;
    int h;
    int mi;
    int sec;
    sqlite3_int64 days;

    if (!s || !out_epoch)
        return SQLITE_ERROR;

    /*
     * Fixed-position parsing.
     *
     * Format:
     *
     *   0123456789012345678
     *   YYYY-MM-DD HH:MM:SS
     */
    y =
        (s[0] - '0') * 1000 +
        (s[1] - '0') * 100 +
        (s[2] - '0') * 10 +
        (s[3] - '0');

    mo =
        (s[5] - '0') * 10 +
        (s[6] - '0');

    d =
        (s[8] - '0') * 10 +
        (s[9] - '0');

    h =
        (s[11] - '0') * 10 +
        (s[12] - '0');

    mi =
        (s[14] - '0') * 10 +
        (s[15] - '0');

    sec =
        (s[17] - '0') * 10 +
        (s[18] - '0');

    days = brinDaysFromCivil(y, mo, d);

    *out_epoch =
        days * 86400LL +
        (sqlite3_int64)h * 3600LL +
        (sqlite3_int64)mi * 60LL +
        (sqlite3_int64)sec;

    return SQLITE_OK;
}


/* --------------------------------------------------
 * brinCivilFromDays
 *
 * PURPOSE
 * -------
 * Convert days since Unix epoch back to a Gregorian
 * calendar date.
 *
 * This is the inverse operation of brinDaysFromCivil().
 * It is timezone-free and does not use time_t.
 * -------------------------------------------------- */
static void brinCivilFromDays(
    sqlite3_int64 z,
    int *out_y,
    int *out_m,
    int *out_d
){
    z += 719468;

    const sqlite3_int64 era =
        (z >= 0 ? z : z - 146096) / 146097;

    const unsigned doe =
        (unsigned)(z - era * 146097);

    const unsigned yoe =
        (doe - doe / 1460 + doe / 36524 -
         doe / 146096) / 365;

    sqlite3_int64 y = (sqlite3_int64)yoe + era * 400;

    const unsigned doy =
        doe - (365 * yoe + yoe / 4 - yoe / 100);

    const unsigned mp =
        (5 * doy + 2) / 153;

    const unsigned d =
        doy - (153 * mp + 2) / 5 + 1;

    const unsigned m =
        mp + (mp < 10 ? 3 : -9);

    y += (m <= 2);

    *out_y = (int)y;
    *out_m = (int)m;
    *out_d = (int)d;
}


/* --------------------------------------------------
 * brinFormatEpochFixed
 *
 * PURPOSE
 * -------
 * Convert epoch seconds to the benchmark datetime format:
 *
 *   YYYY-MM-DD HH:MM:SS
 *
 * This function does not use gmtime() or time_t.
 * It is deterministic for large future years.
 * -------------------------------------------------- */
static void brinFormatEpochFixed(
    sqlite3_int64 epoch,
    char *buffer,
    size_t buffer_size
){
    sqlite3_int64 days;
    sqlite3_int64 rem;
    int y;
    int mo;
    int d;
    int h;
    int mi;
    int sec;

    if (!buffer || buffer_size == 0)
        return;

    days = epoch / 86400LL;
    rem = epoch % 86400LL;

    /*
     * Support negative epochs correctly, even though the
     * benchmark currently uses future positive dates.
     */
    if (rem < 0) {
        rem += 86400LL;
        days--;
    }

    brinCivilFromDays(days, &y, &mo, &d);

    h = (int)(rem / 3600LL);
    rem %= 3600LL;

    mi = (int)(rem / 60LL);
    sec = (int)(rem % 60LL);

    snprintf(buffer, buffer_size,
             "%04d-%02d-%02d %02d:%02d:%02d",
             y, mo, d, h, mi, sec);
}


/* --------------------------------------------------
 * brinCopyFixedDateTime
 *
 * PURPOSE
 * -------
 * Copy the benchmark datetime string into a fixed-size
 * local buffer.
 *
 * ACCEPTED FORMAT
 * ---------------
 * The source is assumed to be:
 *
 *   YYYY-MM-DD HH:MM:SS
 *
 * The function copies exactly 19 characters and appends
 * a null terminator.
 *
 * IMPORTANT
 * ---------
 * No format validation is performed here.
 *
 * This is intentional for benchmark speed. The dataset is
 * controlled and always produces valid datetime strings.
 *
 * RETURN VALUE
 * ------------
 * SQLITE_OK on success.
 * SQLITE_ERROR only if pointers are invalid or the output
 * buffer is too small.
 * -------------------------------------------------- */
static int brinCopyFixedDateTime(
    const char *src,
    char *dst,
    size_t dst_size
){
    if (!src || !dst)
        return SQLITE_ERROR;

    if (dst_size < BRIN_DATETIME_BUFSZ)
        return SQLITE_ERROR;

    memcpy(dst, src, BRIN_DATETIME_LEN);
    dst[BRIN_DATETIME_LEN] = '\0';

    return SQLITE_OK;
}


/* --------------------------------------------------
 * brinSqlValueAsDouble
 *
 * PURPOSE
 * -------
 * Convert a sqlite3_value from xBestIndex() or xFilter()
 * into the internal numeric representation used by BRIN.
 *
 * INTEGER / REAL
 * --------------
 * Values are read directly as double.
 *
 * TEXT
 * ----
 * Values are assumed to be benchmark datetimes:
 *
 *   YYYY-MM-DD HH:MM:SS
 *
 * They are converted to Unix epoch seconds.
 *
 * No datetime validation is performed. The benchmark always
 * provides valid fixed-format strings.
 * -------------------------------------------------- */
static int brinSqlValueAsDouble(
    BrinVtab *v,
    sqlite3_value *value,
    double *out
){
    int type;

    if (!v || !value || !out)
        return SQLITE_ERROR;

    type = sqlite3_value_type(value);

    if (type == SQLITE_NULL)
        return SQLITE_CONSTRAINT;

    if (v->affinity == BRIN_TYPE_TEXT) {
        const char *txt;
        sqlite3_int64 epoch = 0;
        int rc;

        /*
         * For this benchmark version, TEXT BRIN queries are
         * expected to pass datetime literals as TEXT.
         */
        if (type != SQLITE_TEXT)
            return SQLITE_CONSTRAINT;

        txt = (const char*)sqlite3_value_text(value);

        rc = brinParseFixedDateTimeToEpoch(txt, &epoch);
        if (rc != SQLITE_OK)
            return rc;

        *out = (double)epoch;
        return SQLITE_OK;
    }

    *out = sqlite3_value_double(value);
    return SQLITE_OK;
}


/* --------------------------------------------------
 * brinStmtValueAsDouble
 *
 * PURPOSE
 * -------
 * Convert a value from sqlite3_stmt into the internal
 * numeric representation used by BRIN.
 *
 * TEXT values must be in the fixed benchmark format:
 *
 *   YYYY-MM-DD HH:MM:SS
 * -------------------------------------------------- */
static int brinStmtValueAsDouble(
    BrinVtab *v,
    sqlite3_stmt *stmt,
    int col,
    double *out
){
    int type;

    if (!v || !stmt || !out)
        return SQLITE_ERROR;

    type = sqlite3_column_type(stmt, col);

    if (type == SQLITE_NULL)
        return SQLITE_CONSTRAINT;

    if (v->affinity == BRIN_TYPE_TEXT) {
        const char *txt;
        sqlite3_int64 epoch = 0;

        if (type != SQLITE_TEXT)
            return SQLITE_CONSTRAINT;

        txt = (const char*)sqlite3_column_text(stmt, col);

        if (brinParseFixedDateTimeToEpoch(txt, &epoch)
            != SQLITE_OK)
        {
            return SQLITE_CONSTRAINT;
        }

        *out = (double)epoch;
        return SQLITE_OK;
    }

    *out = sqlite3_column_double(stmt, col);
    return SQLITE_OK;
}


/* --------------------------------------------------
 * brinRangeMinAsDouble
 *
 * PURPOSE
 * -------
 * Return the range minimum as a comparable number.
 *
 * For TEXT, this returns min_epoch.
 * -------------------------------------------------- */
static double brinRangeMinAsDouble(const BrinRange *r)
{
    if (r->type == BRIN_TYPE_TEXT)
        return (double)r->u.txt.min_epoch;

    return r->u.num.min;
}


/* --------------------------------------------------
 * brinRangeMaxAsDouble
 *
 * PURPOSE
 * -------
 * Return the range maximum as a comparable number.
 *
 * For TEXT, this returns max_epoch.
 * -------------------------------------------------- */
static double brinRangeMaxAsDouble(const BrinRange *r)
{
    if (r->type == BRIN_TYPE_TEXT)
        return (double)r->u.txt.max_epoch;

    return r->u.num.max;
}


/* --------------------------------------------------
 * brinFindCandidateRange
 *
 * PURPOSE
 * -------
 * Find the first and last BRIN block that may overlap
 * a query range [low, high].
 *
 * This is used by xBestIndex() and xFilter().
 *
 * INTERNAL REPRESENTATION
 * -----------------------
 * All values are compared as numbers:
 *
 *   INTEGER -> double
 *   REAL    -> double
 *   TEXT    -> epoch seconds as double
 * -------------------------------------------------- */
static int brinFindCandidateRange(
    BrinVtab *v,
    double low,
    double high,
    int *out_start,
    int *out_end
){
    int left;
    int right;
    int mid;
    int start;
    int end;

    if (!v || !out_start || !out_end)
        return SQLITE_ERROR;

    *out_start = v->total_blocks;
    *out_end = -1;

    if (v->total_blocks <= 0)
        return SQLITE_OK;

    start = v->total_blocks;

    left = 0;
    right = v->total_blocks - 1;

    while (left <= right) {
        mid = (left + right) / 2;

        BrinRange *r = &v->ranges[mid];

        if (brinRangeMaxAsDouble(r) >= low) {
            start = mid;
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    if (start == v->total_blocks)
        return SQLITE_OK;

    end = -1;

    left = 0;
    right = v->total_blocks - 1;

    while (left <= right) {
        mid = (left + right) / 2;

        BrinRange *r = &v->ranges[mid];

        if (brinRangeMinAsDouble(r) <= high) {
            end = mid;
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    if (end < start)
        return SQLITE_OK;

    *out_start = start;
    *out_end = end;

    return SQLITE_OK;
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
 * Incrementally update the in-memory BRIN index by reading
 * only rows appended after v->last_indexed_rowid.
 *
 * THESIS / BENCHMARK ASSUMPTIONS
 * ------------------------------
 * - The base table is append-only.
 * - No UPDATE operations are considered.
 * - No DELETE operations are considered.
 * - rowid increases monotonically.
 * - The indexed column is strictly ordered.
 * - TEXT datetime values always use:
 *
 *     YYYY-MM-DD HH:MM:SS
 *
 * TEXT-AS-EPOCH BEHAVIOR
 * ----------------------
 * In this version, TEXT values are not stored as heap strings.
 * They are converted to epoch seconds and stored as:
 *
 *   u.txt.min_epoch
 *   u.txt.max_epoch
 *
 * Because incremental update only sees newly appended rows,
 * each appended TEXT value is converted to epoch immediately.
 *
 * WHY THIS IS OK
 * --------------
 * The full build avoids converting every TEXT row by only
 * parsing the first and last value of each block.
 *
 * Incremental update is different:
 *
 *   - it only processes new appended rows
 *   - the last block must remain immediately queryable
 *
 * Therefore, when a new TEXT row extends the last block, its
 * epoch value becomes the new max_epoch right away.
 *
 * RETURN VALUE
 * ------------
 * SQLITE_OK on success, or an SQLite error code.
 * -------------------------------------------------------- */
static int brinIncrementalUpdate(BrinVtab *v)
{
    sqlite3_stmt *stmt = NULL;
    int rc = SQLITE_OK;
    int found_new_rows = 0;

    char sql[512];

    if (!v || !v->db || !v->index_ready)
        return SQLITE_OK;

    DEBUG_PRINT("[BRIN] brinIncrementalUpdate()\n");
    DEBUG_PRINT("last_indexed_rowid before update: %lld\n",
                v->last_indexed_rowid);
    DEBUG_PRINT("last_block_size before update   : %d\n",
                v->last_block_size);
    DEBUG_PRINT("total_blocks before update      : %d\n",
                v->total_blocks);

    /*
     * Only scan rows appended after the last indexed rowid.
     *
     * ORDER BY rowid ASC keeps the incremental update
     * deterministic and consistent with the full build.
     */
    snprintf(sql, sizeof(sql),
        "SELECT rowid, %s FROM %s "
        "WHERE rowid > ? "
        "ORDER BY rowid ASC;",
        v->column,
        v->table
    );

    rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        DEBUG_PRINT("brinIncrementalUpdate prepare failed: %s\n",
                    sqlite3_errmsg(v->db));
        return rc;
    }

    sqlite3_bind_int64(stmt, 1, v->last_indexed_rowid);

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        sqlite3_int64 rowid;
        int value_type;

        rowid = sqlite3_column_int64(stmt, 0);
        value_type = sqlite3_column_type(stmt, 1);

        found_new_rows = 1;

        DEBUG_PRINT("Processing appended rowid=%lld\n", rowid);

        /*
         * The benchmark should never generate NULL values.
         * This check is only defensive.
         */
        if (value_type == SQLITE_NULL) {
            sqlite3_finalize(stmt);
            return SQLITE_CONSTRAINT;
        }

        /*
         * CASE 1:
         * There are no BRIN blocks yet.
         *
         * This should be rare because the full build normally
         * creates the initial index, but the case is handled
         * defensively.
         */
        if (v->total_blocks == 0)
        {
            BrinRange *tmp;
            BrinRange *newBlock;

            tmp = realloc(v->ranges, sizeof(BrinRange));
            if (!tmp) {
                sqlite3_finalize(stmt);
                return SQLITE_NOMEM;
            }

            v->ranges = tmp;

            newBlock = &v->ranges[0];
            memset(newBlock, 0, sizeof(BrinRange));

            newBlock->type = v->affinity;
            newBlock->start_rowid = rowid;
            newBlock->end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT) {
                const char *txt;
                sqlite3_int64 epoch = 0;

                txt = (const char*)sqlite3_column_text(stmt, 1);

                rc = brinParseFixedDateTimeToEpoch(txt, &epoch);
                if (rc != SQLITE_OK) {
                    sqlite3_finalize(stmt);
                    return rc;
                }

                newBlock->u.txt.min_epoch = epoch;
                newBlock->u.txt.max_epoch = epoch;

                DEBUG_PRINT(
                    "Initialized first TEXT block with epoch=%lld\n",
                    epoch
                );
            }
            else {
                double val;

                val = sqlite3_column_double(stmt, 1);

                newBlock->u.num.min = val;
                newBlock->u.num.max = val;

                DEBUG_PRINT(
                    "Initialized first numeric block with value=%.6f\n",
                    val
                );
            }

            v->total_blocks = 1;
            v->last_block_size = 1;
            v->last_indexed_rowid = rowid;

            continue;
        }

        /*
         * CASE 2:
         * At least one block exists.
         *
         * Since the workload is append-only, only the last
         * BRIN block can change.
         */
        BrinRange *lastBlock;
        lastBlock = &v->ranges[v->total_blocks - 1];

        /*
         * CASE 2A:
         * The last block still has space.
         *
         * Extend the block by moving end_rowid forward and
         * updating max.
         */
        if (v->last_block_size < v->block_size)
        {
            lastBlock->end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT) {
                const char *txt;
                sqlite3_int64 epoch = 0;

                txt = (const char*)sqlite3_column_text(stmt, 1);

                rc = brinParseFixedDateTimeToEpoch(txt, &epoch);
                if (rc != SQLITE_OK) {
                    sqlite3_finalize(stmt);
                    return rc;
                }

                /*
                 * No ordering validation is performed here.
                 *
                 * The dataset generator guarantees that the
                 * values are valid and strictly ordered.
                 */
                lastBlock->u.txt.max_epoch = epoch;

                DEBUG_PRINT(
                    "Extended TEXT block max_epoch=%lld\n",
                    epoch
                );
            }
            else {
                double val;

                val = sqlite3_column_double(stmt, 1);

                /*
                 * No ordering validation is performed here.
                 *
                 * The benchmark data is generated in sorted order.
                 */
                lastBlock->u.num.max = val;

                DEBUG_PRINT(
                    "Extended numeric block max=%.6f\n",
                    val
                );
            }

            v->last_block_size++;
        }
        /*
         * CASE 2B:
         * The last block is full.
         *
         * Create a new block. The appended row becomes both
         * min and max of that new block.
         */
        else
        {
            BrinRange *tmp;
            BrinRange *newBlock;

            tmp = realloc(
                v->ranges,
                (size_t)(v->total_blocks + 1) *
                sizeof(BrinRange)
            );

            if (!tmp) {
                sqlite3_finalize(stmt);
                return SQLITE_NOMEM;
            }

            v->ranges = tmp;

            newBlock = &v->ranges[v->total_blocks];
            memset(newBlock, 0, sizeof(BrinRange));

            newBlock->type = v->affinity;
            newBlock->start_rowid = rowid;
            newBlock->end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT) {
                const char *txt;
                sqlite3_int64 epoch = 0;

                txt = (const char*)sqlite3_column_text(stmt, 1);

                rc = brinParseFixedDateTimeToEpoch(txt, &epoch);
                if (rc != SQLITE_OK) {
                    sqlite3_finalize(stmt);
                    return rc;
                }

                newBlock->u.txt.min_epoch = epoch;
                newBlock->u.txt.max_epoch = epoch;

                DEBUG_PRINT(
                    "Created new TEXT block with min=max epoch=%lld\n",
                    epoch
                );
            }
            else {
                double val;

                val = sqlite3_column_double(stmt, 1);

                newBlock->u.num.min = val;
                newBlock->u.num.max = val;

                DEBUG_PRINT(
                    "Created new numeric block with min=max %.6f\n",
                    val
                );
            }

            v->total_blocks++;
            v->last_block_size = 1;
        }

        /*
         * Update global incremental state after each appended row.
         */
        v->last_indexed_rowid = rowid;
    }

    if (rc != SQLITE_DONE) {
        DEBUG_PRINT("brinIncrementalUpdate step failed: %s\n",
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
    DEBUG_PRINT("last_indexed_rowid after update: %lld\n",
                v->last_indexed_rowid);
    DEBUG_PRINT("last_block_size after update   : %d\n",
                v->last_block_size);
    DEBUG_PRINT("total_blocks after update      : %d\n",
                v->total_blocks);

    return SQLITE_OK;
}


/* --------------------------------------------------------
 * brinBuildIndex
 *
 * PURPOSE
 * -------
 * Build the full in-memory BRIN index by scanning the base
 * table once in rowid order.
 *
 * FIXED-SIZE BLOCK MODEL
 * ----------------------
 * This function keeps the base BRIN behavior:
 *
 *   one BRIN range = v->block_size rows
 *
 * TEXT-AS-EPOCH OPTIMIZATION
 * --------------------------
 * For TEXT columns, values are assumed to use exactly this
 * fixed datetime format:
 *
 *   YYYY-MM-DD HH:MM:SS
 *
 * Internally, TEXT min/max values are stored as Unix epoch
 * seconds:
 *
 *   min_epoch
 *   max_epoch
 *
 * To avoid parsing every TEXT row:
 *
 *   1. When a new block starts:
 *        parse the first TEXT value and store min_epoch.
 *
 *   2. While rows are added to the current block:
 *        copy the latest TEXT value into a stack buffer.
 *
 *   3. When the block closes:
 *        parse that latest TEXT value and store max_epoch.
 *
 * This is correct under the thesis assumption:
 *
 *   value[n] < value[n + 1]
 *
 * SAFETY
 * ------
 * The build is transactional from the vtab perspective:
 *
 *   - build into new_ranges first
 *   - only replace v->ranges if the whole build succeeds
 *
 * This avoids leaving the virtual table with a partially
 * built BRIN index after an error.
 * -------------------------------------------------------- */
static int brinBuildIndex(BrinVtab *v)
{
    sqlite3_stmt *stmt = NULL;
    int rc = SQLITE_OK;

    char sql[1024];

    BrinRange *new_ranges = NULL;
    int new_total_blocks = 0;
    int capacity = 128;

    BrinRange current;
    int current_active = 0;
    int block_pos = 0;

    sqlite3_int64 last_rowid_seen = 0;
    int last_stored_block_size = 0;

    double prev_num = 0.0;
    int have_prev_num = 0;

    char prev_text[BRIN_DATETIME_BUFSZ];
    int have_prev_text = 0;

    char text_block_max[BRIN_DATETIME_BUFSZ];
    int text_block_max_valid = 0;

    if (!v || !v->db)
        return SQLITE_ERROR;

    if (v->block_size <= 0) {
        sqlite3_free(v->base.zErrMsg);
        v->base.zErrMsg = sqlite3_mprintf(
            "BRIN build failed: block_size must be > 0"
        );
        return SQLITE_ERROR;
    }

    DEBUG_PRINT("[BRIN] brinBuildIndex()\n");
    DEBUG_PRINT("Table      : %s\n", v->table);
    DEBUG_PRINT("Column     : %s\n", v->column);
    DEBUG_PRINT("Block size : %d\n", v->block_size);
    DEBUG_PRINT("Affinity   : %d\n\n", v->affinity);

    sqlite3_free(v->base.zErrMsg);
    v->base.zErrMsg = NULL;

    /*
     * ORDER BY rowid ASC makes the build order explicit.
     *
     * The BRIN summaries depend on rowid order because each
     * range stores start_rowid and end_rowid.
     */
    snprintf(sql, sizeof(sql),
             "SELECT rowid, %s FROM %s ORDER BY rowid ASC;",
             v->column,
             v->table);

    rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_free(v->base.zErrMsg);
        v->base.zErrMsg = sqlite3_mprintf(
            "BRIN build failed: prepare error: %s",
            sqlite3_errmsg(v->db)
        );
        return rc;
    }

    /*
     * Defensive check:
     *
     * The build query must return exactly:
     *   column 0 -> rowid
     *   column 1 -> indexed value
     */
    if (sqlite3_column_count(stmt) != 2) {
        sqlite3_finalize(stmt);

        sqlite3_free(v->base.zErrMsg);
        v->base.zErrMsg = sqlite3_mprintf(
            "BRIN build failed: expected 2 columns"
        );

        return SQLITE_ERROR;
    }

    new_ranges = calloc((size_t)capacity, sizeof(BrinRange));
    if (!new_ranges) {
        sqlite3_finalize(stmt);
        return SQLITE_NOMEM;
    }

    memset(&current, 0, sizeof(BrinRange));
    memset(prev_text, 0, sizeof(prev_text));
    memset(text_block_max, 0, sizeof(text_block_max));

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        sqlite3_int64 rowid = sqlite3_column_int64(stmt, 0);
        int value_type = sqlite3_column_type(stmt, 1);

        if (value_type == SQLITE_NULL) {
            sqlite3_free(v->base.zErrMsg);
            v->base.zErrMsg = sqlite3_mprintf(
                "BRIN build failed: NULL in column '%s' "
                "at rowid %lld",
                v->column,
                rowid
            );
            rc = SQLITE_CONSTRAINT;
            goto build_error;
        }

        last_rowid_seen = rowid;

        /*
         * Validate global order.
         *
         * For numeric values:
         *   compare as numbers.
         *
         * For TEXT datetime:
         *   compare lexically because the accepted format
         *   YYYY-MM-DD HH:MM:SS preserves chronological order.
         *
         * This avoids converting every TEXT row to epoch.
         */
        if (v->affinity == BRIN_TYPE_TEXT) {
            const char *txt =
                (const char*)sqlite3_column_text(stmt, 1);

            rc = brinCopyFixedDateTime(
                txt,
                text_block_max,
                sizeof(text_block_max)
            );

            if (rc != SQLITE_OK) {
                sqlite3_free(v->base.zErrMsg);
                v->base.zErrMsg = sqlite3_mprintf(
                    "BRIN build failed: invalid datetime format "
                    "at rowid %lld",
                    rowid
                );
                goto build_error;
            }

            text_block_max_valid = 1;

            /*
             * The thesis assumption is strictly increasing:
             *
             *   value[n] < value[n + 1]
             *
             * Therefore equality is also rejected.
             *
             * If you later want to allow duplicates, change
             * this condition from >= 0 to > 0.
             */
            if (have_prev_text) {
                if (strcmp(prev_text, text_block_max) >= 0) {
                    sqlite3_free(v->base.zErrMsg);
                    v->base.zErrMsg = sqlite3_mprintf(
                        "BRIN build failed: TEXT datetime values "
                        "are not strictly ordered at rowid %lld",
                        rowid
                    );
                    rc = SQLITE_CONSTRAINT;
                    goto build_error;
                }
            }

            memcpy(prev_text, text_block_max, BRIN_DATETIME_BUFSZ);
            have_prev_text = 1;
        }
        else {
            double val = sqlite3_column_double(stmt, 1);

            /*
             * Strictly increasing numeric order.
             *
             * If you want to allow equal adjacent values, change
             * <= to <.
             */
            if (have_prev_num) {
                if (val <= prev_num) {
                    sqlite3_free(v->base.zErrMsg);
                    v->base.zErrMsg = sqlite3_mprintf(
                        "BRIN build failed: numeric values are "
                        "not strictly ordered at rowid %lld",
                        rowid
                    );
                    rc = SQLITE_CONSTRAINT;
                    goto build_error;
                }
            }

            prev_num = val;
            have_prev_num = 1;
        }

        /*
         * Start a new fixed-size BRIN block.
         */
        if (block_pos == 0)
        {
            DEBUG_PRINT("Starting new block at rowid=%lld\n", rowid);

            memset(&current, 0, sizeof(BrinRange));

            current.type = v->affinity;
            current.start_rowid = rowid;
            current.end_rowid = rowid;
            current_active = 1;

            if (v->affinity == BRIN_TYPE_TEXT)
            {
                sqlite3_int64 epoch = 0;

                /*
                 * First TEXT value of the block becomes min_epoch.
                 */
                rc = brinParseFixedDateTimeToEpoch(
                    text_block_max,
                    &epoch
                );

                if (rc != SQLITE_OK) {
                    sqlite3_free(v->base.zErrMsg);
                    v->base.zErrMsg = sqlite3_mprintf(
                        "BRIN build failed: cannot parse datetime "
                        "at rowid %lld",
                        rowid
                    );
                    goto build_error;
                }

                current.u.txt.min_epoch = epoch;
                current.u.txt.max_epoch = epoch;

                DEBUG_PRINT(
                    "Initial TEXT epoch for block: %lld\n",
                    epoch
                );
            }
            else
            {
                double val = sqlite3_column_double(stmt, 1);

                current.u.num.min = val;
                current.u.num.max = val;

                DEBUG_PRINT(
                    "Initial numeric value for block: %.6f\n",
                    val
                );
            }
        }
        else
        {
            current.end_rowid = rowid;

            if (v->affinity == BRIN_TYPE_TEXT)
            {
                /*
                 * Do not parse intermediate TEXT values.
                 *
                 * text_block_max already contains the latest
                 * value seen in this block.
                 */
                DEBUG_PRINT(
                    "Updated pending TEXT max for block: %s\n",
                    text_block_max
                );
            }
            else
            {
                double val = sqlite3_column_double(stmt, 1);

                current.u.num.max = val;

                DEBUG_PRINT(
                    "Updated block numeric max to: %.6f\n",
                    val
                );
            }
        }

        block_pos++;

        /*
         * Store full block.
         */
        if (block_pos >= v->block_size)
        {
            if (v->affinity == BRIN_TYPE_TEXT) {
                sqlite3_int64 max_epoch = 0;

                if (!text_block_max_valid) {
                    rc = SQLITE_ERROR;
                    goto build_error;
                }

                /*
                 * Last TEXT value of the block becomes max_epoch.
                 */
                rc = brinParseFixedDateTimeToEpoch(
                    text_block_max,
                    &max_epoch
                );

                if (rc != SQLITE_OK) {
                    sqlite3_free(v->base.zErrMsg);
                    v->base.zErrMsg = sqlite3_mprintf(
                        "BRIN build failed: cannot parse block "
                        "max datetime at rowid %lld",
                        rowid
                    );
                    goto build_error;
                }

                current.u.txt.max_epoch = max_epoch;
            }

            if (new_total_blocks >= capacity)
            {
                int new_capacity = capacity * 2;

                BrinRange *tmp = realloc(
                    new_ranges,
                    (size_t)new_capacity * sizeof(BrinRange)
                );

                if (!tmp) {
                    rc = SQLITE_NOMEM;
                    goto build_error;
                }

                new_ranges = tmp;
                capacity = new_capacity;
            }

            new_ranges[new_total_blocks++] = current;
            last_stored_block_size = block_pos;

            if (v->affinity == BRIN_TYPE_TEXT) {
                char min_buf[BRIN_DATETIME_BUFSZ];
                char max_buf[BRIN_DATETIME_BUFSZ];

                brinFormatEpochFixed(
                    current.u.txt.min_epoch,
                    min_buf,
                    sizeof(min_buf)
                );

                brinFormatEpochFixed(
                    current.u.txt.max_epoch,
                    max_buf,
                    sizeof(max_buf)
                );

                DEBUG_PRINT(
                    "Stored TEXT block %d: rowid [%lld, %lld], "
                    "min=%s, max=%s\n",
                    new_total_blocks - 1,
                    current.start_rowid,
                    current.end_rowid,
                    min_buf,
                    max_buf
                );
            }
            else {
                DEBUG_PRINT(
                    "Stored numeric block %d: rowid [%lld, %lld], "
                    "min=%.6f, max=%.6f\n",
                    new_total_blocks - 1,
                    current.start_rowid,
                    current.end_rowid,
                    current.u.num.min,
                    current.u.num.max
                );
            }

            memset(&current, 0, sizeof(BrinRange));
            memset(text_block_max, 0, sizeof(text_block_max));

            text_block_max_valid = 0;
            current_active = 0;
            block_pos = 0;
        }
    }

    /*
     * sqlite3_step() must end with SQLITE_DONE.
     */
    if (rc != SQLITE_DONE) {
        sqlite3_free(v->base.zErrMsg);
        v->base.zErrMsg = sqlite3_mprintf(
            "BRIN build failed: sqlite3_step error: %s",
            sqlite3_errmsg(v->db)
        );
        goto build_error;
    }

    /*
     * Store final partial block, if any.
     */
    if (block_pos > 0)
    {
        if (!current_active) {
            rc = SQLITE_ERROR;
            goto build_error;
        }

        if (v->affinity == BRIN_TYPE_TEXT) {
            sqlite3_int64 max_epoch = 0;

            if (!text_block_max_valid) {
                rc = SQLITE_ERROR;
                goto build_error;
            }

            rc = brinParseFixedDateTimeToEpoch(
                text_block_max,
                &max_epoch
            );

            if (rc != SQLITE_OK) {
                sqlite3_free(v->base.zErrMsg);
                v->base.zErrMsg = sqlite3_mprintf(
                    "BRIN build failed: cannot parse final "
                    "TEXT max datetime"
                );
                goto build_error;
            }

            current.u.txt.max_epoch = max_epoch;
        }

        if (new_total_blocks >= capacity)
        {
            int new_capacity = capacity * 2;

            BrinRange *tmp = realloc(
                new_ranges,
                (size_t)new_capacity * sizeof(BrinRange)
            );

            if (!tmp) {
                rc = SQLITE_NOMEM;
                goto build_error;
            }

            new_ranges = tmp;
            capacity = new_capacity;
        }

        new_ranges[new_total_blocks++] = current;
        last_stored_block_size = block_pos;

        if (v->affinity == BRIN_TYPE_TEXT) {
            char min_buf[BRIN_DATETIME_BUFSZ];
            char max_buf[BRIN_DATETIME_BUFSZ];

            brinFormatEpochFixed(
                current.u.txt.min_epoch,
                min_buf,
                sizeof(min_buf)
            );

            brinFormatEpochFixed(
                current.u.txt.max_epoch,
                max_buf,
                sizeof(max_buf)
            );

            DEBUG_PRINT(
                "Stored partial TEXT block %d: "
                "rowid [%lld, %lld], size=%d, "
                "min=%s, max=%s\n",
                new_total_blocks - 1,
                current.start_rowid,
                current.end_rowid,
                block_pos,
                min_buf,
                max_buf
            );
        }
        else {
            DEBUG_PRINT(
                "Stored partial numeric block %d: "
                "rowid [%lld, %lld], size=%d, "
                "min=%.6f, max=%.6f\n",
                new_total_blocks - 1,
                current.start_rowid,
                current.end_rowid,
                block_pos,
                current.u.num.min,
                current.u.num.max
            );
        }

        memset(&current, 0, sizeof(BrinRange));
        current_active = 0;
        block_pos = 0;
    }

    sqlite3_finalize(stmt);
    stmt = NULL;

    /*
     * Commit the new BRIN summaries only after a successful build.
     */
    if (v->ranges) {
        free(v->ranges);
    }

    v->ranges = new_ranges;
    v->total_blocks = new_total_blocks;
    v->last_indexed_rowid = last_rowid_seen;
    v->last_block_size = last_stored_block_size;
    v->index_ready = 1;

    DEBUG_PRINT("Total blocks         : %d\n", v->total_blocks);
    DEBUG_PRINT("Last indexed rowid   : %lld\n",
                v->last_indexed_rowid);
    DEBUG_PRINT("Last block size      : %d\n",
                v->last_block_size);

    return SQLITE_OK;

build_error:
    if (stmt) {
        sqlite3_finalize(stmt);
    }

    if (new_ranges) {
        free(new_ranges);
    }

    return rc;
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
            "CREATE TABLE x("
            "min INTEGER, "
            "max INTEGER, "
            "start_rowid INT, "
            "end_rowid INT, "
            "needs_recheck INT)"
        );
        v->affinity = BRIN_TYPE_INTEGER;
    }
    if (strcmp(affinity, "REAL") == 0) {
        rc = sqlite3_declare_vtab(db,
            "CREATE TABLE x("
            "min REAL, "
            "max REAL, "
            "start_rowid INT, "
            "end_rowid INT, "
            "needs_recheck INT)"
        );
        v->affinity = BRIN_TYPE_REAL;
    }
    if (strcmp(affinity, "TEXT") == 0) {
        rc = sqlite3_declare_vtab(db,
            "CREATE TABLE x("
            "min TEXT, "
            "max TEXT, "
            "start_rowid INT, "
            "end_rowid INT, "
            "needs_recheck INT)"
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
 * Allocate and initialize a cursor.
 *
 * COALESCING NOTE
 * ---------------
 * The cursor owns output_ranges. This array is built in
 * xFilter() and freed in xClose().
 * -------------------------------------------------- */
static int brinOpen(
    sqlite3_vtab *pVtab,
    sqlite3_vtab_cursor **ppCursor
){
    BrinCursor *c;

    DEBUG_PRINT("[BRIN] brinOpen()\n");

    c = calloc(1, sizeof(BrinCursor));
    if (!c)
        return SQLITE_NOMEM;

    c->v = (BrinVtab*)pVtab;
    c->eof = 1;

    c->output_ranges = NULL;
    c->output_count = 0;
    c->output_capacity = 0;
    c->current_output = 0;

    c->needs_recheck_filter = -1;

    *ppCursor = &c->base;
    return SQLITE_OK;
}


/* --------------------------------------------------
 * xClose
 *
 * PURPOSE
 * -------
 * Destroy a cursor and release the coalesced output list.
 * -------------------------------------------------- */
static int brinClose(sqlite3_vtab_cursor *cur)
{
    BrinCursor *c = (BrinCursor*)cur;

    DEBUG_PRINT("[BRIN] brinClose()\n");

    if (c) {
        brinResetOutputRanges(c);
        free(c);
    }

    return SQLITE_OK;
}


/* --------------------------------------------------
 * xBestIndex
 *
 * PURPOSE
 * -------
 * Called by SQLite during query planning.
 *
 * This method tells SQLite which constraints the virtual
 * table can use and how expensive the scan is expected to
 * be.
 *
 * SUPPORTED CONSTRAINTS
 * ---------------------
 * Required for BRIN range scan:
 *
 *   min <= high
 *   max >= low
 *
 * Optional:
 *
 *   needs_recheck = 0
 *   needs_recheck = 1
 *
 * The optional needs_recheck constraint allows SQL to ask
 * the vtab for only:
 *
 *   - fully-covered ranges
 *   - boundary/recheck ranges
 *
 * This is useful with UNION ALL:
 *
 *   branch 1 -> needs_recheck = 0, no base-table recheck
 *   branch 2 -> needs_recheck = 1, with base-table recheck
 * -------------------------------------------------- */
static int brinBestIndex(
    sqlite3_vtab *pVtab,
    sqlite3_index_info *pIdxInfo
){
    BrinVtab *v = (BrinVtab*)pVtab;

    int minTerm = -1;
    int maxTerm = -1;
    int recheckTerm = -1;

    DEBUG_PRINT("[BRIN] brinBestIndex()\n");
    DEBUG_PRINT("total_blocks currently known: %d\n",
                v->total_blocks);

    pIdxInfo->idxNum = 0;
    pIdxInfo->idxStr = NULL;
    pIdxInfo->needToFreeIdxStr = 0;
    pIdxInfo->orderByConsumed = 0;

    /*
     * Find usable constraints.
     *
     * Column mapping:
     *   0 -> min
     *   1 -> max
     *   2 -> start_rowid
     *   3 -> end_rowid
     *   4 -> needs_recheck
     */
    for (int i = 0; i < pIdxInfo->nConstraint; i++) {
        const struct sqlite3_index_constraint *c;

        c = &pIdxInfo->aConstraint[i];

        DEBUG_PRINT("Constraint %d usable=%d column=%d op=%d\n",
                    i,
                    c->usable,
                    c->iColumn,
                    c->op);

        if (!c->usable) {
            DEBUG_PRINT("Skipping constraint %d: not usable\n", i);
            continue;
        }

        if (c->iColumn == 0 &&
            c->op == SQLITE_INDEX_CONSTRAINT_LE)
        {
            minTerm = i;
            DEBUG_PRINT("Detected BRIN constraint: min <= ?\n");
        }
        else if (c->iColumn == 1 &&
                 c->op == SQLITE_INDEX_CONSTRAINT_GE)
        {
            maxTerm = i;
            DEBUG_PRINT("Detected BRIN constraint: max >= ?\n");
        }
        else if (c->iColumn == 4 &&
                 c->op == SQLITE_INDEX_CONSTRAINT_EQ)
        {
            recheckTerm = i;
            DEBUG_PRINT(
                "Detected optional constraint: needs_recheck = ?\n"
            );
        }
    }

    /*
     * The BRIN range scan requires both range bounds.
     */
    if (minTerm >= 0 && maxTerm >= 0) {
        sqlite3_value *pHigh = NULL;
        sqlite3_value *pLow = NULL;
        sqlite3_value *pRecheck = NULL;

        int rcHigh;
        int rcLow;
        int rcRecheck;

        int needs_recheck_filter = -1;

        /*
         * argv[0] in xFilter = high
         * argv[1] in xFilter = low
         */
        pIdxInfo->aConstraintUsage[minTerm].argvIndex = 1;
        pIdxInfo->aConstraintUsage[minTerm].omit = 1;

        pIdxInfo->aConstraintUsage[maxTerm].argvIndex = 2;
        pIdxInfo->aConstraintUsage[maxTerm].omit = 1;

        /*
         * Optional:
         *
         * argv[2] in xFilter = needs_recheck filter.
         */
        if (recheckTerm >= 0) {
            pIdxInfo->aConstraintUsage[recheckTerm].argvIndex = 3;
            pIdxInfo->aConstraintUsage[recheckTerm].omit = 1;
        }

        /*
         * Try to compute a better estimate if RHS values are
         * known at planning time.
         */
        rcHigh =
            sqlite3_vtab_rhs_value(pIdxInfo, minTerm, &pHigh);

        rcLow =
            sqlite3_vtab_rhs_value(pIdxInfo, maxTerm, &pLow);

        if (recheckTerm >= 0) {
            rcRecheck =
                sqlite3_vtab_rhs_value(
                    pIdxInfo,
                    recheckTerm,
                    &pRecheck
                );

            if (rcRecheck == SQLITE_OK && pRecheck != NULL) {
                int recheck_value;

                recheck_value = sqlite3_value_int(pRecheck);

                if (recheck_value == 0) {
                    needs_recheck_filter = 0;
                }
                else if (recheck_value == 1) {
                    needs_recheck_filter = 1;
                }
            }
        }

        if (rcHigh == SQLITE_OK &&
            rcLow == SQLITE_OK &&
            pHigh != NULL &&
            pLow != NULL &&
            v->total_blocks > 0)
        {
            double high = 0.0;
            double low = 0.0;

            int okHigh;
            int okLow;

            okHigh = brinSqlValueAsDouble(v, pHigh, &high);
            okLow = brinSqlValueAsDouble(v, pLow, &low);

            if (okHigh == SQLITE_OK && okLow == SQLITE_OK) {
                int start = v->total_blocks;
                int end = -1;
                int candidate_blocks;
                int output_ranges;

                if (low > high) {
                    double tmp;

                    tmp = low;
                    low = high;
                    high = tmp;
                }

                DEBUG_PRINT(
                    "Planning range normalized to [%.6f, %.6f]\n",
                    low,
                    high
                );

                brinFindCandidateRange(
                    v,
                    low,
                    high,
                    &start,
                    &end
                );

                if (start != v->total_blocks && end >= start) {
                    candidate_blocks = end - start + 1;

                    output_ranges =
                        brinEstimateOutputRangeCount(
                            v,
                            start,
                            end,
                            low,
                            high,
                            needs_recheck_filter
                        );

                    if (output_ranges <= 0) {
                        output_ranges = 1;
                    }

                    pIdxInfo->estimatedRows = output_ranges;

                    /*
                     * Cost is still related to candidate blocks,
                     * because those blocks drive the rowid ranges
                     * that the base table will read.
                     */
                    pIdxInfo->estimatedCost =
                        (double)candidate_blocks;

                    DEBUG_PRINT(
                        "Exact candidate block interval: [%d, %d]\n",
                        start,
                        end
                    );

                    DEBUG_PRINT(
                        "Exact candidate blocks: %d\n",
                        candidate_blocks
                    );

                    DEBUG_PRINT(
                        "Estimated coalesced output ranges: %d\n",
                        output_ranges
                    );
                }
                else {
                    pIdxInfo->estimatedRows = 1;
                    pIdxInfo->estimatedCost = 1.0;

                    DEBUG_PRINT(
                        "Literal range produces no candidates\n"
                    );
                }
            }
            else {
                pIdxInfo->estimatedRows = 2;
                pIdxInfo->estimatedCost = 2.0;

                DEBUG_PRINT(
                    "RHS values not parseable in xBestIndex\n"
                );
            }
        }
        else {
            /*
             * This is common when using host parameters.
             *
             * With coalescing, the output is usually small:
             * left boundary, middle fully-covered, right boundary.
             */
            pIdxInfo->estimatedRows = 3;
            pIdxInfo->estimatedCost = 3.0;

            DEBUG_PRINT(
                "RHS values not available in xBestIndex\n"
            );
        }

        if (pIdxInfo->nOrderBy == 1 &&
            pIdxInfo->aOrderBy[0].iColumn == 2 &&
            pIdxInfo->aOrderBy[0].desc == 0)
        {
            pIdxInfo->orderByConsumed = 1;
            DEBUG_PRINT("ORDER BY start_rowid ASC consumed\n");
        }
    }
    else {
        int blocks = 1;

        if (v->total_blocks > 0) {
            blocks = v->total_blocks;
        }

        pIdxInfo->estimatedRows = blocks;
        pIdxInfo->estimatedCost = 1000000.0;

        DEBUG_PRINT(
            "BRIN plan rejected: both bounds are required\n"
        );
    }

    return SQLITE_OK;
}


/* --------------------------------------------------
 * xFilter
 *
 * PURPOSE
 * -------
 * Start executing a BRIN scan.
 *
 * INPUT FROM xBestIndex
 * ---------------------
 * argv[0] = high
 * argv[1] = low
 * argv[2] = optional needs_recheck filter
 *
 * OUTPUT BEHAVIOR
 * ---------------
 * The original implementation returned one virtual row per
 * candidate BRIN block.
 *
 * This implementation returns one virtual row per coalesced
 * output segment.
 *
 * Example:
 *
 *   block 10      -> needs_recheck = 1
 *   block 11..20  -> needs_recheck = 0
 *   block 21      -> needs_recheck = 1
 *
 * The cursor will output 3 rows, not 12 rows.
 * -------------------------------------------------- */
static int brinFilter(
    sqlite3_vtab_cursor *cur,
    int idxNum,
    const char *idxStr,
    int argc,
    sqlite3_value **argv
){
    BrinCursor *c = (BrinCursor*)cur;
    BrinVtab *v = c->v;

    double high = 0.0;
    double low = 0.0;

    int rcHigh;
    int rcLow;
    int rc;

    int start = 0;
    int end = -1;
    int candidate_blocks = 0;

    (void)idxNum;
    (void)idxStr;

    DEBUG_PRINT("[BRIN] brinFilter()\n");

    c->eof = 1;

    brinResetOutputRanges(c);

    c->needs_recheck_filter = -1;

    /*
     * Valid arg counts:
     *
     *   2 -> high, low
     *   3 -> high, low, needs_recheck
     */
    if (argc != 2 && argc != 3) {
        DEBUG_PRINT("xFilter called with invalid argc=%d\n", argc);
        return SQLITE_OK;
    }

    if (argc == 3) {
        int filter_value;

        filter_value = sqlite3_value_int(argv[2]);

        if (filter_value == 0) {
            c->needs_recheck_filter = 0;
        }
        else if (filter_value == 1) {
            c->needs_recheck_filter = 1;
        }
        else {
            /*
             * Invalid needs_recheck value.
             *
             * Return no rows.
             */
            DEBUG_PRINT(
                "Invalid needs_recheck filter value: %d\n",
                filter_value
            );
            return SQLITE_OK;
        }
    }

    rc = brinIncrementalUpdate(v);
    if (rc != SQLITE_OK) {
        DEBUG_PRINT("brinIncrementalUpdate failed: %d\n", rc);
        return rc;
    }

    if (v->total_blocks == 0) {
        DEBUG_PRINT("BRIN index is empty\n");
        return SQLITE_OK;
    }

    rcHigh = brinSqlValueAsDouble(v, argv[0], &high);
    rcLow = brinSqlValueAsDouble(v, argv[1], &low);

    if (rcHigh != SQLITE_OK || rcLow != SQLITE_OK) {
        DEBUG_PRINT("Invalid range values in xFilter\n");
        return SQLITE_OK;
    }

    if (low > high) {
        double tmp;

        tmp = low;
        low = high;
        high = tmp;
    }

    c->low = (sqlite3_int64)low;
    c->high = (sqlite3_int64)high;

    DEBUG_PRINT("Execution range normalized to [%.6f, %.6f]\n",
                low,
                high);

    start = v->total_blocks;
    end = -1;

    rc = brinFindCandidateRange(
        v,
        low,
        high,
        &start,
        &end
    );

    if (rc != SQLITE_OK)
        return rc;

    if (start == v->total_blocks || end < start) {
        DEBUG_PRINT("No candidate BRIN block found\n");
        return SQLITE_OK;
    }

    c->start_block = start;
    c->end_block = end;
    c->current_block = start;

    candidate_blocks = end - start + 1;

    DEBUG_PRINT("Candidate block interval: [%d, %d]\n",
                start,
                end);

    DEBUG_PRINT("Candidate blocks count: %d\n",
                candidate_blocks);

    DEBUG_PRINT("Total BRIN blocks: %d\n",
                v->total_blocks);

    DEBUG_PRINT("Candidate block selectivity: %.4f%%\n",
                100.0 * candidate_blocks /
                (double)v->total_blocks);

    rc = brinBuildOutputRanges(
        c,
        start,
        end,
        low,
        high
    );

    if (rc != SQLITE_OK)
        return rc;

    if (c->output_count <= 0) {
        DEBUG_PRINT("No output ranges after filtering\n");
        return SQLITE_OK;
    }

    c->current_output = 0;
    c->eof = 0;

    DEBUG_PRINT("Coalesced output ranges: %d\n",
                c->output_count);

    for (int i = 0; i < c->output_count; i++) {
        BrinOutputRange *out;

        out = &c->output_ranges[i];

        DEBUG_PRINT(
            "  output %d: blocks [%d, %d], needs_recheck=%d\n",
            i,
            out->start_block,
            out->end_block,
            out->needs_recheck
        );
    }

    return SQLITE_OK;
}


/* --------------------------------------------------
 * xNext
 *
 * PURPOSE
 * -------
 * Advance to the next coalesced output range.
 *
 * In the original implementation, xNext() advanced one
 * BRIN block at a time.
 *
 * In this version, xNext() advances one output segment
 * at a time.
 * -------------------------------------------------- */
static int brinNext(sqlite3_vtab_cursor *cur)
{
    BrinCursor *c = (BrinCursor*)cur;

    DEBUG_PRINT("[BRIN] brinNext()\n");

    c->current_output++;

    if (c->current_output >= c->output_count) {
        c->eof = 1;
    }

    return SQLITE_OK;
}


/* --------------------------------------------------
 * xEof
 *
 * PURPOSE
 * -------
 * Return non-zero when the cursor has no more rows.
 * -------------------------------------------------- */
static int brinEof(sqlite3_vtab_cursor *cur)
{
    BrinCursor *c = (BrinCursor*)cur;

    DEBUG_PRINT("[BRIN] brinEof()\n");

    return c->eof;
}


/* --------------------------------------------------
 * xColumn
 *
 * PURPOSE
 * -------
 * Return one column for the current coalesced output row.
 *
 * VIRTUAL TABLE SCHEMA
 * --------------------
 * column 0 -> min
 * column 1 -> max
 * column 2 -> start_rowid
 * column 3 -> end_rowid
 * column 4 -> needs_recheck
 *
 * COALESCING BEHAVIOR
 * -------------------
 * One output row may represent several contiguous BRIN
 * blocks.
 *
 * Therefore:
 *
 *   min         = min of first block in segment
 *   max         = max of last block in segment
 *   start_rowid = start_rowid of first block
 *   end_rowid   = end_rowid of last block
 * -------------------------------------------------- */
static int brinColumn(
    sqlite3_vtab_cursor *cur,
    sqlite3_context *ctx,
    int col
){
    BrinCursor *c = (BrinCursor*)cur;
    BrinOutputRange *out;
    BrinRange *first;
    BrinRange *last;

    DEBUG_PRINT("[BRIN] brinColumn()\n");

    if (c->eof) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    if (c->current_output < 0) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    if (c->current_output >= c->output_count) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    out = &c->output_ranges[c->current_output];

    first = &c->v->ranges[out->start_block];
    last = &c->v->ranges[out->end_block];

    switch (col)
    {
        case 0:
            if (first->type == BRIN_TYPE_INTEGER) {
                sqlite3_result_int64(
                    ctx,
                    (sqlite3_int64)first->u.num.min
                );
            }
            else if (first->type == BRIN_TYPE_REAL) {
                sqlite3_result_double(ctx, first->u.num.min);
            }
            else if (first->type == BRIN_TYPE_TEXT) {
                char buf[BRIN_DATETIME_BUFSZ];

                brinFormatEpochFixed(
                    first->u.txt.min_epoch,
                    buf,
                    sizeof(buf)
                );

                sqlite3_result_text(
                    ctx,
                    buf,
                    -1,
                    SQLITE_TRANSIENT
                );
            }
            else {
                sqlite3_result_null(ctx);
            }
            break;

        case 1:
            if (last->type == BRIN_TYPE_INTEGER) {
                sqlite3_result_int64(
                    ctx,
                    (sqlite3_int64)last->u.num.max
                );
            }
            else if (last->type == BRIN_TYPE_REAL) {
                sqlite3_result_double(ctx, last->u.num.max);
            }
            else if (last->type == BRIN_TYPE_TEXT) {
                char buf[BRIN_DATETIME_BUFSZ];

                brinFormatEpochFixed(
                    last->u.txt.max_epoch,
                    buf,
                    sizeof(buf)
                );

                sqlite3_result_text(
                    ctx,
                    buf,
                    -1,
                    SQLITE_TRANSIENT
                );
            }
            else {
                sqlite3_result_null(ctx);
            }
            break;

        case 2:
            sqlite3_result_int64(ctx, first->start_rowid);
            break;

        case 3:
            sqlite3_result_int64(ctx, last->end_rowid);
            break;

        case 4:
            sqlite3_result_int(ctx, out->needs_recheck);
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
 * Return a unique rowid for the current virtual output row.
 *
 * With coalescing, virtual rows are output segments rather
 * than raw BRIN blocks.
 *
 * The simplest stable rowid is current_output + 1.
 * -------------------------------------------------- */
static int brinRowid(
    sqlite3_vtab_cursor *cur,
    sqlite3_int64 *pRowid
){
    BrinCursor *c = (BrinCursor*)cur;

    DEBUG_PRINT("[BRIN] brinRowid()\n");

    if (!pRowid)
        return SQLITE_ERROR;

    if (c->eof) {
        *pRowid = 0;
        return SQLITE_OK;
    }

    *pRowid = (sqlite3_int64)c->current_output + 1;

    return SQLITE_OK;
}


/* --------------------------------------------------
 * xDisconnect
 *
 * PURPOSE
 * -------
 * Release all resources associated with one virtual
 * table instance.
 *
 * TEXT-AS-EPOCH NOTE
 * ------------------
 * In the base implementation, TEXT ranges owned heap
 * memory:
 *
 *   char *min
 *   char *max
 *
 * In this version, TEXT ranges store:
 *
 *   sqlite3_int64 min_epoch
 *   sqlite3_int64 max_epoch
 *
 * Therefore, there is no per-range TEXT heap memory to
 * release.
 * -------------------------------------------------- */
static int brinDisconnect(sqlite3_vtab *pVTab)
{
    BrinVtab *v = (BrinVtab*)pVTab;

    DEBUG_PRINT("[BRIN] brinDisconnect()\n");

    if (v) {
        if (v->ranges) {
            free(v->ranges);
            v->ranges = NULL;
        }

        if (v->table) {
            sqlite3_free(v->table);
            v->table = NULL;
        }

        if (v->column) {
            sqlite3_free(v->column);
            v->column = NULL;
        }

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
