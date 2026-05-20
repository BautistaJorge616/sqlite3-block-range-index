#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <time.h>

#define GIB_DIVISOR (1024.0 * 1024.0 * 1024.0)

typedef enum BrinAffinity {
    BRIN_TYPE_INTEGER,
    BRIN_TYPE_REAL,
    BRIN_TYPE_TEXT
} BrinAffinity;

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


double print_brin_vtab_size(sqlite3 *db, int block_size)
{
    sqlite3_stmt *stmt = NULL;
    sqlite3_int64 total_rows = 0;
    sqlite3_int64 total_blocks = 0;
    sqlite3_int64 brin_bytes = 0;
    double brin_gib = 0.0;
    int rc;

    if (!db) {
        printf("BRIN SIZE: database handle is NULL\n");
        return 0.0;
    }

    if (block_size <= 0) {
        printf("BRIN SIZE: invalid block_size=%d\n", block_size);
        return 0.0;
    }

    rc = sqlite3_prepare_v2(
        db,
        "SELECT COALESCE(MAX(rowid), 0) FROM logs;",
        -1,
        &stmt,
        NULL
    );

    if (rc != SQLITE_OK) {
        printf("BRIN SIZE: prepare failed: %s\n", sqlite3_errmsg(db));
        return 0.0;
    }

    rc = sqlite3_step(stmt);

    if (rc == SQLITE_ROW) {
        total_rows = sqlite3_column_int64(stmt, 0);
    } else {
        printf("BRIN SIZE: step failed: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return 0.0;
    }

    sqlite3_finalize(stmt);

    /*
     * ceil(total_rows / block_size), using integer arithmetic.
     */
    if (total_rows > 0) {
        total_blocks =
            (total_rows + block_size - 1) / block_size;
    } else {
        total_blocks = 0;
    }

    brin_bytes =
        total_blocks * (sqlite3_int64)sizeof(BrinRange);

    brin_gib =
        (double)brin_bytes / GIB_DIVISOR;

    printf("----------------------------------------\n");
    printf("BRIN VTab estimated in-memory size\n");
    printf("sizeof(BrinRange): %zu bytes\n", sizeof(BrinRange));
    printf("total_rows: %lld\n", (long long)total_rows);
    printf("estimated_blocks: %lld\n", (long long)total_blocks);
    printf("BRIN SIZE: %lld bytes\n", (long long)brin_bytes);
    printf("BRIN SIZE: %.9f GiB\n", brin_gib);
    printf("----------------------------------------\n");

    return (double)brin_bytes;
}

double print_btree_index_size(sqlite3 *db, const char *index_name)
{
    sqlite3_stmt *stmt = NULL;
    sqlite3_int64 index_bytes = 0;
    double index_gib = 0.0;
    int rc;

    if (!db) {
        printf("B-tree SIZE: database handle is NULL\n");
        return 0.0;
    }

    if (!index_name) {
        printf("B-tree SIZE: index name is NULL\n");
        return 0.0;
    }

    rc = sqlite3_prepare_v2(
        db,
        "SELECT COALESCE(SUM(pgsize), 0) "
        "FROM dbstat "
        "WHERE name = ?;",
        -1,
        &stmt,
        NULL
    );

    if (rc != SQLITE_OK) {
        printf("B-tree SIZE: dbstat unavailable or prepare failed: %s\n",
               sqlite3_errmsg(db));
        return 0.0;
    }

    sqlite3_bind_text(stmt, 1, index_name, -1, SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);

    if (rc == SQLITE_ROW) {
        index_bytes = sqlite3_column_int64(stmt, 0);
    } else {
        printf("B-tree SIZE: step failed: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return 0.0;
    }

    sqlite3_finalize(stmt);

    index_gib =
        (double)index_bytes / GIB_DIVISOR;

    printf("----------------------------------------\n");
    printf("B-tree index physical size\n");
    printf("index name: %s\n", index_name);
    printf("B-tree SIZE: %lld bytes\n", (long long)index_bytes);
    printf("B-tree SIZE: %.9f GiB\n", index_gib);
    printf("----------------------------------------\n");

    return (double)index_bytes;
}


double now()
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC,&t);
    return t.tv_sec + t.tv_nsec / 1e9;
}

double run_query(sqlite3 *db,const char *sql)
{

    sqlite3_stmt *stmt;


    sqlite3_exec(db, "PRAGMA shrink_memory;", 0, 0, 0);
    sqlite3_db_release_memory(db);

    sqlite3_prepare_v2(db,sql,-1,&stmt,NULL);

    double start = now();

    int row_counter = 0;
    while(sqlite3_step(stmt)==SQLITE_ROW){
        row_counter++;
    }

    double total_time = now() - start;

    printf("ROW_COUNTER: %d\n", row_counter);
    sqlite3_finalize(stmt);

    return total_time;

}

double run_exec(sqlite3 *db,const char *sql)
{
    double start = now();

    sqlite3_exec(db,sql,0,0,0);

    return now() - start;
}

int main(int argc, char *argv[])
{

    if (argc < 2) {
        printf("Use: %s <block_size>\n", argv[0]);
        return 1;
    }

    int block_size = atoi(argv[1]);

    printf("--------INTEGER--------\n");
    printf("BLOCK SIZE: %d\n", block_size);

    sqlite3 *db;
    char *err=0;

    sqlite3_open("test.db",&db);

    sqlite3_enable_load_extension(db,1);

    sqlite3_load_extension(
        db,
        "./brin.so",
        "sqlite3_brin_init",
        &err
    );



    // BUILD

    char * btree_create = "CREATE INDEX btree_int ON logs(d_integer);";
    char brin_create[256];
    snprintf(brin_create, sizeof(brin_create),
            "CREATE VIRTUAL TABLE idx_1 USING brin(logs, d_integer, %d);",
            block_size);


    double build_btree = run_exec(db, btree_create);
    double build_brin  = run_exec(db, brin_create);

    double build_speedup = build_btree / build_brin;

    printf("%.6f BUILD: B-tree\n", build_btree);
    printf("%.6f BUILD: BRIN\n",   build_brin);
    printf("----------------------------------------\n");
    printf("%.6fx BUILD SPEEDUP\n", build_speedup);
    printf("----------------------------------------\n");


    // Queries
    double query_btree = run_query(
                               db,
                               "SELECT * FROM logs "
                               "WHERE d_integer "
                               "BETWEEN 46577836800 AND 47477835000;"
                             );

    double btree_size_bytes =
        print_btree_index_size(db, "btree_int");

    double drop_btree =
        run_exec(db, "DROP INDEX btree_int;");

    double query_brin = run_query(
                              db,
                              "SELECT l.* "
                              "FROM idx_1 AS b "
                              "JOIN logs AS l "
                              "ON l.rowid BETWEEN b.start_rowid "
                              "AND b.end_rowid "
                              "WHERE b.min <= 47477835000 "
                              "AND b.max >= 46577836800 "
                              "AND b.needs_recheck = 0 "

                              "UNION ALL "

                              "SELECT l.* "
                              "FROM idx_1 AS b "
                              "JOIN logs AS l "
                              "ON l.rowid BETWEEN b.start_rowid "
                              "AND b.end_rowid "
                              "WHERE b.min <= 47477835000 "
                              "AND b.max >= 46577836800 "
                              "AND b.needs_recheck = 1 "
                              "AND l.d_integer BETWEEN 46577836800 "
                              "AND 47477835000;"
                             );

    double brin_size_bytes =
        print_brin_vtab_size(db, block_size);

    double query_default_idx = run_query(
                               db,
                               "SELECT * FROM logs "
                               "WHERE d_integer "
                               "BETWEEN 46577836800 AND 47477835000;"
                             );


    printf("%.6f QUERY: B-tree\n", query_btree);
    printf("%.6f QUERY: Default\n", query_default_idx);
    printf("%.6f QUERY: BRIN\n",   query_brin);


    double query_speedup_1 = query_btree / query_brin;
    double query_speedup_2 = query_default_idx / query_brin;


    printf("----------------------------------------\n");
    printf("%.6fx QUERY SPEEDUP\n", query_speedup_1 );
    printf("%.6fx QUERY SPEEDUP\n", query_speedup_2 );
    printf("----------------------------------------\n");

    if (btree_size_bytes > 0.0 && brin_size_bytes > 0.0) {
        double size_ratio = btree_size_bytes / brin_size_bytes;

        printf("----------------------------------------\n");
        printf("B-tree is approximately %.2fx larger than BRIN\n",
           size_ratio);
        printf("----------------------------------------\n");
    } else {
        printf("----------------------------------------\n");
        printf("SIZE RATIO: unavailable\n");
        printf("----------------------------------------\n");
   }

    // DROP
    double drop_brin  = run_exec(db, "DROP TABLE idx_1;");

    double drop_speedup = drop_btree / drop_brin;

    printf("%.6f DROP: B-tree\n",   drop_btree);
    printf("%.6f DROP: BRIN\n",     drop_brin);
    printf("----------------------------------------\n");
    printf("%.6fx DROP SPEEDUP\n", drop_speedup );
    printf("----------------------------------------\n");

    sqlite3_close(db);

    return 0;
}

