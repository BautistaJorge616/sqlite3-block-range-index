#include <stdio.h>       // Standard I/O functions
#include <stdlib.h>      // Standard library functions
#include <sqlite3.h>     // SQLite library
#include <time.h>        // Time functions


double run_query(sqlite3 *db, const char *sql)
{
    sqlite3_stmt *stmt;
    struct timespec start, end;

    clock_gettime(CLOCK_MONOTONIC, &start);

    /* Clean page cache before each run */
    sqlite3_exec(db, "PRAGMA shrink_memory;", 0, 0, 0);

    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        /* consume rows */
    }

    sqlite3_finalize(stmt);

    clock_gettime(CLOCK_MONOTONIC, &end);

    double elapsed =
        (end.tv_sec - start.tv_sec) +
        (end.tv_nsec - start.tv_nsec) / 1e9;

    return elapsed;
}


double measure_build(sqlite3 *db, const char *sql)
{
    struct timespec start, end;

    clock_gettime(CLOCK_MONOTONIC, &start);

    /* Clean page cache before each run */
    sqlite3_exec(db, "PRAGMA shrink_memory;", 0, 0, 0);

    sqlite3_exec(db, sql, 0, 0, 0);

    clock_gettime(CLOCK_MONOTONIC, &end);

    return (end.tv_sec - start.tv_sec) +
           (end.tv_nsec - start.tv_nsec) / 1e9;
}


int main()
{

    sqlite3 *db;         // Pointer to SQLite database
    char *err_msg = 0;   // Error message holder
    int rc;              // Return code for SQLite operations

    // Open or create the SQLite database file named
    rc = sqlite3_open("test.db", &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    // Enable extension loading (mandatory for security reasons)
    rc = sqlite3_enable_load_extension(db, 1);
    if (rc != SQLITE_OK) {
        fprintf(stderr,
                "Error enabling extension loading: %s\n",
                sqlite3_errmsg(db));
        return rc;
    }

    // Load the extension file 'brin.so'
    rc = sqlite3_load_extension(db,
                                "./brin.so",         // Path to the module file
                                "sqlite3_brin_init", // Entry point function
                                &err_msg);

    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to load brin.so: %s\n", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Define BRIN strcture

    sqlite3_exec(db, "DROP TABLE IF EXISTS idx_1;", 0, 0, 0);

    const char *sql_brin_struct = "CREATE VIRTUAL TABLE idx_1 USING brin"
                                   "(logs, d_integer ,1000);";

    double build_time = measure_build(db, sql_brin_struct);

    printf("BRIN build time: %.12f seconds\n", build_time);

    double btree_build = measure_build(db,
    "CREATE INDEX btree_int ON logs(d_integer);");

    printf("BTree build time: %.12f seconds\n", btree_build);

    const char *btree_query =
        "SELECT * FROM logs "
        "WHERE d_integer BETWEEN 2762086934 AND 2942086934;";

    sqlite3_exec(db, "DROP INDEX btree_int;", 0, 0, 0);

    const char *brin_query =
        "SELECT l.* "
        "FROM idx_1 b JOIN logs l "
        "ON l.rowid BETWEEN b.start_rowid AND b.end_rowid "
        "WHERE b.min <= 2942086934 "
        "AND b.max >= 2762086934; ";

    double t1 =run_query(db, btree_query);
    double t2 =run_query(db, brin_query);

    printf("BTree time: %.12f seconds\n", t1);
    printf("BRIN  time: %.12f seconds\n", t2);

    sqlite3_close(db);
}

