#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <time.h>

#define TOTAL_ROWS 10000000

double now()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

double run_query(sqlite3 *db, const char *sql)
{
    sqlite3_stmt *stmt;

    sqlite3_exec(db, "PRAGMA shrink_memory;", 0, 0, 0);

    double start = now();

    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);

    while (sqlite3_step(stmt) == SQLITE_ROW) {}

    sqlite3_finalize(stmt);

    return now() - start;
}

void print_plan(sqlite3 *db, const char *sql)
{
    sqlite3_stmt *stmt;

    char explain[1024];
    snprintf(explain, sizeof(explain), "EXPLAIN QUERY PLAN %s", sql);

    sqlite3_prepare_v2(db, explain, -1, &stmt, NULL);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        printf("PLAN: %s\n", sqlite3_column_text(stmt, 3));
    }

    sqlite3_finalize(stmt);
}

int main()
{
    sqlite3 *db;
    char *err_msg = 0;

    sqlite3_open("test.db", &db);
    sqlite3_enable_load_extension(db, 1);

    sqlite3_load_extension(db, "./brin.so", "sqlite3_brin_init", &err_msg);

    /* BLOCK SIZES (10M base) */
    int block_sizes[] = {
        1000,       // 0.01%
        5000,       // 0.05%
        10000,      // 0.1%
        50000,      // 0.5%
        100000,     // 1%
        200000,     // 2%
        500000,     // 5%
        1000000     // 10%
    };

    int n_blocks = sizeof(block_sizes) / sizeof(int);

    /* QUERY SIZES */
    int query_sizes[] = {
        10,
        50,
        100,
        500,
        1000,
        5000,
        10000,
        50000,
        100000,
        200000,
        500000,
        1000000,
        2000000,
        5000000,
        8000000
    };

    int n_queries = sizeof(query_sizes) / sizeof(int);

    for (int b = 0; b < n_blocks; b++)
    {
        int block = block_sizes[b];

        printf("\n=====================================\n");
        printf("BLOCK SIZE = %d (%.4f%% of table)\n",
               block,
               (block * 100.0) / TOTAL_ROWS);
        printf("=====================================\n");

        sqlite3_exec(db, "DROP TABLE IF EXISTS idx_1;", 0, 0, 0);

        char create_brin[256];
        snprintf(create_brin, sizeof(create_brin),
            "CREATE VIRTUAL TABLE idx_1 USING brin(logs, d_integer, %d);",
            block);

        double t_brin_build_start = now();
        sqlite3_exec(db, create_brin, 0, 0, 0);
        double t_brin_build = now() - t_brin_build_start;

        printf("BRIN build time: %.6f sec\n", t_brin_build);

        sqlite3_exec(db, "DROP INDEX IF EXISTS idx_btree;", 0, 0, 0);

        double t_btree_build_start = now();
        sqlite3_exec(db, "CREATE INDEX idx_btree ON logs(d_integer);", 0, 0, 0);
        double t_btree_build = now() - t_btree_build_start;

        printf("BTree build time: %.6f sec\n", t_btree_build);

        for (int q = 0; q < n_queries; q++)
        {
            int size = query_sizes[q];

            long start_val = 1773961872;
            long end_val = start_val + (size * 1800); // 30 min

            char btree_query[512];
            snprintf(btree_query, sizeof(btree_query),
                "SELECT * FROM logs WHERE d_integer BETWEEN %ld AND %ld;",
                start_val, end_val);

            char brin_query[512];
            snprintf(brin_query, sizeof(brin_query),
                "SELECT l.* FROM idx_1 b JOIN logs l "
                "ON l.rowid BETWEEN b.start_rowid AND b.end_rowid "
                "WHERE b.min <= %ld AND b.max >= %ld;",
                end_val, start_val);

            printf("\n--- Query size = %d (%.4f%%) ---\n",
                   size,
                   (size * 100.0) / TOTAL_ROWS);

            printf("BTree PLAN:\n");
            print_plan(db, btree_query);

            double t_btree = run_query(db, btree_query);

            sqlite3_exec(db, "DROP INDEX idx_btree;", 0, 0, 0);

            printf("BRIN PLAN:\n");
            print_plan(db, brin_query);

            double t_brin = run_query(db, brin_query);

            double speedup = t_btree / t_brin;

            printf("BTree: %.6f | BRIN: %.6f | Speedup: %.2fx\n",
                   t_btree, t_brin, speedup);

            sqlite3_exec(db, "CREATE INDEX idx_btree ON logs(d_integer);", 0, 0, 0);
        }
    }

    sqlite3_close(db);
    return 0;
}
