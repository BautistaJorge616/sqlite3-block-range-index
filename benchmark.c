#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <time.h>

double now()
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC,&t);
    return t.tv_sec + t.tv_nsec / 1e9;
}

double run_query(sqlite3 *db,const char *sql)
{
    sqlite3_stmt *stmt;

    double start = now();

    sqlite3_prepare_v2(db,sql,-1,&stmt,NULL);

    while(sqlite3_step(stmt)==SQLITE_ROW){}

    sqlite3_finalize(stmt);

    return now() - start;
}

double run_exec(sqlite3 *db,const char *sql)
{
    double start = now();

    sqlite3_exec(db,sql,0,0,0);

    return now() - start;
}

int main()
{
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
    double build_btree = run_exec(db, "CREATE INDEX btree_int ON logs(d_integer);");
    double build_brin  = run_exec(db, "CREATE VIRTUAL TABLE idx_1 USING brin(logs,d_integer,100000);");

    double build_speedup = build_btree / build_brin;

    printf("%.6f BUILD: B-tree\n", build_btree);
    printf("%.6f BUILD: BRIN\n",   build_brin);
    printf("----------------------------------------\n");
    printf("%.6fx BUILD SPEEDUP\n", build_speedup);
    printf("----------------------------------------\n");


    // Query
    double query_btree = run_query(
                                   db,
                                   "SELECT * FROM logs "
                                   "WHERE d_integer "
                                   "BETWEEN 5500000 AND 6500000;"
                                 );

    double query_brin = run_query(
                                  db,
                                  "SELECT * FROM logs "
                                  "FROM idx_1 b JOIN logs l  "
                                  "ON l.rowid BETWEEN b.start_rowid AND b.end_rowid "
                                  "WHERE b.min <= 5500000 AND b.max >= 6500000;"
                                 );

    printf("%.6f QUERY: B-tree\n", query_btree);
    printf("%.6f QUERY: BRIN\n",   query_brin);

    double query_speedup = query_btree / query_brin;

    printf("----------------------------------------\n");
    printf("%.6fx QUERY SPEEDUP\n", query_speedup );
    printf("----------------------------------------\n");


    // DROP
    double drop_btree = run_exec(db, "DROP INDEX btree_int;");
    double drop_brin  = run_exec(db, "DROP TABLE idx_1;");

    double drop_speedup = drop_btree / drop_brin;

    printf("%.6f DROP: B-tree\n",   drop_btree);
    printf("%.6f DROP: BRIN\n",     drop_brin);
    printf("----------------------------------------\n");
    printf("%.6fx BUILD SPEEDUP\n", drop_speedup );
    printf("----------------------------------------\n");

    sqlite3_close(db);

    return 0;
}

