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

    sqlite3_exec(db, "PRAGMA shrink_memory;", 0, 0, 0);
    sqlite3_prepare_v2(db,sql,-1,&stmt,NULL);

    while(sqlite3_step(stmt)==SQLITE_ROW){

/*
      // id (INTEGER) -> columna 0
      int id = sqlite3_column_int(stmt, 0);

      // d_integer (INTEGER) -> columna 1
      int d_int = sqlite3_column_int(stmt, 1);

      // d_text (TEXT) -> columna 2
      const unsigned char *d_text = sqlite3_column_text(stmt, 2);

      // d_real (REAL) -> columna 3
      double d_real = sqlite3_column_double(stmt, 3);

      // d_datetime (DATETIME se maneja como texto) -> columna 4
      const unsigned char *d_dt = sqlite3_column_text(stmt, 4);

      // Imprimir con formato
      printf("%-3d | %-10d | %-15s | %-8.2f | %-19s\n",
                id, d_int, d_text, d_real, d_dt);
*/

    }

    sqlite3_finalize(stmt);

    return now() - start;
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

    printf("--------TEXT--------\n");
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

    char * btree_create = "CREATE INDEX btree_text ON logs(d_text);";
    char brin_create[256];
    snprintf(brin_create, sizeof(brin_create),
            "CREATE VIRTUAL TABLE idx_1 USING brin(logs, d_text, %d);",
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
                                   "BETWEEN '3445-12-29 08:00:00' "
                                   "AND '3474-07-06 23:30:00' ; "
                                 );

    double drop_btree = run_exec(db, "DROP INDEX btree_text;");


    double query_brin = run_query(
                                  db,
                                  "SELECT * FROM logs AS l "
                                  "JOIN idx_1 AS b "
                                  "ON l.rowid BETWEEN b.start_rowid "
                                  "AND b.end_rowid "
                                  "WHERE b.min <= '3474-07-06 23:30:00' "
                                  "AND b.max >= '3445-12-29 08:00:00' "
                                  "AND l.d_text BETWEEN '3445-12-29 08:00:00' "
                                  "AND '3474-07-06 23:30:00';"
                                 );

    printf("%.6f QUERY: B-tree\n", query_btree);
    printf("%.6f QUERY: BRIN\n",   query_brin);

    double query_speedup = query_btree / query_brin;

    printf("----------------------------------------\n");
    printf("%.6fx QUERY SPEEDUP\n", query_speedup );
    printf("----------------------------------------\n");


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

