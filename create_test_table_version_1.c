#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <time.h>

#define TOTAL_ROWS 50000000LL
#define COMMIT_EVERY 1000000LL

#define START_YEAR 2020
#define START_MONTH 1
#define START_DAY 1
#define START_HOUR 0
#define START_MIN 0
#define START_SEC 0

static int exec_sql(sqlite3 *db, const char *sql) {
    char *err_msg = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);

    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\nSQL: %s\n",
                err_msg ? err_msg : sqlite3_errmsg(db),
                sql);
        sqlite3_free(err_msg);
        return rc;
    }

    return SQLITE_OK;
}

int main(void) {
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    char *err_msg = NULL;
    int rc = SQLITE_OK;

    rc = sqlite3_open("test.db", &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return rc;
    }

    rc = exec_sql(db, "PRAGMA journal_mode = WAL;");
    if (rc != SQLITE_OK) goto cleanup;

    rc = exec_sql(db, "PRAGMA synchronous = NORMAL;");
    if (rc != SQLITE_OK) goto cleanup;

    rc = exec_sql(db, "PRAGMA temp_store = MEMORY;");
    if (rc != SQLITE_OK) goto cleanup;

    rc = sqlite3_enable_load_extension(db, 1);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error enabling extension loading: %s\n",
                sqlite3_errmsg(db));
        goto cleanup;
    }

    rc = sqlite3_load_extension(
        db,
        "./brin.so",
        "sqlite3_brin_init",
        &err_msg
    );

    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to load brin.so: %s\n",
                err_msg ? err_msg : sqlite3_errmsg(db));
        sqlite3_free(err_msg);
        err_msg = NULL;
        goto cleanup;
    }

    rc = exec_sql(db, "DROP TABLE IF EXISTS logs;");
    if (rc != SQLITE_OK) goto cleanup;

    rc = exec_sql(db,
        "CREATE TABLE logs ("
        "id INTEGER PRIMARY KEY, "
        "d_integer INTEGER, "
        "d_text TEXT, "
        "d_real REAL, "
        "d_datetime DATETIME"
        ");"
    );
    if (rc != SQLITE_OK) goto cleanup;

    const char *sql_insert =
        "INSERT INTO logs "
        "(d_integer, d_text, d_real, d_datetime) "
        "VALUES (?, ?, ?, ?);";

    rc = sqlite3_prepare_v2(db, sql_insert, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Prepare failed: %s\n", sqlite3_errmsg(db));
        goto cleanup;
    }

    /*
     * Fixed deterministic start date.
     *
     * struct tm uses:
     *   tm_year = years since 1900
     *   tm_mon  = months since January, range 0-11
     */
    struct tm fecha_value;
    fecha_value.tm_year = START_YEAR - 1900;
    fecha_value.tm_mon = START_MONTH - 1;
    fecha_value.tm_mday = START_DAY;
    fecha_value.tm_hour = START_HOUR;
    fecha_value.tm_min = START_MIN;
    fecha_value.tm_sec = START_SEC;
    fecha_value.tm_isdst = -1;

    rc = exec_sql(db, "BEGIN TRANSACTION;");
    if (rc != SQLITE_OK) goto cleanup;

    for (sqlite3_int64 i = 0; i < TOTAL_ROWS; i++) {
        time_t t_actual = mktime(&fecha_value);

        char buffer_dt[20];

        strftime(
            buffer_dt,
            sizeof(buffer_dt),
            "%Y-%m-%d %H:%M:%S",
            &fecha_value
        );

        sqlite3_bind_int64(stmt, 1, (sqlite3_int64)t_actual);
        sqlite3_bind_text(stmt, 2, buffer_dt, -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(stmt, 3, (double)t_actual);
        sqlite3_bind_text(stmt, 4, buffer_dt, -1, SQLITE_TRANSIENT);

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            fprintf(stderr,
                    "Insert failed at row %lld: %s\n",
                    (long long)i,
                    sqlite3_errmsg(db));

            exec_sql(db, "ROLLBACK;");
            goto cleanup;
        }

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        /*
         * Add 30 minutes.
         *
         * mktime() normalizes the struct tm on the next loop,
         * so values like tm_min = 90 become +1 hour, 30 min.
         */
        fecha_value.tm_min += 30;

        if ((i + 1) % COMMIT_EVERY == 0) {
            rc = exec_sql(db, "COMMIT;");
            if (rc != SQLITE_OK) goto cleanup;

            fflush(stdout);

            rc = exec_sql(db, "BEGIN TRANSACTION;");
            if (rc != SQLITE_OK) goto cleanup;
        }
    }

    rc = exec_sql(db, "COMMIT;");
    if (rc != SQLITE_OK) goto cleanup;

    printf("Done. Inserted %lld rows.\n", (long long)TOTAL_ROWS);

cleanup:
    if (stmt) {
        sqlite3_finalize(stmt);
    }

    if (db) {
        sqlite3_close(db);
    }

    return rc == SQLITE_OK ? 0 : rc;
}
