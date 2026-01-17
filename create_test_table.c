
#include <stdio.h>       // Standard I/O functions
#include <stdlib.h>      // Standard library functions
#include <sqlite3.h>     // SQLite library
#include <time.h>        // Time functions

int main() {
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


    // Drop table if exists
    const char *sql_drop_table = "DROP TABLE IF EXISTS logs;";
    rc = sqlite3_exec(db, sql_drop_table, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Table drop failed: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return rc;
    }


    // SQL statement to create the logs table with four columns
    const char *sql_create_table =
        "CREATE TABLE IF NOT EXISTS logs ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "d_integer          INTEGER,"                 // INTEGER
        "d_text             TEXT,"                    // TEXT
        "d_real             REAL,"                   // REAL
        "d_datetime         DATETIME);";

    // Execute the SQL statement to create the table
    rc = sqlite3_exec(db, sql_create_table, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Table creation failed: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return rc;
    } else {
      fprintf(stdout, "Table created.\n");
    }


    // Insert rows
    sqlite3_stmt *stmt;

    const char *sql_insert = "INSERT INTO logs "
                             " (d_integer, "
                             "  d_text, "
                             "  d_real, "
                             "  d_datetime "
                             " ) VALUES (?, ?, ?, ?);";

    sqlite3_prepare_v2(db, sql_insert, -1, &stmt, 0);

    time_t tiempo_raw = time(NULL);
    struct tm *fecha = localtime(&tiempo_raw);

    // Iniciar transacción (optimiza la escritura de los 100 registros)
    sqlite3_exec(db, "BEGIN TRANSACTION;", 0, 0, 0);

    for (int i = 0; i < 100; i++) {
        // Normalizamos y obtenemos el timestamp actual del bucle
        time_t t_actual = mktime(fecha);

        char buffer_dt[20]; // YYYY-MM-DD HH:MM:SS
        strftime(buffer_dt, sizeof(buffer_dt), "%Y-%m-%d %H:%M:%S", fecha);

        // Bind de los valores
        sqlite3_bind_int64(stmt, 1, (sqlite3_int64)t_actual);
        sqlite3_bind_text(stmt, 2, buffer_dt, -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(stmt, 3, (double)t_actual);
        sqlite3_bind_text(stmt, 4, buffer_dt, -1, SQLITE_TRANSIENT);

        sqlite3_step(stmt);
        sqlite3_reset(stmt);

        fecha->tm_min += 30;
    }

    sqlite3_exec(db, "END TRANSACTION;", 0, 0, 0);


    sqlite3_finalize(stmt);
    sqlite3_close(db);

    return 0;
}


