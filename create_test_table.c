
#include <stdio.h>       // Standard I/O functions
#include <stdlib.h>      // Standard library functions
#include <sqlite3.h>     // SQLite library
#include <time.h>        // Time functions

int main() {
    sqlite3 *db;         // Pointer to SQLite database
    char *err_msg = 0;   // Error message holder
    int rc;              // Return code for SQLite operations

    // Open or create the SQLite database file named "logs.db"
    rc = sqlite3_open("test.db", &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    // SQL statement to create the logs table with four columns
    const char *sql_create_table =
        "CREATE TABLE IF NOT EXISTS logs ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "integer_date INTEGER, "
        "real_date REAL, "
        "text_date TEXT);";

    // Execute the SQL statement to create the table
    rc = sqlite3_exec(db, sql_create_table, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Table creation failed: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return rc;
    }

    // Prepare the SQL insert statement with placeholders
    sqlite3_stmt *stmt;
    const char *sql_insert = "INSERT INTO logs (integer_date, real_date, text_date) VALUES (?, ?, ?);";
    rc = sqlite3_prepare_v2(db, sql_insert, -1, &stmt, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Insert statement preparation failed: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return rc;
    }

    // Get the current time as the base timestamp
    time_t base_time = time(NULL);

    // Begin a transaction for faster bulk insertion
    sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, NULL);

    // Loop to insert 10,000 rows
    for (int i = 0; i < 10000; i++) {
        // Calculate the timestamp for each row, spaced 30 minutes apart
        time_t current_time = base_time + (i * 1800);  // 1800 seconds = 30 minutes

        // Convert timestamp to human-readable format
        struct tm *tm_info = localtime(&current_time);
        char text_date[30];
        strftime(text_date, sizeof(text_date), "%Y-%m-%d %H:%M:%S", tm_info);

        // Convert timestamp to double for real_date
        double real_date = (double)current_time;

        // Bind values to the SQL statement
        sqlite3_bind_int(stmt, 1, (int)current_time);         // integer_date
        sqlite3_bind_double(stmt, 2, real_date);              // real_date
        sqlite3_bind_text(stmt, 3, text_date, -1, SQLITE_STATIC); // text_date

        // Execute the insert statement
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            fprintf(stderr, "Insert failed at row %d: %s\n", i, sqlite3_errmsg(db));
        }

        // Reset the statement for the next iteration
        sqlite3_reset(stmt);
    }

    // End the transaction
    sqlite3_exec(db, "END TRANSACTION;", NULL, NULL, NULL);

    // Finalize the statement and close the database
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    // Print completion message
    printf("Inserted 10,000 log entries spaced 30 minutes apart.\n");
    return 0;
}


