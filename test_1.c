
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
                                "./brin.so",           // Path to the module file
                                "sqlite3_brin_init", // Entry point function
                                &err_msg);

    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to load brin.so: %s\n", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // EXECUTE THE SQL STATEMENTS
    const char *drop = "DROP TABLE brin_index;";
    rc = sqlite3_exec(db, drop, NULL, 0, &err_msg);

    if(rc != SQLITE_OK) {
      fprintf(stderr, "SQL Error: %s\n", err_msg);
    } else {
      fprintf(stdout, "Table dropped.\n");
    }

    const char *create = "CREATE VIRTUAL TABLE brin_index USING brin('logs', 'integer_date');";

    if(rc != SQLITE_OK) {
      fprintf(stderr, "SQL Error: %s\n", err_msg);
    } else {
      fprintf(stdout, "Table created.\n");
    }

    // Close the database
    sqlite3_close(db);

    return 0;
}


