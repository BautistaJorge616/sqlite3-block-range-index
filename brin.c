#include <sqlite3ext.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <time.h>   // Needed for struct tm and time conversion

SQLITE_EXTENSION_INIT1

typedef enum {
    BRIN_TYPE_INTEGER,
    BRIN_TYPE_REAL,
    BRIN_TYPE_TEXT
} BrinAffinity;


// A generic representation of a BRIN block summary
typedef struct BrinRange {

    BrinAffinity type;       // INTEGER / REAL / TEXT

    union {
        struct {             // numeric (INTEGER or REAL)
            double min;
            double max;
        } num;

        struct {             // text (lexicographic min/max)
            char *min;
            char *max;
        } txt;

    } u;

    sqlite3_int64 start_rowid;
    sqlite3_int64 end_rowid;

} BrinRange;


const char* get_affinity(const char *declared_type) {
    if (!declared_type) return "BLOB";

    char type[64];
    snprintf(type, sizeof(type), "%s", declared_type);
    for (int i = 0; type[i]; i++) {
        type[i] = toupper(type[i]);
    }

    if (strstr(type, "INT")) return "INTEGER";
    if (strstr(type, "CHAR") || strstr(type, "CLOB")
                             || strstr(type, "TEXT")) return "TEXT";
    if (strstr(type, "REAL") || strstr(type, "FLOA")
                             || strstr(type, "DOUB")) return "REAL";
    if (strstr(type, "NUMERIC") || strstr(type, "DECIMAL")
                                || strstr(type, "BOOLEAN")
                                || strstr(type, "DATE")
                                || strstr(type, "DATETIME")) return "NUMERIC";

    return "BLOB"; // default
}


/* --- Structure representing the BRIN virtual table --- */
typedef struct {
  sqlite3_vtab base;   // Base class - required by SQLite
  char *table;         // Physical table name (the one we index)
  char *column;        // Column name being indexed
  int block_size;      // Number of rows per block
  BrinAffinity affinity;
  BrinRange *ranges;      // dynamically built index ranges
  int total_blocks;
  int index_ready;
  sqlite3 *db;         // Pointer to the sqlite3 DB handle (stored at xConnect)
} BrinVtab;


/* --- Cursor structure to iterate over BRIN ranges --- */
typedef struct {
  sqlite3_vtab_cursor base;  // Base class - required by SQLite
  int current_block;         // Current block index while scanning
  sqlite3_int64 current_rowid;
  int eof;
} BrinCursor;


/* --- xConnect / xCreate --- */
static int brinConnect(
  sqlite3 *db,             // Pointer to the database connection
  void *pAux,              // pClientData passed during module registration
  int argc,                // Argument count
  const char *const*argv,  // Argument vector
  sqlite3_vtab **ppVtab,   // Pointer for the virtual table structure instance
  char **pzErr             // Output pointer for the error message (char [])
){
  (void)pAux; (void)pzErr;
  if (argc < 5) {
    fprintf(stderr, "brinConnect: not enough args (argc=%d)\n", argc);
    return SQLITE_ERROR;
  }

  fprintf(stdout, "-> Connect / xCreate\n");

  BrinVtab *v = (BrinVtab*)sqlite3_malloc(sizeof(BrinVtab));
  if (v == NULL) return SQLITE_NOMEM;
  memset(v,0,sizeof(BrinVtab));

  v->table      = sqlite3_mprintf("%s",argv[3]);
  v->column     = sqlite3_mprintf("%s",argv[4]);
  v->block_size = atoi(argv[5]);
  v->db         = db;

  // Get column affinity
  const char *dataType, *collation;
  int notNull, isPK, isAuto;

  // Get column metadata

  int rc = 1;

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
    sqlite3_close(db);
    return rc;
  }

  // ToDo: We should add a test to validate this
  const char *affinity = get_affinity(dataType);

  printf("Affinity %s\n", affinity);

  if ( strcmp(affinity, "INTEGER") == 0 ) {
    rc = sqlite3_declare_vtab(db,
           "CREATE TABLE x(min INTEGER, max INTEGER, start_rowid INT, end_rowid INT)"
    );
    v->affinity = BRIN_TYPE_INTEGER;
  }
  else if ( strcmp(affinity, "REAL") == 0 ) {
    rc = sqlite3_declare_vtab(db,
           "CREATE TABLE x(min REAL, max REAL, start_rowid INT, end_rowid INT)"
    );
    v->affinity = BRIN_TYPE_REAL;
  }
  else if ( strcmp(affinity, "TEXT") == 0 ) {
    rc = sqlite3_declare_vtab(db,
           "CREATE TABLE x(min TEXT, max TEXT, start_rowid INT, end_rowid INT)"
    );
    v->affinity = BRIN_TYPE_TEXT;
  }
  else {
    fprintf(stderr,
             "NOT SUPPORTED: %s\n", affinity);

    sqlite3_free(v->table);
    sqlite3_free(v->column);
    sqlite3_free(v);

    return SQLITE_ERROR;

  }

  if (rc != SQLITE_OK){
    fprintf(stderr,
             "brinConnect: declare_vtab failed: %s\n",
             sqlite3_errmsg(db));

    sqlite3_free(v->table);
    sqlite3_free(v->column);
    sqlite3_free(v);

    return rc;
  }

  *ppVtab = (sqlite3_vtab*)v;
  return SQLITE_OK;
}

/* --- xBestIndex --- */
static int brinBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pIdx){

  fprintf(stdout, "-> xBestIndex\n");

  (void)pVtab; (void)pIdx;
  /* ToDo */
  return SQLITE_OK;
}


/* --- xDisconnect / xDestroy --- */
static int brinDisconnect(sqlite3_vtab *pVtab){

  fprintf(stdout, "-> xDisconnect / xDestroy\n");

  BrinVtab *v = (BrinVtab*)pVtab;
  if (!v) return SQLITE_OK;
  if (v->table) sqlite3_free(v->table);
  if (v->column) sqlite3_free(v->column);
  sqlite3_free(v);
  return SQLITE_OK;
}

/* --- xOpen --- */
static int brinOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){

  fprintf(stdout, "-> xOpen\n");

  (void)p;
  BrinCursor *cur = sqlite3_malloc(sizeof(BrinCursor));
  memset(cur, 0, sizeof(BrinCursor));
  *ppCursor = (sqlite3_vtab_cursor*)cur;
  return SQLITE_OK;

}

/* --- xClose --- */
static int brinClose(sqlite3_vtab_cursor *cur){

  fprintf(stdout, "-> xClose\n");

  BrinCursor *c = (BrinCursor*)cur;
  if (!c) return SQLITE_OK;
  sqlite3_free(c);
  return SQLITE_OK;
}

/* --- xFilter --- */
static int brinFilter(sqlite3_vtab_cursor *cur, int idxNum, const char *idxStr,
                      int argc, sqlite3_value **argv){

  fprintf(stdout, "-> xFilter\n");

  (void)idxNum; (void)idxStr; (void)argc; (void)argv;
  BrinCursor *c = (BrinCursor*)cur;
  if (!c) return SQLITE_ERROR;
  c->current_block = 0;
  return SQLITE_OK;
}

/* --- xNext --- */
static int brinNext(sqlite3_vtab_cursor *cur){

  fprintf(stdout, "-> xNext\n");

  BrinCursor *c = (BrinCursor*)cur;
  c->current_block++;
  return SQLITE_OK;
}

/* --- xEof --- */
static int brinEof(sqlite3_vtab_cursor *cur){

  fprintf(stdout, "-> xEoF\n");

  BrinCursor *c = (BrinCursor*)cur;
  if (!c) return 1;
  return c->eof;;
}

/* --- xColumn --- */
static int brinColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){

  fprintf(stdout, "-> xColumn\n");
  return SQLITE_OK;

}

/* --- xRowid --- */
static int brinRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){

  fprintf(stdout, "-> xRowid\n");

  BrinCursor *c = (BrinCursor*)cur;
  if (!c) { *pRowid = 0; return SQLITE_OK; }
  *pRowid = (sqlite_int64)(c->current_block + 1);
  return SQLITE_OK;
}


/* --- Module registration structure --- */
static sqlite3_module BrinModule = {
  2,                /* iVersion */
  brinConnect,      /* xCreate */
  brinConnect,      /* xConnect*/
  brinBestIndex,    /* xBestIndex */
  brinDisconnect,   /* xDisconnect */
  brinDisconnect,   /* xDestroy */
  brinOpen,         /* xOpen */
  brinClose,        /* xClose */
  brinFilter,       /* xFilter */
  brinNext,         /* xNext */
  brinEof,          /* xEoF */
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


/* --- Entry point for .load --- */
int sqlite3_brin_init(sqlite3 *db, char **pzErrMsg,
                            const sqlite3_api_routines *pApi) {
  SQLITE_EXTENSION_INIT2(pApi);

  int rc = sqlite3_create_module(db, "brin", &BrinModule, 0);

  if( rc != 0 ) {
    printf("The module could not be created.\n");
  }

  return rc;
}

