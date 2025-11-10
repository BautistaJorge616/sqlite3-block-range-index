#include <sqlite3ext.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>   // Needed for struct tm and time conversion

SQLITE_EXTENSION_INIT1

/* --- Structure representing the BRIN virtual table --- */
typedef struct {
  sqlite3_vtab base;   // Base class - required by SQLite
  char *table;         // Physical table name (the one we index)
  char *column;        // Column name being indexed
  int block_size;      // Number of rows per block
  sqlite3 *db;         // Pointer to the sqlite3 DB handle (stored at xConnect)
} BrinVtab;


/* --- Cursor structure to iterate over BRIN ranges --- */
typedef struct {
  sqlite3_vtab_cursor base;  // Base class - required by SQLite
  int current_block;         // Current block index while scanning
  int total_blocks;          // Total number of blocks
  struct BrinRange {
    double min;
    double max;
    sqlite3_int64 start_rowid;
    sqlite3_int64 end_rowid;
  } *ranges;                 // Dynamic array of block summaries
} BrinCursor;


/* --- Helper: convert sqlite3_value to double (safe) --- */
static double brin_value_to_double(sqlite3_value *val){
  if (!val) return 0.0;
  int type = sqlite3_value_type(val);
  switch (type) {
    case SQLITE_INTEGER: return (double)sqlite3_value_int64(val);
    case SQLITE_FLOAT:   return sqlite3_value_double(val);
    case SQLITE_TEXT: {
      const unsigned char *txt = sqlite3_value_text(val);
      if (!txt) return 0.0;
      int Y=0,M=0,D=0,h=0,m=0;
      double s=0.0;
      if (sscanf((const char*)txt, "%d-%d-%d %d:%d:%lf", &Y,&M,&D,&h,&m,&s) >= 3){
        struct tm t;
        memset(&t,0,sizeof(t));
        t.tm_year = Y - 1900;
        t.tm_mon  = (M>0?M-1:0);
        t.tm_mday = (D>0?D:1);
        t.tm_hour = h;
        t.tm_min  = m;
        t.tm_sec  = (int)s;
        /* Prefer timegm if available (UTC) otherwise mktime (local) */
        #ifdef HAVE_TIMEGM
          return (double)timegm(&t);
        #else
          return (double)mktime(&t);
        #endif
      }
      return 0.0;
    }
    default: return 0.0;
  }
}

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

  fprintf(stdout, "Address of the new index: %p\n", v);
  memset(v,0,sizeof(BrinVtab));

  /* argv layout:
     argv[0] = module name
     argv[1] = db name (main/temp)
     argv[2] = virtual table name
     argv[3..] = arguments passed in CREATE VIRTUAL TABLE
                                       ... USING brin(arg1,arg2,...)
     so we expect argv[3]=table and argv[4]=column
  */

  v->table =      sqlite3_mprintf("%s",argv[3]);
  v->column =     sqlite3_mprintf("%s",argv[4]);
  v->block_size = atoi(argv[5]);
  v->db = db; /* store DB handle for safe use in xOpen */

  if ( v->table == NULL || v->column == NULL ) {
    fprintf(stderr, "brinConnect: allocation failed for names\n");
    if (v->table)      sqlite3_free(v->table);
    if (v->column)     sqlite3_free(v->column);
    sqlite3_free(v);
    return SQLITE_NOMEM;
  }

  /* Expose a simple schema for the virtual table view */
  int rc = sqlite3_declare_vtab(db,
           "CREATE TABLE x(min REAL, max REAL, start_rowid INT, end_rowid INT)"
  );

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

  BrinVtab *v = (BrinVtab*)p;
  if (!v) return SQLITE_ERROR;

  sqlite3 *db = v->db;
  if (!db){
    fprintf(stderr, "brinOpen: stored db handle is NULL\n");
    return SQLITE_ERROR;
  }

  BrinCursor *cur = (BrinCursor*)sqlite3_malloc(sizeof(BrinCursor));
  if (!cur) return SQLITE_NOMEM;
  memset(cur,0,sizeof(BrinCursor));

  /* Build SELECT SQL - careful with sizes */
  char sql[1024];
  int n = snprintf(sql, sizeof(sql), "SELECT rowid, %s FROM %s ORDER BY rowid", v->column, v->table);
  if (n < 0 || n >= (int)sizeof(sql)) {
    fprintf(stderr, "brinOpen: SQL buffer overflow or snprintf error\n");
    sqlite3_free(cur);
    return SQLITE_ERROR;
  }

  sqlite3_stmt *stmt = NULL;
  int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "brinOpen: prepare failed: %s\n", sqlite3_errmsg(db));
    sqlite3_free(cur);
    return rc;
  }

  int capacity = 128;
  cur->ranges = (struct BrinRange*)sqlite3_malloc(capacity * sizeof(*cur->ranges));
  if (!cur->ranges){
    sqlite3_finalize(stmt);
    sqlite3_free(cur);
    return SQLITE_NOMEM;
  }

  double min = 1e308, max = -1e308;
  sqlite3_int64 start = 0, last = 0;
  int count = 0;

  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    sqlite3_int64 rowid = sqlite3_column_int64(stmt,0);
    double val = brin_value_to_double(sqlite3_column_value(stmt,1));

    if (start == 0) start = rowid;
    if (val < min) min = val;
    if (val > max) max = val;

    count++;
    if ((count % v->block_size) == 0) {
      /* ensure capacity */
      if (cur->total_blocks >= capacity) {
        capacity *= 2;
        void *tmp = sqlite3_realloc(cur->ranges, capacity * sizeof(*cur->ranges));
        if (!tmp) {
          fprintf(stderr, "brinOpen: realloc failed\n");
          sqlite3_finalize(stmt);
          sqlite3_free(cur->ranges);
          sqlite3_free(cur);
          return SQLITE_NOMEM;
        }
        cur->ranges = tmp;
      }
      cur->ranges[cur->total_blocks].min = min;
      cur->ranges[cur->total_blocks].max = max;
      cur->ranges[cur->total_blocks].start_rowid = start;
      cur->ranges[cur->total_blocks].end_rowid = rowid;
      cur->total_blocks++;
      /* reset */
      start = 0;
      min = 1e308;
      max = -1e308;
    }
    last = rowid;
  }

  if (rc != SQLITE_DONE) {
    fprintf(stderr, "brinOpen: sqlite3_step error: %s\n", sqlite3_errmsg(db));
    sqlite3_finalize(stmt);
    sqlite3_free(cur->ranges);
    sqlite3_free(cur);
    return rc;
  }

  if (start != 0) {
    if (cur->total_blocks >= capacity) {
      void *tmp = sqlite3_realloc(cur->ranges, (capacity+1) * sizeof(*cur->ranges));
      if (!tmp) {
        sqlite3_finalize(stmt);
        sqlite3_free(cur->ranges);
        sqlite3_free(cur);
        return SQLITE_NOMEM;
      }
      cur->ranges = tmp;
    }
    cur->ranges[cur->total_blocks].min = min;
    cur->ranges[cur->total_blocks].max = max;
    cur->ranges[cur->total_blocks].start_rowid = start;
    cur->ranges[cur->total_blocks].end_rowid = last;
    cur->total_blocks++;
  }

  sqlite3_finalize(stmt);

  /* finished building */
  cur->current_block = 0;
  *ppCursor = (sqlite3_vtab_cursor*)cur;
  return SQLITE_OK;
}

/* --- xClose --- */
static int brinClose(sqlite3_vtab_cursor *cur){

  fprintf(stdout, "-> xClose\n");

  BrinCursor *c = (BrinCursor*)cur;
  if (!c) return SQLITE_OK;
  if (c->ranges) sqlite3_free(c->ranges);
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
  return (c->current_block >= c->total_blocks);
}

/* --- xColumn --- */
static int brinColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){

  fprintf(stdout, "-> xColumn\n");

  BrinCursor *c = (BrinCursor*)cur;
  if (!c || c->total_blocks <= 0 || c->current_block < 0 || c->current_block >= c->total_blocks){
    sqlite3_result_null(ctx);
    return SQLITE_OK;
  }
  struct BrinRange r = c->ranges[c->current_block];
  switch(i){
    case 0: sqlite3_result_double(ctx, r.min); break;
    case 1: sqlite3_result_double(ctx, r.max); break;
    case 2: sqlite3_result_int64(ctx, r.start_rowid); break;
    case 3: sqlite3_result_int64(ctx, r.end_rowid); break;
    default: sqlite3_result_null(ctx); break;
  }
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

