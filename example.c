#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <stdio.h>

/* Estructuras base */
typedef struct {
  sqlite3_vtab base;
} ExampleVtab;

typedef struct {
  sqlite3_vtab_cursor base;
  int rowid;
  int eof;
} ExampleCursor;

/* --- xCreate / xConnect --- */
static int exConnect(
  sqlite3 *db, void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab, char **pzErr
){
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  int rc = sqlite3_declare_vtab(db, "CREATE TABLE x(value TEXT)");
  if (rc != SQLITE_OK) return rc;
  *ppVtab = sqlite3_malloc(sizeof(ExampleVtab));
  memset(*ppVtab, 0, sizeof(ExampleVtab));
  return SQLITE_OK;
}

/* --- xDisconnect / xDestroy --- */
static int exDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/* --- xOpen / xClose --- */
static int exOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  (void)p;
  ExampleCursor *cur = sqlite3_malloc(sizeof(ExampleCursor));
  memset(cur, 0, sizeof(ExampleCursor));
  *ppCursor = (sqlite3_vtab_cursor*)cur;
  return SQLITE_OK;
}

static int exClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}

/* --- xFilter / xNext / xEof --- */
static int exFilter(sqlite3_vtab_cursor *cur, int idxNum, const char *idxStr,
                    int argc, sqlite3_value **argv){
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;
  ExampleCursor *c = (ExampleCursor*)cur;
  c->rowid = 1;
  c->eof = 0;
  return SQLITE_OK;
}

static int exNext(sqlite3_vtab_cursor *cur){
  ExampleCursor *c = (ExampleCursor*)cur;
  if (c->rowid >= 3) c->eof = 1;
  else c->rowid++;
  return SQLITE_OK;
}

static int exEof(sqlite3_vtab_cursor *cur){
  ExampleCursor *c = (ExampleCursor*)cur;
  return c->eof;
}

/* --- xColumn / xRowid --- */
static int exColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  (void)i;
  ExampleCursor *c = (ExampleCursor*)cur;
  char buf[32];
  snprintf(buf, sizeof(buf), "Row %d", c->rowid);
  sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
  return SQLITE_OK;
}

static int exRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  ExampleCursor *c = (ExampleCursor*)cur;
  *pRowid = c->rowid;
  return SQLITE_OK;
}

/* --- xBestIndex --- */
static int exBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  (void)tab;
  (void)pIdxInfo;
  return SQLITE_OK;
}

/* --- Definición del módulo --- */
static sqlite3_module ExampleModule = {
  0,
  exConnect, exConnect, /* xCreate, xConnect */
  exBestIndex,                    /* xBestIndex */
  exDisconnect, exDisconnect,
  exOpen, exClose,
  exFilter, exNext, exEof, exColumn, exRowid,
  0,0,0,0,0,0,0,0,0,0,0
};

/* --- Punto de entrada (.load) --- */
int sqlite3_example_init(sqlite3 *db, char **pzErrMsg,
                         const sqlite3_api_routines *pApi){
  SQLITE_EXTENSION_INIT2(pApi);
  int rc = sqlite3_create_module(db, "example", &ExampleModule, 0);

  if(rc != 0) {
    printf("Failed to load module.\n");
  }

  return rc;
}

