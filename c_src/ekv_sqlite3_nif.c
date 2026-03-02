#include <limits.h>
#include <string.h>
#include <erl_nif.h>
#include "sqlite3.h"

/* ------------------------------------------------------------------ */
/* Resource types                                                      */
/* ------------------------------------------------------------------ */

typedef struct {
    sqlite3      *db;
    ErlNifMutex  *mutex;
} connection_t;

typedef struct {
    sqlite3_stmt *stmt;
    connection_t *conn;   /* prevented from GC via enif_keep_resource */
} statement_t;

static ErlNifResourceType *connection_type = NULL;
static ErlNifResourceType *statement_type  = NULL;

/* Cached atoms */
static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM atom_nil;
static ERL_NIF_TERM atom_row;
static ERL_NIF_TERM atom_done;
static ERL_NIF_TERM atom_true;
static ERL_NIF_TERM atom_false;
static ERL_NIF_TERM atom_promise;
static ERL_NIF_TERM atom_nack;
static ERL_NIF_TERM atom_stale;

static ERL_NIF_TERM make_atom(ErlNifEnv *env, const char *name)
{
    ERL_NIF_TERM atom;
    if (enif_make_existing_atom(env, name, &atom, ERL_NIF_LATIN1))
        return atom;
    return enif_make_atom(env, name);
}

static ERL_NIF_TERM make_error(ErlNifEnv *env, const char *msg)
{
    return enif_make_tuple2(env, atom_error,
        enif_make_string(env, msg, ERL_NIF_LATIN1));
}

static ERL_NIF_TERM make_sqlite_error(ErlNifEnv *env, sqlite3 *db)
{
    const char *msg = sqlite3_errmsg(db);
    size_t len = strlen(msg);
    ERL_NIF_TERM bin;
    unsigned char *buf = enif_make_new_binary(env, len, &bin);
    memcpy(buf, msg, len);
    return enif_make_tuple2(env, atom_error, bin);
}

/* ------------------------------------------------------------------ */
/* Resource destructors                                                */
/* ------------------------------------------------------------------ */

static void connection_dtor(ErlNifEnv *env, void *obj)
{
    (void)env;
    connection_t *conn = (connection_t *)obj;
    if (conn->db) {
        sqlite3_close_v2(conn->db);
        conn->db = NULL;
    }
    if (conn->mutex) {
        enif_mutex_destroy(conn->mutex);
        conn->mutex = NULL;
    }
}

static void statement_dtor(ErlNifEnv *env, void *obj)
{
    (void)env;
    statement_t *s = (statement_t *)obj;
    if (s->stmt) {
        sqlite3_finalize(s->stmt);
        s->stmt = NULL;
    }
    if (s->conn) {
        enif_release_resource(s->conn);
        s->conn = NULL;
    }
}

/* ------------------------------------------------------------------ */
/* NIF: open(path) -> {:ok, db} | {:error, msg}                        */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    ErlNifBinary path_bin;

    if (!enif_inspect_iolist_as_binary(env, argv[0], &path_bin))
        return enif_make_badarg(env);

    /* Null-terminate the path */
    char *path = enif_alloc(path_bin.size + 1);
    if (!path) return make_error(env, "alloc failed");
    memcpy(path, path_bin.data, path_bin.size);
    path[path_bin.size] = '\0';

    sqlite3 *db = NULL;
    int rc = sqlite3_open_v2(path, &db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
        NULL);
    enif_free(path);

    if (rc != SQLITE_OK) {
        const char *msg = db ? sqlite3_errmsg(db) : "out of memory";
        ERL_NIF_TERM err = make_error(env, msg);
        if (db) sqlite3_close_v2(db);
        return err;
    }

    sqlite3_busy_timeout(db, 5000);

    connection_t *conn = enif_alloc_resource(connection_type, sizeof(connection_t));
    conn->db = db;
    conn->mutex = enif_mutex_create("ekv_sqlite3");
    if (!conn->mutex) {
        sqlite3_close_v2(db);
        conn->db = NULL;
        enif_release_resource(conn);
        return make_error(env, "mutex creation failed");
    }

    ERL_NIF_TERM conn_term = enif_make_resource(env, conn);
    enif_release_resource(conn);

    return enif_make_tuple2(env, atom_ok, conn_term);
}

/* ------------------------------------------------------------------ */
/* NIF: close(db) -> :ok | {:error, msg}                               */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    enif_mutex_lock(conn->mutex);
    if (conn->db) {
        int rc = sqlite3_close_v2(conn->db);
        conn->db = NULL;
        enif_mutex_unlock(conn->mutex);
        if (rc != SQLITE_OK)
            return make_error(env, "close failed");
    } else {
        enif_mutex_unlock(conn->mutex);
    }
    return atom_ok;
}

/* ------------------------------------------------------------------ */
/* NIF: execute(db, sql) -> :ok | {:error, msg}                        */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_execute(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    ErlNifBinary sql_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &sql_bin))
        return enif_make_badarg(env);

    char *sql = enif_alloc(sql_bin.size + 1);
    if (!sql) return make_error(env, "alloc failed");
    memcpy(sql, sql_bin.data, sql_bin.size);
    sql[sql_bin.size] = '\0';

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        enif_free(sql);
        return make_error(env, "database closed");
    }

    char *errmsg = NULL;
    int rc = sqlite3_exec(conn->db, sql, NULL, NULL, &errmsg);
    enif_mutex_unlock(conn->mutex);
    enif_free(sql);

    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err;
        if (errmsg) {
            err = make_error(env, errmsg);
            sqlite3_free(errmsg);
        } else {
            err = make_error(env, "execute failed");
        }
        return err;
    }

    return atom_ok;
}

/* ------------------------------------------------------------------ */
/* NIF: prepare(db, sql) -> {:ok, stmt} | {:error, msg}                */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_prepare(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    ErlNifBinary sql_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &sql_bin))
        return enif_make_badarg(env);

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "database closed");
    }

    sqlite3_stmt *raw_stmt = NULL;
    int rc = sqlite3_prepare_v3(conn->db, (const char *)sql_bin.data,
        (int)sql_bin.size, 0, &raw_stmt, NULL);

    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }
    enif_mutex_unlock(conn->mutex);

    statement_t *s = enif_alloc_resource(statement_type, sizeof(statement_t));
    s->stmt = raw_stmt;
    s->conn = conn;
    enif_keep_resource(conn);

    ERL_NIF_TERM stmt_term = enif_make_resource(env, s);
    enif_release_resource(s);

    return enif_make_tuple2(env, atom_ok, stmt_term);
}

/* ------------------------------------------------------------------ */
/* Shared bind helper (no mutex — caller must hold it)                 */
/* ------------------------------------------------------------------ */

/* Returns 0 on success, -1 on badarg, positive sqlite error code */
static int bind_args(ErlNifEnv *env, sqlite3_stmt *stmt, ERL_NIF_TERM list)
{
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);

    if (enif_is_empty_list(env, list))
        return 0;

    ERL_NIF_TERM head;
    int idx = 1;

    while (enif_get_list_cell(env, list, &head, &list)) {
        int rc;

        /* nil atom -> NULL */
        if (enif_is_atom(env, head)) {
            char atom_buf[16];
            if (enif_get_atom(env, head, atom_buf, sizeof(atom_buf), ERL_NIF_LATIN1)
                && strcmp(atom_buf, "nil") == 0) {
                rc = sqlite3_bind_null(stmt, idx);
            } else {
                return -1;
            }
        }
        /* integer */
        else if (enif_is_number(env, head)) {
            ErlNifSInt64 ival;
            double dval;
            if (enif_get_int64(env, head, &ival)) {
                rc = sqlite3_bind_int64(stmt, idx, ival);
            } else if (enif_get_double(env, head, &dval)) {
                rc = sqlite3_bind_double(stmt, idx, dval);
            } else {
                return -1;
            }
        }
        /* binary/string */
        else if (enif_is_binary(env, head)) {
            ErlNifBinary bin;
            enif_inspect_binary(env, head, &bin);
            if (bin.size > INT_MAX)
                return -1;
            rc = sqlite3_bind_text(stmt, idx,
                (const char *)bin.data, (int)bin.size, SQLITE_TRANSIENT);
        }
        else {
            return -1;
        }

        if (rc != SQLITE_OK)
            return rc;
        idx++;
    }

    return 0;
}

/* ------------------------------------------------------------------ */
/* NIF: bind(stmt, args) -> :ok | {:error, msg}                        */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_bind(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    statement_t *s;
    if (!enif_get_resource(env, argv[0], statement_type, (void **)&s))
        return enif_make_badarg(env);

    if (!s->conn)
        return make_error(env, "statement released");

    enif_mutex_lock(s->conn->mutex);
    if (!s->stmt) {
        enif_mutex_unlock(s->conn->mutex);
        return make_error(env, "statement finalized");
    }
    int result = bind_args(env, s->stmt, argv[1]);
    if (result == -1) {
        enif_mutex_unlock(s->conn->mutex);
        return enif_make_badarg(env);
    }
    if (result > 0) {
        ERL_NIF_TERM err = make_sqlite_error(env, s->conn->db);
        enif_mutex_unlock(s->conn->mutex);
        return err;
    }
    enif_mutex_unlock(s->conn->mutex);
    return atom_ok;
}

/* ------------------------------------------------------------------ */
/* Column extraction helper                                            */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM make_column(ErlNifEnv *env, sqlite3_stmt *stmt, int col)
{
    switch (sqlite3_column_type(stmt, col)) {
    case SQLITE_INTEGER:
        return enif_make_int64(env, sqlite3_column_int64(stmt, col));

    case SQLITE_FLOAT:
        return enif_make_double(env, sqlite3_column_double(stmt, col));

    case SQLITE_TEXT: {
        int len = sqlite3_column_bytes(stmt, col);
        const unsigned char *text = sqlite3_column_text(stmt, col);
        ERL_NIF_TERM bin;
        unsigned char *buf = enif_make_new_binary(env, (size_t)len, &bin);
        if (len > 0) memcpy(buf, text, (size_t)len);
        return bin;
    }

    case SQLITE_BLOB: {
        int len = sqlite3_column_bytes(stmt, col);
        const void *blob = sqlite3_column_blob(stmt, col);
        ERL_NIF_TERM bin;
        unsigned char *buf = enif_make_new_binary(env, (size_t)len, &bin);
        if (len > 0) memcpy(buf, blob, (size_t)len);
        return bin;
    }

    case SQLITE_NULL:
    default:
        return atom_nil;
    }
}

/* ------------------------------------------------------------------ */
/* NIF: step(db, stmt) -> {:row, list} | :done | {:error, msg}         */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_step(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    statement_t *s;
    if (!enif_get_resource(env, argv[1], statement_type, (void **)&s))
        return enif_make_badarg(env);

    if (s->conn != conn)
        return make_error(env, "statement does not belong to this connection");

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "database closed");
    }
    if (!s->stmt) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "statement finalized");
    }

    int rc = sqlite3_step(s->stmt);

    if (rc == SQLITE_ROW) {
        int ncols = sqlite3_column_count(s->stmt);
        if (ncols == 0) {
            enif_mutex_unlock(conn->mutex);
            return enif_make_tuple2(env, atom_row, enif_make_list(env, 0));
        }
        ERL_NIF_TERM *cols = enif_alloc(sizeof(ERL_NIF_TERM) * (size_t)ncols);
        if (!cols) {
            sqlite3_reset(s->stmt);
            enif_mutex_unlock(conn->mutex);
            return make_error(env, "alloc failed");
        }
        for (int i = 0; i < ncols; i++)
            cols[i] = make_column(env, s->stmt, i);
        ERL_NIF_TERM row_list = enif_make_list_from_array(env, cols, (unsigned)ncols);
        enif_free(cols);
        enif_mutex_unlock(conn->mutex);
        return enif_make_tuple2(env, atom_row, row_list);
    }

    if (rc == SQLITE_DONE) {
        sqlite3_reset(s->stmt);
        enif_mutex_unlock(conn->mutex);
        return atom_done;
    }

    /* Error */
    ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
    sqlite3_reset(s->stmt);
    enif_mutex_unlock(conn->mutex);
    return err;
}

/* ------------------------------------------------------------------ */
/* NIF: release(db, stmt) -> :ok                                       */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_release(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    statement_t *s;
    if (!enif_get_resource(env, argv[1], statement_type, (void **)&s))
        return enif_make_badarg(env);

    if (s->conn != conn)
        return make_error(env, "statement does not belong to this connection");

    enif_mutex_lock(conn->mutex);
    if (s->stmt) {
        sqlite3_finalize(s->stmt);
        s->stmt = NULL;
    }
    connection_t *to_release = s->conn;
    s->conn = NULL;
    enif_mutex_unlock(conn->mutex);

    if (to_release) {
        enif_release_resource(to_release);
    }

    return atom_ok;
}

/* ------------------------------------------------------------------ */
/* NIF: write_entry(db, kv_stmt, oplog_stmt, kv_args, oplog_args)      */
/*   -> {:ok, true|false} | {:error, msg}                              */
/*                                                                     */
/* Single dirty IO bounce: BEGIN IMMEDIATE, bind+step kv upsert,       */
/* check sqlite3_changes() for LWW result. If 0 (LWW lost), ROLLBACK  */
/* and return {:ok, false}. Otherwise bind+step oplog, COMMIT, return  */
/* {:ok, true}.                                                        */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_write_entry(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    statement_t *kv_s;
    if (!enif_get_resource(env, argv[1], statement_type, (void **)&kv_s))
        return enif_make_badarg(env);

    statement_t *oplog_s;
    if (!enif_get_resource(env, argv[2], statement_type, (void **)&oplog_s))
        return enif_make_badarg(env);

    if (kv_s->conn != conn || oplog_s->conn != conn)
        return make_error(env, "statement does not belong to this connection");

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "database closed");
    }
    if (!kv_s->stmt || !oplog_s->stmt) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "statement finalized");
    }

    /* 1. Bind kv args */
    int br = bind_args(env, kv_s->stmt, argv[3]);
    if (br != 0) {
        enif_mutex_unlock(conn->mutex);
        return (br == -1) ? enif_make_badarg(env)
                          : make_sqlite_error(env, conn->db);
    }

    /* 2. BEGIN IMMEDIATE */
    int rc = sqlite3_exec(conn->db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_reset(kv_s->stmt);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    /* 3. Step kv upsert */
    rc = sqlite3_step(kv_s->stmt);
    if (rc != SQLITE_DONE) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_reset(kv_s->stmt);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }
    sqlite3_reset(kv_s->stmt);

    /* 4. Check LWW result */
    int changes = sqlite3_changes(conn->db);
    if (changes == 0) {
        /* LWW lost — rollback */
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return enif_make_tuple2(env, atom_ok, atom_false);
    }

    /* 5. Bind + step oplog */
    br = bind_args(env, oplog_s->stmt, argv[4]);
    if (br != 0) {
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return (br == -1) ? enif_make_badarg(env)
                          : make_sqlite_error(env, conn->db);
    }

    rc = sqlite3_step(oplog_s->stmt);
    if (rc != SQLITE_DONE) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_reset(oplog_s->stmt);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }
    sqlite3_reset(oplog_s->stmt);

    /* 6. COMMIT */
    rc = sqlite3_exec(conn->db, "COMMIT", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    enif_mutex_unlock(conn->mutex);
    return enif_make_tuple2(env, atom_ok, atom_true);
}

/* ------------------------------------------------------------------ */
/* NIF: read_entry(db, stmt, args) -> {:ok, [cols]} | {:ok, nil} | err */
/*                                                                     */
/* Single dirty IO bounce: reset+bind, step, extract row or nil.       */
/* The statement is NOT finalized — it's cached for reuse.             */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_read_entry(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    statement_t *s;
    if (!enif_get_resource(env, argv[1], statement_type, (void **)&s))
        return enif_make_badarg(env);

    if (s->conn != conn)
        return make_error(env, "statement does not belong to this connection");

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "database closed");
    }
    if (!s->stmt) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "statement finalized");
    }

    /* 1. Reset + bind */
    int br = bind_args(env, s->stmt, argv[2]);
    if (br != 0) {
        sqlite3_reset(s->stmt);
        enif_mutex_unlock(conn->mutex);
        return (br == -1) ? enif_make_badarg(env)
                          : make_sqlite_error(env, conn->db);
    }

    /* 2. Step */
    int rc = sqlite3_step(s->stmt);

    if (rc == SQLITE_ROW) {
        int ncols = sqlite3_column_count(s->stmt);
        if (ncols == 0) {
            sqlite3_reset(s->stmt);
            enif_mutex_unlock(conn->mutex);
            return enif_make_tuple2(env, atom_ok, enif_make_list(env, 0));
        }
        ERL_NIF_TERM *cols = enif_alloc(sizeof(ERL_NIF_TERM) * (size_t)ncols);
        if (!cols) {
            sqlite3_reset(s->stmt);
            enif_mutex_unlock(conn->mutex);
            return make_error(env, "alloc failed");
        }
        for (int i = 0; i < ncols; i++)
            cols[i] = make_column(env, s->stmt, i);
        ERL_NIF_TERM row_list = enif_make_list_from_array(env, cols, (unsigned)ncols);
        enif_free(cols);
        sqlite3_reset(s->stmt);
        enif_mutex_unlock(conn->mutex);
        return enif_make_tuple2(env, atom_ok, row_list);
    }

    if (rc == SQLITE_DONE) {
        sqlite3_reset(s->stmt);
        enif_mutex_unlock(conn->mutex);
        return enif_make_tuple2(env, atom_ok, atom_nil);
    }

    /* Error */
    ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
    sqlite3_reset(s->stmt);
    enif_mutex_unlock(conn->mutex);
    return err;
}

/* ------------------------------------------------------------------ */
/* NIF: fetch_all(db, sql, args) -> {:ok, rows} | {:error, msg}        */
/*                                                                     */
/* Single dirty IO bounce: prepare, bind, step all rows, finalize.     */
/* Returns {:ok, [[col1, col2, ...], ...]}.                            */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_fetch_all(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    ErlNifBinary sql_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &sql_bin))
        return enif_make_badarg(env);

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "database closed");
    }

    /* 1. Prepare */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v3(conn->db, (const char *)sql_bin.data,
        (int)sql_bin.size, 0, &stmt, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    /* 2. Bind */
    int br = bind_args(env, stmt, argv[2]);
    if (br != 0) {
        sqlite3_finalize(stmt);
        enif_mutex_unlock(conn->mutex);
        return (br == -1) ? enif_make_badarg(env)
                          : make_sqlite_error(env, conn->db);
    }

    /* 3. Step all rows, collecting into a dynamic array */
    size_t cap = 64;
    size_t len = 0;
    ERL_NIF_TERM *rows = enif_alloc(sizeof(ERL_NIF_TERM) * cap);
    if (!rows) {
        sqlite3_finalize(stmt);
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "alloc failed");
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        int ncols = sqlite3_column_count(stmt);
        ERL_NIF_TERM row;
        if (ncols == 0) {
            row = enif_make_list(env, 0);
        } else {
            ERL_NIF_TERM *cols = enif_alloc(sizeof(ERL_NIF_TERM) * (size_t)ncols);
            if (!cols) {
                sqlite3_finalize(stmt);
                enif_free(rows);
                enif_mutex_unlock(conn->mutex);
                return make_error(env, "alloc failed");
            }
            for (int i = 0; i < ncols; i++)
                cols[i] = make_column(env, stmt, i);
            row = enif_make_list_from_array(env, cols, (unsigned)ncols);
            enif_free(cols);
        }

        if (len == cap) {
            size_t new_cap = cap * 2;
            ERL_NIF_TERM *new_rows = enif_realloc(rows, sizeof(ERL_NIF_TERM) * new_cap);
            if (!new_rows) {
                sqlite3_finalize(stmt);
                enif_free(rows);
                enif_mutex_unlock(conn->mutex);
                return make_error(env, "alloc failed");
            }
            rows = new_rows;
            cap = new_cap;
        }
        rows[len++] = row;
    }

    if (rc != SQLITE_DONE) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_finalize(stmt);
        enif_free(rows);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    /* 4. Finalize */
    sqlite3_finalize(stmt);
    enif_mutex_unlock(conn->mutex);

    /* 5. Build result list — rows are already in order */
    ERL_NIF_TERM result = enif_make_list_from_array(env, rows, (unsigned)len);
    enif_free(rows);

    return enif_make_tuple2(env, atom_ok, result);
}

/* ------------------------------------------------------------------ */
/* NIF: ekv_backup(source_path, dest_path) -> :ok | {:error, msg}      */
/*                                                                     */
/* Standalone backup using SQLite backup API. Opens and closes its own */
/* connections. Source opened READONLY — safe alongside WAL writers.    */
/* backup_step(-1) copies all pages in one shot.                       */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_backup(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    ErlNifBinary src_bin, dst_bin;

    if (!enif_inspect_iolist_as_binary(env, argv[0], &src_bin))
        return enif_make_badarg(env);
    if (!enif_inspect_iolist_as_binary(env, argv[1], &dst_bin))
        return enif_make_badarg(env);

    /* Null-terminate paths */
    char *src_path = enif_alloc(src_bin.size + 1);
    if (!src_path) return make_error(env, "alloc failed");
    memcpy(src_path, src_bin.data, src_bin.size);
    src_path[src_bin.size] = '\0';

    char *dst_path = enif_alloc(dst_bin.size + 1);
    if (!dst_path) {
        enif_free(src_path);
        return make_error(env, "alloc failed");
    }
    memcpy(dst_path, dst_bin.data, dst_bin.size);
    dst_path[dst_bin.size] = '\0';

    /* Open source READONLY */
    sqlite3 *src_db = NULL;
    int rc = sqlite3_open_v2(src_path, &src_db, SQLITE_OPEN_READONLY, NULL);
    enif_free(src_path);
    if (rc != SQLITE_OK) {
        const char *msg = src_db ? sqlite3_errmsg(src_db) : "out of memory";
        ERL_NIF_TERM err = make_error(env, msg);
        if (src_db) sqlite3_close_v2(src_db);
        enif_free(dst_path);
        return err;
    }

    /* Open dest READWRITE|CREATE */
    sqlite3 *dst_db = NULL;
    rc = sqlite3_open_v2(dst_path, &dst_db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    enif_free(dst_path);
    if (rc != SQLITE_OK) {
        const char *msg = dst_db ? sqlite3_errmsg(dst_db) : "out of memory";
        ERL_NIF_TERM err = make_error(env, msg);
        if (dst_db) sqlite3_close_v2(dst_db);
        sqlite3_close_v2(src_db);
        return err;
    }

    /* Run backup */
    sqlite3_backup *backup = sqlite3_backup_init(dst_db, "main", src_db, "main");
    if (!backup) {
        ERL_NIF_TERM err = make_error(env, sqlite3_errmsg(dst_db));
        sqlite3_close_v2(dst_db);
        sqlite3_close_v2(src_db);
        return err;
    }

    rc = sqlite3_backup_step(backup, -1);
    sqlite3_backup_finish(backup);

    sqlite3_close_v2(dst_db);
    sqlite3_close_v2(src_db);

    if (rc != SQLITE_DONE) {
        return make_error(env, "backup failed");
    }

    return atom_ok;
}

/* ------------------------------------------------------------------ */
/* NIF: paxos_prepare(db, key, ballot_counter, ballot_node)            */
/*   -> {:ok, :promise, acc_c, acc_n, kv_row | nil}                    */
/*   -> {:ok, :nack, promised_c, promised_n}                           */
/*   -> {:error, msg}                                                  */
/*                                                                     */
/* Single dirty IO bounce. Atomic CASPaxos prepare-phase acceptor op.  */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_paxos_prepare(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    ErlNifBinary key_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &key_bin))
        return enif_make_badarg(env);

    ErlNifSInt64 ballot_c;
    if (!enif_get_int64(env, argv[2], &ballot_c))
        return enif_make_badarg(env);

    ErlNifBinary ballot_n_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[3], &ballot_n_bin))
        return enif_make_badarg(env);

    /* Null-terminate ballot_n for strcmp */
    char *ballot_n_str = enif_alloc(ballot_n_bin.size + 1);
    if (!ballot_n_str) return make_error(env, "alloc failed");
    memcpy(ballot_n_str, ballot_n_bin.data, ballot_n_bin.size);
    ballot_n_str[ballot_n_bin.size] = '\0';
    int ballot_n_len = (int)ballot_n_bin.size;

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return make_error(env, "database closed");
    }

    /* BEGIN IMMEDIATE */
    int rc = sqlite3_exec(conn->db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return err;
    }

    /* SELECT from kv_paxos (including value columns for accepted state) */
    sqlite3_stmt *sel = NULL;
    rc = sqlite3_prepare_v3(conn->db,
        "SELECT promised_counter, promised_node, accepted_counter, accepted_node, "
        "accepted_value, accepted_timestamp, accepted_origin, "
        "accepted_expires_at, accepted_deleted_at "
        "FROM kv_paxos WHERE key = ?1", -1, 0, &sel, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return err;
    }
    sqlite3_bind_text(sel, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);

    rc = sqlite3_step(sel);

    ErlNifSInt64 promised_c = 0, accepted_c = 0;
    const char *promised_n_str = "";
    int promised_n_len = 0;
    const char *accepted_n_str = "";
    int accepted_n_len = 0;
    int has_row = 0;
    int has_accepted_value = 0;
    ERL_NIF_TERM paxos_value_cols[5]; /* value, ts, origin, expires, deleted */

    if (rc == SQLITE_ROW) {
        has_row = 1;
        promised_c = sqlite3_column_int64(sel, 0);
        if (sqlite3_column_type(sel, 1) != SQLITE_NULL) {
            promised_n_str = (const char *)sqlite3_column_text(sel, 1);
            promised_n_len = sqlite3_column_bytes(sel, 1);
        }
        accepted_c = sqlite3_column_int64(sel, 2);
        if (sqlite3_column_type(sel, 3) != SQLITE_NULL) {
            accepted_n_str = (const char *)sqlite3_column_text(sel, 3);
            accepted_n_len = sqlite3_column_bytes(sel, 3);
        }

        /* Check if there's a pending accepted value (acc_c > 0 AND value NOT NULL) */
        if (accepted_c > 0 && sqlite3_column_type(sel, 4) != SQLITE_NULL) {
            has_accepted_value = 1;
            for (int i = 0; i < 5; i++)
                paxos_value_cols[i] = make_column(env, sel, 4 + i);
        }
    }

    /* Copy accepted_n before finalizing (data owned by stmt) */
    char *accepted_n_copy = enif_alloc(accepted_n_len + 1);
    memcpy(accepted_n_copy, accepted_n_str, accepted_n_len);
    accepted_n_copy[accepted_n_len] = '\0';

    /* Check ballot > promised (strictly greater) */
    int ballot_wins;
    if (ballot_c != promised_c) {
        ballot_wins = ballot_c > promised_c;
    } else {
        ballot_wins = strcmp(ballot_n_str, promised_n_str) > 0;
    }

    if (!ballot_wins) {
        /* NACK — copy promised_n before finalizing */
        int prom_n_len_copy = promised_n_len;
        char *prom_n_copy = enif_alloc(prom_n_len_copy + 1);
        memcpy(prom_n_copy, promised_n_str, prom_n_len_copy);
        prom_n_copy[prom_n_len_copy] = '\0';

        sqlite3_finalize(sel);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);

        ERL_NIF_TERM promised_n_term;
        {
            ErlNifBinary bin;
            enif_alloc_binary(prom_n_len_copy, &bin);
            if (prom_n_len_copy > 0) memcpy(bin.data, prom_n_copy, prom_n_len_copy);
            promised_n_term = enif_make_binary(env, &bin);
        }

        enif_free(prom_n_copy);
        enif_free(ballot_n_str);
        enif_free(accepted_n_copy);
        return enif_make_tuple4(env, atom_ok, atom_nack,
            enif_make_int64(env, promised_c),
            promised_n_term);
    }

    sqlite3_finalize(sel);

    /* Update or insert promise */
    sqlite3_stmt *ups = NULL;
    if (has_row) {
        rc = sqlite3_prepare_v3(conn->db,
            "UPDATE kv_paxos SET promised_counter = ?2, promised_node = ?3 WHERE key = ?1",
            -1, 0, &ups, NULL);
    } else {
        rc = sqlite3_prepare_v3(conn->db,
            "INSERT INTO kv_paxos (key, promised_counter, promised_node, accepted_counter, accepted_node) "
            "VALUES (?1, ?2, ?3, 0, '')",
            -1, 0, &ups, NULL);
    }
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        enif_free(accepted_n_copy);
        return err;
    }
    sqlite3_bind_text(ups, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);
    sqlite3_bind_int64(ups, 2, ballot_c);
    sqlite3_bind_text(ups, 3, ballot_n_str, ballot_n_len, SQLITE_TRANSIENT);
    rc = sqlite3_step(ups);
    sqlite3_finalize(ups);
    if (rc != SQLITE_DONE) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        enif_free(accepted_n_copy);
        return err;
    }

    /* Get current value: prefer kv_paxos accepted value, fall back to kv */
    ERL_NIF_TERM kv_row;
    if (has_accepted_value) {
        /* Use accepted value from kv_paxos (pending accept, not yet committed) */
        kv_row = enif_make_list_from_array(env, paxos_value_cols, 5);
    } else {
        /* Fall back to kv table (committed state) */
        sqlite3_stmt *kv_sel = NULL;
        rc = sqlite3_prepare_v3(conn->db,
            "SELECT value, timestamp, origin_node, expires_at, deleted_at FROM kv WHERE key = ?1",
            -1, 0, &kv_sel, NULL);
        if (rc != SQLITE_OK) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
            enif_mutex_unlock(conn->mutex);
            enif_free(ballot_n_str);
            enif_free(accepted_n_copy);
            return err;
        }
        sqlite3_bind_text(kv_sel, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);

        rc = sqlite3_step(kv_sel);
        if (rc == SQLITE_ROW) {
            int ncols = sqlite3_column_count(kv_sel);
            ERL_NIF_TERM cols[5];
            for (int i = 0; i < ncols && i < 5; i++)
                cols[i] = make_column(env, kv_sel, i);
            kv_row = enif_make_list_from_array(env, cols, (unsigned)ncols);
        } else {
            kv_row = atom_nil;
        }
        sqlite3_finalize(kv_sel);
    }

    /* COMMIT */
    rc = sqlite3_exec(conn->db, "COMMIT", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        enif_free(accepted_n_copy);
        return err;
    }

    enif_mutex_unlock(conn->mutex);

    /* Build accepted_n return term */
    ERL_NIF_TERM accepted_n_term;
    {
        ErlNifBinary bin;
        enif_alloc_binary(accepted_n_len, &bin);
        if (accepted_n_len > 0) memcpy(bin.data, accepted_n_copy, accepted_n_len);
        accepted_n_term = enif_make_binary(env, &bin);
    }

    enif_free(ballot_n_str);
    enif_free(accepted_n_copy);

    /* {:ok, :promise, accepted_c, accepted_n, kv_row_or_nil} */
    return enif_make_tuple(env, 5, atom_ok, atom_promise,
        enif_make_int64(env, accepted_c),
        accepted_n_term,
        kv_row);
}

/* ------------------------------------------------------------------ */
/* NIF: paxos_accept(db, key, ballot_c, ballot_n, value_args)          */
/*   -> {:ok, true}   (accepted — written to kv_paxos only)            */
/*   -> {:ok, false}  (rejected — ballot < promised)                   */
/*   -> {:error, msg}                                                  */
/*                                                                     */
/* Single dirty IO bounce. Writes to kv_paxos only (not kv/oplog).     */
/* value_args = [value_binary, timestamp, origin_str, expires_at,      */
/*               deleted_at]                                           */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_paxos_accept(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    ErlNifBinary key_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &key_bin))
        return enif_make_badarg(env);

    ErlNifSInt64 ballot_c;
    if (!enif_get_int64(env, argv[2], &ballot_c))
        return enif_make_badarg(env);

    ErlNifBinary ballot_n_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[3], &ballot_n_bin))
        return enif_make_badarg(env);

    /* Null-terminate ballot_n for strcmp */
    char *ballot_n_str = enif_alloc(ballot_n_bin.size + 1);
    if (!ballot_n_str) return make_error(env, "alloc failed");
    memcpy(ballot_n_str, ballot_n_bin.data, ballot_n_bin.size);
    ballot_n_str[ballot_n_bin.size] = '\0';
    int ballot_n_len = (int)ballot_n_bin.size;

    /* Parse value_args list: [value_binary, timestamp, origin_str, expires_at, deleted_at] */
    ERL_NIF_TERM val_list = argv[4];
    ERL_NIF_TERM val_elems[5];
    ERL_NIF_TERM head;
    for (int i = 0; i < 5; i++) {
        if (!enif_get_list_cell(env, val_list, &head, &val_list)) {
            enif_free(ballot_n_str);
            return enif_make_badarg(env);
        }
        val_elems[i] = head;
    }

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return make_error(env, "database closed");
    }

    /* 1. BEGIN IMMEDIATE */
    int rc = sqlite3_exec(conn->db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return err;
    }

    /* 2. Check promised ballot */
    sqlite3_stmt *sel = NULL;
    rc = sqlite3_prepare_v3(conn->db,
        "SELECT promised_counter, promised_node FROM kv_paxos WHERE key = ?1",
        -1, 0, &sel, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return err;
    }
    sqlite3_bind_text(sel, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);

    rc = sqlite3_step(sel);
    ErlNifSInt64 promised_c = 0;
    const char *promised_n_str = "";

    if (rc == SQLITE_ROW) {
        promised_c = sqlite3_column_int64(sel, 0);
        if (sqlite3_column_type(sel, 1) != SQLITE_NULL) {
            promised_n_str = (const char *)sqlite3_column_text(sel, 1);
        }
    }

    /* ballot >= promised? */
    int ballot_ok;
    if (ballot_c != promised_c) {
        ballot_ok = ballot_c > promised_c;
    } else {
        ballot_ok = strcmp(ballot_n_str, promised_n_str) >= 0;
    }

    sqlite3_finalize(sel);

    if (!ballot_ok) {
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return enif_make_tuple2(env, atom_ok, atom_false);
    }

    /* 3. UPSERT kv_paxos with ballot + value columns */
    sqlite3_stmt *pax_ups = NULL;
    rc = sqlite3_prepare_v3(conn->db,
        "INSERT INTO kv_paxos (key, promised_counter, promised_node, "
        "accepted_counter, accepted_node, "
        "accepted_value, accepted_timestamp, accepted_origin, "
        "accepted_expires_at, accepted_deleted_at) "
        "VALUES (?1, ?2, ?3, ?2, ?3, ?4, ?5, ?6, ?7, ?8) "
        "ON CONFLICT(key) DO UPDATE SET "
        "promised_counter=excluded.promised_counter, promised_node=excluded.promised_node, "
        "accepted_counter=excluded.accepted_counter, accepted_node=excluded.accepted_node, "
        "accepted_value=excluded.accepted_value, accepted_timestamp=excluded.accepted_timestamp, "
        "accepted_origin=excluded.accepted_origin, accepted_expires_at=excluded.accepted_expires_at, "
        "accepted_deleted_at=excluded.accepted_deleted_at",
        -1, 0, &pax_ups, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return err;
    }
    sqlite3_bind_text(pax_ups, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);
    sqlite3_bind_int64(pax_ups, 2, ballot_c);
    sqlite3_bind_text(pax_ups, 3, ballot_n_str, ballot_n_len, SQLITE_TRANSIENT);

    /* Bind value_args to positions ?4-?8 */
    for (int i = 0; i < 5; i++) {
        int pos = 4 + i;
        if (enif_is_atom(env, val_elems[i])) {
            char atom_buf[16];
            if (enif_get_atom(env, val_elems[i], atom_buf, sizeof(atom_buf), ERL_NIF_LATIN1)
                && strcmp(atom_buf, "nil") == 0) {
                sqlite3_bind_null(pax_ups, pos);
            } else {
                sqlite3_finalize(pax_ups);
                sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
                enif_mutex_unlock(conn->mutex);
                enif_free(ballot_n_str);
                return enif_make_badarg(env);
            }
        } else if (enif_is_number(env, val_elems[i])) {
            ErlNifSInt64 ival;
            double dval;
            if (enif_get_int64(env, val_elems[i], &ival)) {
                sqlite3_bind_int64(pax_ups, pos, ival);
            } else if (enif_get_double(env, val_elems[i], &dval)) {
                sqlite3_bind_double(pax_ups, pos, dval);
            } else {
                sqlite3_finalize(pax_ups);
                sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
                enif_mutex_unlock(conn->mutex);
                enif_free(ballot_n_str);
                return enif_make_badarg(env);
            }
        } else if (enif_is_binary(env, val_elems[i])) {
            ErlNifBinary bin;
            enif_inspect_binary(env, val_elems[i], &bin);
            if (i == 0) {
                /* value_binary → bind as BLOB */
                sqlite3_bind_blob(pax_ups, pos, bin.data, (int)bin.size, SQLITE_TRANSIENT);
            } else {
                /* origin_str → bind as TEXT */
                sqlite3_bind_text(pax_ups, pos, (const char *)bin.data, (int)bin.size, SQLITE_TRANSIENT);
            }
        } else {
            sqlite3_finalize(pax_ups);
            sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
            enif_mutex_unlock(conn->mutex);
            enif_free(ballot_n_str);
            return enif_make_badarg(env);
        }
    }

    rc = sqlite3_step(pax_ups);
    sqlite3_finalize(pax_ups);
    if (rc != SQLITE_DONE) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return err;
    }

    /* 4. COMMIT */
    rc = sqlite3_exec(conn->db, "COMMIT", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return err;
    }

    enif_mutex_unlock(conn->mutex);
    enif_free(ballot_n_str);
    return enif_make_tuple2(env, atom_ok, atom_true);
}

/* ------------------------------------------------------------------ */
/* NIF: paxos_promote(db, kv_force_stmt, oplog_stmt, key,             */
/*                    ballot_c, ballot_n)                              */
/*   -> {:ok, value, ts, origin, expires, deleted, prev_value|nil}     */
/*   -> {:ok, :stale}                                                  */
/*   -> {:error, msg}                                                  */
/*                                                                     */
/* Single dirty IO bounce. Promotes accepted value from kv_paxos to    */
/* kv + oplog on commit confirmation. Clears kv_paxos value columns.  */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_paxos_promote(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    statement_t *kv_s;
    if (!enif_get_resource(env, argv[1], statement_type, (void **)&kv_s))
        return enif_make_badarg(env);

    statement_t *oplog_s;
    if (!enif_get_resource(env, argv[2], statement_type, (void **)&oplog_s))
        return enif_make_badarg(env);

    if (kv_s->conn != conn || oplog_s->conn != conn)
        return make_error(env, "statement does not belong to this connection");

    ErlNifBinary key_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[3], &key_bin))
        return enif_make_badarg(env);

    ErlNifSInt64 ballot_c;
    if (!enif_get_int64(env, argv[4], &ballot_c))
        return enif_make_badarg(env);

    ErlNifBinary ballot_n_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[5], &ballot_n_bin))
        return enif_make_badarg(env);

    /* Null-terminate ballot_n for strcmp */
    char *ballot_n_str = enif_alloc(ballot_n_bin.size + 1);
    if (!ballot_n_str) return make_error(env, "alloc failed");
    memcpy(ballot_n_str, ballot_n_bin.data, ballot_n_bin.size);
    ballot_n_str[ballot_n_bin.size] = '\0';

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return make_error(env, "database closed");
    }
    if (!kv_s->stmt || !oplog_s->stmt) {
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return make_error(env, "statement finalized");
    }

    /* 1. BEGIN IMMEDIATE */
    int rc = sqlite3_exec(conn->db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return err;
    }

    /* 2. SELECT accepted state from kv_paxos */
    sqlite3_stmt *sel = NULL;
    rc = sqlite3_prepare_v3(conn->db,
        "SELECT accepted_counter, accepted_node, "
        "accepted_value, accepted_timestamp, accepted_origin, "
        "accepted_expires_at, accepted_deleted_at "
        "FROM kv_paxos WHERE key = ?1",
        -1, 0, &sel, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return err;
    }
    sqlite3_bind_text(sel, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);

    rc = sqlite3_step(sel);

    /* 3. Check if ballot matches */
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(sel);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return enif_make_tuple2(env, atom_ok, atom_stale);
    }

    ErlNifSInt64 acc_c = sqlite3_column_int64(sel, 0);
    const char *acc_n_str = "";
    if (sqlite3_column_type(sel, 1) != SQLITE_NULL) {
        acc_n_str = (const char *)sqlite3_column_text(sel, 1);
    }

    if (acc_c != ballot_c || strcmp(acc_n_str, ballot_n_str) != 0) {
        sqlite3_finalize(sel);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return enif_make_tuple2(env, atom_ok, atom_stale);
    }

    /* Extract value columns from kv_paxos */
    ERL_NIF_TERM value_term = make_column(env, sel, 2);     /* accepted_value (BLOB) */
    ERL_NIF_TERM ts_term = make_column(env, sel, 3);        /* accepted_timestamp */
    ERL_NIF_TERM origin_term = make_column(env, sel, 4);    /* accepted_origin */
    ERL_NIF_TERM expires_term = make_column(env, sel, 5);   /* accepted_expires_at */
    ERL_NIF_TERM deleted_term = make_column(env, sel, 6);   /* accepted_deleted_at */

    /* We need the raw values for binding to kv/oplog statements */
    /* Re-read the raw column data for bind operations */
    int has_value = sqlite3_column_type(sel, 2) != SQLITE_NULL;
    int value_len = has_value ? sqlite3_column_bytes(sel, 2) : 0;
    const void *value_data = has_value ? sqlite3_column_blob(sel, 2) : NULL;

    ErlNifSInt64 timestamp = 0;
    if (sqlite3_column_type(sel, 3) != SQLITE_NULL)
        timestamp = sqlite3_column_int64(sel, 3);

    int has_origin = sqlite3_column_type(sel, 4) != SQLITE_NULL;
    int origin_len = has_origin ? sqlite3_column_bytes(sel, 4) : 0;
    const char *origin_data = has_origin ? (const char *)sqlite3_column_text(sel, 4) : NULL;

    int has_expires = sqlite3_column_type(sel, 5) != SQLITE_NULL;
    ErlNifSInt64 expires_at = 0;
    if (has_expires)
        expires_at = sqlite3_column_int64(sel, 5);

    int has_deleted = sqlite3_column_type(sel, 6) != SQLITE_NULL;
    ErlNifSInt64 deleted_at = 0;
    if (has_deleted)
        deleted_at = sqlite3_column_int64(sel, 6);

    /* Copy value data before finalizing sel (data owned by stmt) */
    void *value_copy = NULL;
    if (has_value && value_len > 0) {
        value_copy = enif_alloc(value_len);
        memcpy(value_copy, value_data, value_len);
    }
    char *origin_copy = NULL;
    if (has_origin && origin_len > 0) {
        origin_copy = enif_alloc(origin_len);
        memcpy(origin_copy, origin_data, origin_len);
    }

    sqlite3_finalize(sel);

    /* 4. Read prev value from kv (for subscriber events) */
    sqlite3_stmt *prev_sel = NULL;
    rc = sqlite3_prepare_v3(conn->db,
        "SELECT value FROM kv WHERE key = ?1 AND deleted_at IS NULL",
        -1, 0, &prev_sel, NULL);
    ERL_NIF_TERM prev_value = atom_nil;
    if (rc == SQLITE_OK) {
        sqlite3_bind_text(prev_sel, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);
        if (sqlite3_step(prev_sel) == SQLITE_ROW) {
            prev_value = make_column(env, prev_sel, 0);
        }
        sqlite3_finalize(prev_sel);
    }

    /* 5. Bind + step kv_force_upsert: [key, value, ts, origin, expires, deleted] */
    sqlite3_reset(kv_s->stmt);
    sqlite3_clear_bindings(kv_s->stmt);
    sqlite3_bind_text(kv_s->stmt, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);
    if (has_value)
        sqlite3_bind_blob(kv_s->stmt, 2, value_copy, value_len, SQLITE_TRANSIENT);
    else
        sqlite3_bind_null(kv_s->stmt, 2);
    sqlite3_bind_int64(kv_s->stmt, 3, timestamp);
    if (has_origin)
        sqlite3_bind_text(kv_s->stmt, 4, origin_copy, origin_len, SQLITE_TRANSIENT);
    else
        sqlite3_bind_null(kv_s->stmt, 4);
    if (has_expires)
        sqlite3_bind_int64(kv_s->stmt, 5, expires_at);
    else
        sqlite3_bind_null(kv_s->stmt, 5);
    if (has_deleted)
        sqlite3_bind_int64(kv_s->stmt, 6, deleted_at);
    else
        sqlite3_bind_null(kv_s->stmt, 6);

    rc = sqlite3_step(kv_s->stmt);
    if (rc != SQLITE_DONE) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_reset(kv_s->stmt);
        if (value_copy) enif_free(value_copy);
        if (origin_copy) enif_free(origin_copy);
        enif_free(ballot_n_str);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }
    sqlite3_reset(kv_s->stmt);

    /* 6. Bind + step oplog_insert: [key, value, ts, origin, expires, is_delete] */
    sqlite3_reset(oplog_s->stmt);
    sqlite3_clear_bindings(oplog_s->stmt);
    sqlite3_bind_text(oplog_s->stmt, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);
    if (has_value)
        sqlite3_bind_blob(oplog_s->stmt, 2, value_copy, value_len, SQLITE_TRANSIENT);
    else
        sqlite3_bind_null(oplog_s->stmt, 2);
    sqlite3_bind_int64(oplog_s->stmt, 3, timestamp);
    if (has_origin)
        sqlite3_bind_text(oplog_s->stmt, 4, origin_copy, origin_len, SQLITE_TRANSIENT);
    else
        sqlite3_bind_null(oplog_s->stmt, 4);
    if (has_expires)
        sqlite3_bind_int64(oplog_s->stmt, 5, expires_at);
    else
        sqlite3_bind_null(oplog_s->stmt, 5);
    sqlite3_bind_int64(oplog_s->stmt, 6, has_deleted ? 1 : 0);  /* is_delete */

    rc = sqlite3_step(oplog_s->stmt);
    if (rc != SQLITE_DONE) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_reset(oplog_s->stmt);
        if (value_copy) enif_free(value_copy);
        if (origin_copy) enif_free(origin_copy);
        enif_free(ballot_n_str);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }
    sqlite3_reset(oplog_s->stmt);

    /* 7. Clear accepted columns in kv_paxos (no storage doubling) */
    sqlite3_stmt *clr = NULL;
    rc = sqlite3_prepare_v3(conn->db,
        "UPDATE kv_paxos SET accepted_counter = 0, accepted_node = '', "
        "accepted_value = NULL, accepted_timestamp = NULL, accepted_origin = NULL, "
        "accepted_expires_at = NULL, accepted_deleted_at = NULL "
        "WHERE key = ?1",
        -1, 0, &clr, NULL);
    if (rc == SQLITE_OK) {
        sqlite3_bind_text(clr, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);
        sqlite3_step(clr);
        sqlite3_finalize(clr);
    }

    /* 8. COMMIT */
    rc = sqlite3_exec(conn->db, "COMMIT", NULL, NULL, NULL);
    if (value_copy) enif_free(value_copy);
    if (origin_copy) enif_free(origin_copy);
    enif_free(ballot_n_str);

    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    enif_mutex_unlock(conn->mutex);

    /* {:ok, value, timestamp, origin, expires_at, deleted_at, prev_value_or_nil} */
    return enif_make_tuple(env, 7, atom_ok,
        value_term, ts_term, origin_term, expires_term, deleted_term, prev_value);
}

/* ------------------------------------------------------------------ */
/* NIF table & lifecycle                                               */
/* ------------------------------------------------------------------ */

static ErlNifFunc nif_funcs[] = {
    {"ekv_open",          1, ekv_open,          ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_close",         1, ekv_close,         ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_execute",       2, ekv_execute,       ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_prepare",       2, ekv_prepare,       ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_bind",          2, ekv_bind,          ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_step",          2, ekv_step,          ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_release",       2, ekv_release,       ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_write_entry",   5, ekv_write_entry,   ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_read_entry",    3, ekv_read_entry,    ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_fetch_all",     3, ekv_fetch_all,     ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_backup",        2, ekv_backup,        ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_paxos_prepare", 4, ekv_paxos_prepare, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_paxos_accept",  5, ekv_paxos_accept,  ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_paxos_promote", 6, ekv_paxos_promote, ERL_NIF_DIRTY_JOB_IO_BOUND},
};

static int on_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    (void)priv_data;
    (void)load_info;

    connection_type = enif_open_resource_type(env, NULL, "connection",
        connection_dtor, ERL_NIF_RT_CREATE, NULL);
    if (!connection_type) return -1;

    statement_type = enif_open_resource_type(env, NULL, "statement",
        statement_dtor, ERL_NIF_RT_CREATE, NULL);
    if (!statement_type) return -1;

    atom_ok      = make_atom(env, "ok");
    atom_error   = make_atom(env, "error");
    atom_nil     = make_atom(env, "nil");
    atom_row     = make_atom(env, "row");
    atom_done    = make_atom(env, "done");
    atom_true    = make_atom(env, "true");
    atom_false   = make_atom(env, "false");
    atom_promise = make_atom(env, "promise");
    atom_nack    = make_atom(env, "nack");
    atom_stale   = make_atom(env, "stale");

    return 0;
}

ERL_NIF_INIT(Elixir.EKV.Sqlite3NIF, nif_funcs, on_load, NULL, NULL, NULL)
