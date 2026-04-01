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
    sqlite3_stmt *select_local_origin_seq_stmt;
    sqlite3_stmt *upsert_local_origin_seq_stmt;
    sqlite3_stmt *select_origin_progress_stmt;
    sqlite3_stmt *upsert_origin_progress_stmt;
    sqlite3_stmt *scan_origin_oplog_stmt;
    sqlite3_stmt *cas_managed_key_check_stmt;
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
static ERL_NIF_TERM atom_applied;
static ERL_NIF_TERM atom_ignored;
static ERL_NIF_TERM atom_cas_managed_key;
static ERL_NIF_TERM atom_promise;
static ERL_NIF_TERM atom_nack;
static ERL_NIF_TERM atom_stale;

static int read_local_origin_seq(connection_t *conn, sqlite3_int64 *out_seq);
static int write_local_origin_seq(connection_t *conn, sqlite3_int64 seq);

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

static int make_binary_term(ErlNifEnv *env, const void *src, size_t len, ERL_NIF_TERM *term)
{
    ErlNifBinary bin;
    if (!enif_alloc_binary(len, &bin))
        return 0;

    if (len > 0) {
        if (!src) {
            enif_release_binary(&bin);
            return 0;
        }
        memcpy(bin.data, src, len);
    }

    *term = enif_make_binary(env, &bin);
    return 1;
}

static int copy_alloc(const void *src, size_t len, void **dst)
{
    void *copy;

    *dst = NULL;
    if (len == 0)
        return 1;
    if (!src)
        return 0;

    copy = enif_alloc(len);
    if (!copy)
        return 0;

    memcpy(copy, src, len);
    *dst = copy;
    return 1;
}

static ERL_NIF_TERM make_sqlite_error(ErlNifEnv *env, sqlite3 *db)
{
    const char *msg = sqlite3_errmsg(db);
    ERL_NIF_TERM bin;

    if (make_binary_term(env, msg, strlen(msg), &bin))
        return enif_make_tuple2(env, atom_error, bin);

    return make_error(env, msg);
}

static void finalize_stmt(sqlite3_stmt **stmt)
{
    if (*stmt) {
        sqlite3_finalize(*stmt);
        *stmt = NULL;
    }
}

static void finalize_cached_connection_stmts(connection_t *conn)
{
    finalize_stmt(&conn->select_local_origin_seq_stmt);
    finalize_stmt(&conn->upsert_local_origin_seq_stmt);
    finalize_stmt(&conn->select_origin_progress_stmt);
    finalize_stmt(&conn->upsert_origin_progress_stmt);
    finalize_stmt(&conn->scan_origin_oplog_stmt);
    finalize_stmt(&conn->cas_managed_key_check_stmt);
}

static int ensure_cached_stmt(sqlite3 *db, sqlite3_stmt **stmt, const char *sql)
{
    if (*stmt)
        return SQLITE_OK;

    return sqlite3_prepare_v3(db, sql, -1, 0, stmt, NULL);
}

/* ------------------------------------------------------------------ */
/* Resource destructors                                                */
/* ------------------------------------------------------------------ */

static void connection_dtor(ErlNifEnv *env, void *obj)
{
    (void)env;
    connection_t *conn = (connection_t *)obj;
    finalize_cached_connection_stmts(conn);
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
    if (!conn) {
        sqlite3_close_v2(db);
        return make_error(env, "alloc failed");
    }
    memset(conn, 0, sizeof(connection_t));
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
    finalize_cached_connection_stmts(conn);
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
    if (!s) {
        sqlite3_finalize(raw_stmt);
        return make_error(env, "alloc failed");
    }
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
            if (!enif_inspect_binary(env, head, &bin))
                return -1;
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

static int list_nth_term(ErlNifEnv *env, ERL_NIF_TERM list, unsigned nth, ERL_NIF_TERM *out)
{
    ERL_NIF_TERM head;
    unsigned idx = 0;

    while (enif_get_list_cell(env, list, &head, &list)) {
        if (idx == nth) {
            *out = head;
            return 1;
        }
        idx++;
    }

    return 0;
}

static int bind_and_step_single_text(
    sqlite3_stmt *stmt,
    const char *text,
    int text_len,
    int *step_rc
)
{
    int rc;

    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
    rc = sqlite3_bind_text(stmt, 1, text, text_len, SQLITE_TRANSIENT);
    if (rc != SQLITE_OK)
        return rc;

    rc = sqlite3_step(stmt);
    if (step_rc)
        *step_rc = rc;

    if (rc == SQLITE_DONE)
        return SQLITE_OK;

    return rc;
}

static int next_local_origin_seq(connection_t *conn, sqlite3_int64 *out_seq)
{
    int rc;

    rc = read_local_origin_seq(conn, out_seq);
    if (rc != SQLITE_OK) return rc;

    *out_seq = *out_seq + 1;
    return write_local_origin_seq(conn, *out_seq);
}

static int read_local_origin_seq(connection_t *conn, sqlite3_int64 *out_seq)
{
    sqlite3_int64 current = 0;
    int rc;

    rc = ensure_cached_stmt(
        conn->db,
        &conn->select_local_origin_seq_stmt,
        "SELECT value_int FROM kv_meta WHERE key = 'local_origin_seq'"
    );
    if (rc != SQLITE_OK) return rc;

    sqlite3_reset(conn->select_local_origin_seq_stmt);
    sqlite3_clear_bindings(conn->select_local_origin_seq_stmt);

    rc = sqlite3_step(conn->select_local_origin_seq_stmt);
    if (rc == SQLITE_ROW && sqlite3_column_type(conn->select_local_origin_seq_stmt, 0) != SQLITE_NULL) {
        current = sqlite3_column_int64(conn->select_local_origin_seq_stmt, 0);
        rc = SQLITE_OK;
    } else if (rc == SQLITE_DONE) {
        rc = SQLITE_OK;
    }
    sqlite3_reset(conn->select_local_origin_seq_stmt);
    sqlite3_clear_bindings(conn->select_local_origin_seq_stmt);
    if (rc != SQLITE_OK) return rc;

    *out_seq = current;
    return SQLITE_OK;
}

static int write_local_origin_seq(connection_t *conn, sqlite3_int64 seq)
{
    int rc;

    rc = ensure_cached_stmt(
        conn->db,
        &conn->upsert_local_origin_seq_stmt,
        "INSERT INTO kv_meta (key, value_int) VALUES ('local_origin_seq', ?1) "
        "ON CONFLICT(key) DO UPDATE SET value_int = excluded.value_int"
    );
    if (rc != SQLITE_OK) return rc;

    sqlite3_reset(conn->upsert_local_origin_seq_stmt);
    sqlite3_clear_bindings(conn->upsert_local_origin_seq_stmt);
    sqlite3_bind_int64(conn->upsert_local_origin_seq_stmt, 1, seq);
    rc = sqlite3_step(conn->upsert_local_origin_seq_stmt);
    sqlite3_reset(conn->upsert_local_origin_seq_stmt);
    sqlite3_clear_bindings(conn->upsert_local_origin_seq_stmt);

    return (rc == SQLITE_DONE) ? SQLITE_OK : rc;
}

static int read_local_origin_progress(
    connection_t *conn,
    const char *origin_node,
    int origin_node_len,
    sqlite3_int64 *out_seq
)
{
    int rc = ensure_cached_stmt(
        conn->db,
        &conn->select_origin_progress_stmt,
        "SELECT last_seq FROM kv_origin_progress WHERE origin_node = ?1"
    );
    if (rc != SQLITE_OK) return rc;

    sqlite3_reset(conn->select_origin_progress_stmt);
    sqlite3_clear_bindings(conn->select_origin_progress_stmt);
    sqlite3_bind_text(conn->select_origin_progress_stmt, 1, origin_node, origin_node_len, SQLITE_TRANSIENT);
    rc = sqlite3_step(conn->select_origin_progress_stmt);

    if (rc == SQLITE_ROW && sqlite3_column_type(conn->select_origin_progress_stmt, 0) != SQLITE_NULL) {
        *out_seq = sqlite3_column_int64(conn->select_origin_progress_stmt, 0);
        rc = SQLITE_OK;
    } else if (rc == SQLITE_DONE) {
        *out_seq = 0;
        rc = SQLITE_OK;
    }

    sqlite3_reset(conn->select_origin_progress_stmt);
    sqlite3_clear_bindings(conn->select_origin_progress_stmt);
    return rc;
}

static int write_local_origin_progress(
    connection_t *conn,
    const char *origin_node,
    int origin_node_len,
    sqlite3_int64 origin_seq
)
{
    int rc = ensure_cached_stmt(
        conn->db,
        &conn->upsert_origin_progress_stmt,
        "INSERT INTO kv_origin_progress (origin_node, last_seq) VALUES (?1, ?2) "
        "ON CONFLICT(origin_node) DO UPDATE SET last_seq = excluded.last_seq"
    );
    if (rc != SQLITE_OK) return rc;

    sqlite3_reset(conn->upsert_origin_progress_stmt);
    sqlite3_clear_bindings(conn->upsert_origin_progress_stmt);
    sqlite3_bind_text(conn->upsert_origin_progress_stmt, 1, origin_node, origin_node_len, SQLITE_TRANSIENT);
    sqlite3_bind_int64(conn->upsert_origin_progress_stmt, 2, origin_seq);
    rc = sqlite3_step(conn->upsert_origin_progress_stmt);
    sqlite3_reset(conn->upsert_origin_progress_stmt);
    sqlite3_clear_bindings(conn->upsert_origin_progress_stmt);

    return (rc == SQLITE_DONE) ? SQLITE_OK : rc;
}

static int advance_local_origin_progress(
    connection_t *conn,
    const char *origin_node,
    int origin_node_len,
    sqlite3_int64 seen_seq,
    sqlite3_int64 *out_progress
)
{
    sqlite3_int64 current = 0;
    sqlite3_int64 last_contiguous = 0;
    sqlite3_int64 expected = 0;
    int rc = read_local_origin_progress(conn, origin_node, origin_node_len, &current);
    if (rc != SQLITE_OK) return rc;

    if (seen_seq <= current) {
        *out_progress = current;
        return SQLITE_OK;
    }

    if (seen_seq > current + 1) {
        *out_progress = current;
        return SQLITE_OK;
    }

    rc = ensure_cached_stmt(
        conn->db,
        &conn->scan_origin_oplog_stmt,
        "SELECT origin_seq FROM kv_oplog "
        "WHERE origin_node = ?1 AND origin_seq > ?2 "
        "ORDER BY origin_seq"
    );
    if (rc != SQLITE_OK) return rc;

    sqlite3_reset(conn->scan_origin_oplog_stmt);
    sqlite3_clear_bindings(conn->scan_origin_oplog_stmt);
    sqlite3_bind_text(conn->scan_origin_oplog_stmt, 1, origin_node, origin_node_len, SQLITE_TRANSIENT);
    sqlite3_bind_int64(conn->scan_origin_oplog_stmt, 2, current);

    last_contiguous = current;
    expected = current + 1;

    while ((rc = sqlite3_step(conn->scan_origin_oplog_stmt)) == SQLITE_ROW) {
        sqlite3_int64 row_seq = sqlite3_column_int64(conn->scan_origin_oplog_stmt, 0);
        if (row_seq != expected)
            break;

        last_contiguous = row_seq;
        expected++;
    }

    sqlite3_reset(conn->scan_origin_oplog_stmt);
    sqlite3_clear_bindings(conn->scan_origin_oplog_stmt);

    if (rc != SQLITE_ROW && rc != SQLITE_DONE)
        return rc;

    rc = write_local_origin_progress(conn, origin_node, origin_node_len, last_contiguous);
    if (rc != SQLITE_OK) return rc;

    *out_progress = last_contiguous;
    return SQLITE_OK;
}

static int refresh_local_origin_progress(
    connection_t *conn,
    const char *origin_node,
    int origin_node_len,
    sqlite3_int64 *out_progress
)
{
    sqlite3_int64 current = 0;
    sqlite3_int64 last_contiguous = 0;
    sqlite3_int64 expected = 0;
    int rc = read_local_origin_progress(conn, origin_node, origin_node_len, &current);
    if (rc != SQLITE_OK) return rc;

    rc = ensure_cached_stmt(
        conn->db,
        &conn->scan_origin_oplog_stmt,
        "SELECT origin_seq FROM kv_oplog "
        "WHERE origin_node = ?1 AND origin_seq > ?2 "
        "ORDER BY origin_seq"
    );
    if (rc != SQLITE_OK) return rc;

    sqlite3_reset(conn->scan_origin_oplog_stmt);
    sqlite3_clear_bindings(conn->scan_origin_oplog_stmt);
    sqlite3_bind_text(conn->scan_origin_oplog_stmt, 1, origin_node, origin_node_len, SQLITE_TRANSIENT);
    sqlite3_bind_int64(conn->scan_origin_oplog_stmt, 2, current);

    last_contiguous = current;
    expected = current + 1;

    while ((rc = sqlite3_step(conn->scan_origin_oplog_stmt)) == SQLITE_ROW) {
        sqlite3_int64 row_seq = sqlite3_column_int64(conn->scan_origin_oplog_stmt, 0);
        if (row_seq != expected)
            break;

        last_contiguous = row_seq;
        expected++;
    }

    sqlite3_reset(conn->scan_origin_oplog_stmt);
    sqlite3_clear_bindings(conn->scan_origin_oplog_stmt);

    if (rc != SQLITE_ROW && rc != SQLITE_DONE)
        return rc;

    if (last_contiguous > current) {
        rc = write_local_origin_progress(conn, origin_node, origin_node_len, last_contiguous);
        if (rc != SQLITE_OK) return rc;
    }

    *out_progress = last_contiguous;
    return SQLITE_OK;
}

static int begin_immediate(sqlite3 *db)
{
    return sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
}

static void rollback_tx(sqlite3 *db)
{
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
}

static int commit_tx(sqlite3 *db)
{
    return sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
}

static int get_progress_entry(
    ErlNifEnv *env,
    ERL_NIF_TERM term,
    ErlNifBinary *origin_node,
    ErlNifSInt64 *seq
)
{
    const ERL_NIF_TERM *elems;
    int arity;

    if (!enif_get_tuple(env, term, &arity, &elems) || arity != 2) {
        return 0;
    }

    if (!enif_inspect_iolist_as_binary(env, elems[0], origin_node)) {
        return 0;
    }

    if (!enif_get_int64(env, elems[1], seq)) {
        return 0;
    }

    return 1;
}

static int get_replication_batch_entry(
    ErlNifEnv *env,
    ERL_NIF_TERM term,
    ErlNifBinary *key_bin,
    int *has_value,
    ErlNifBinary *value_bin,
    ErlNifSInt64 *timestamp,
    ErlNifSInt64 *origin_seq,
    int *has_expires_at,
    ErlNifSInt64 *expires_at,
    int *has_deleted_at,
    ErlNifSInt64 *deleted_at
)
{
    const ERL_NIF_TERM *elems;
    int arity;

    if (!enif_get_tuple(env, term, &arity, &elems) || arity != 6) {
        return 0;
    }

    if (!enif_inspect_iolist_as_binary(env, elems[0], key_bin)) {
        return 0;
    }

    if (enif_is_identical(elems[1], atom_nil)) {
        *has_value = 0;
    } else {
        if (!enif_inspect_iolist_as_binary(env, elems[1], value_bin)) {
            return 0;
        }
        *has_value = 1;
    }

    if (!enif_get_int64(env, elems[2], timestamp)) {
        return 0;
    }

    if (!enif_get_int64(env, elems[3], origin_seq) || *origin_seq < 0) {
        return 0;
    }

    if (enif_is_identical(elems[4], atom_nil)) {
        *has_expires_at = 0;
    } else {
        if (!enif_get_int64(env, elems[4], expires_at)) {
            return 0;
        }
        *has_expires_at = 1;
    }

    if (enif_is_identical(elems[5], atom_nil)) {
        *has_deleted_at = 0;
    } else {
        if (!enif_get_int64(env, elems[5], deleted_at)) {
            return 0;
        }
        *has_deleted_at = 1;
    }

    return 1;
}

static int get_local_batch_entry(
    ErlNifEnv *env,
    ERL_NIF_TERM term,
    ErlNifBinary *key_bin,
    int *has_value,
    ErlNifBinary *value_bin,
    ErlNifSInt64 *timestamp,
    int *has_expires_at,
    ErlNifSInt64 *expires_at,
    int *has_deleted_at,
    ErlNifSInt64 *deleted_at
)
{
    const ERL_NIF_TERM *elems;
    int arity;

    if (!enif_get_tuple(env, term, &arity, &elems) || arity != 5) {
        return 0;
    }

    if (!enif_inspect_iolist_as_binary(env, elems[0], key_bin)) {
        return 0;
    }

    if (enif_is_identical(elems[1], atom_nil)) {
        *has_value = 0;
    } else {
        if (!enif_inspect_iolist_as_binary(env, elems[1], value_bin)) {
            return 0;
        }
        *has_value = 1;
    }

    if (!enif_get_int64(env, elems[2], timestamp)) {
        return 0;
    }

    if (enif_is_identical(elems[3], atom_nil)) {
        *has_expires_at = 0;
    } else {
        if (!enif_get_int64(env, elems[3], expires_at)) {
            return 0;
        }
        *has_expires_at = 1;
    }

    if (enif_is_identical(elems[4], atom_nil)) {
        *has_deleted_at = 0;
    } else {
        if (!enif_get_int64(env, elems[4], deleted_at)) {
            return 0;
        }
        *has_deleted_at = 1;
    }

    return 1;
}

static int step_progress_entries(
    ErlNifEnv *env,
    sqlite3_stmt *stmt,
    ERL_NIF_TERM entries,
    const char *member_node,
    int member_node_len
)
{
    ERL_NIF_TERM head;

    while (enif_get_list_cell(env, entries, &head, &entries)) {
        ErlNifBinary origin_node;
        ErlNifSInt64 seq;

        if (!get_progress_entry(env, head, &origin_node, &seq)) {
            return -1;
        }

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        if (member_node != NULL) {
            int rc = sqlite3_bind_text(stmt, 1, member_node, member_node_len, SQLITE_TRANSIENT);
            if (rc != SQLITE_OK) return rc;

            rc = sqlite3_bind_text(
                stmt,
                2,
                (const char *)origin_node.data,
                (int)origin_node.size,
                SQLITE_TRANSIENT
            );
            if (rc != SQLITE_OK) return rc;

            rc = sqlite3_bind_int64(stmt, 3, (sqlite3_int64)seq);
            if (rc != SQLITE_OK) return rc;
        } else {
            int rc = sqlite3_bind_text(
                stmt,
                1,
                (const char *)origin_node.data,
                (int)origin_node.size,
                SQLITE_TRANSIENT
            );
            if (rc != SQLITE_OK) return rc;

            rc = sqlite3_bind_int64(stmt, 2, (sqlite3_int64)seq);
            if (rc != SQLITE_OK) return rc;
        }

        if (sqlite3_step(stmt) != SQLITE_DONE) {
            return sqlite3_errcode(sqlite3_db_handle(stmt));
        }
    }

    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
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

static int make_column(ErlNifEnv *env, sqlite3_stmt *stmt, int col, ERL_NIF_TERM *term)
{
    switch (sqlite3_column_type(stmt, col)) {
    case SQLITE_INTEGER:
        *term = enif_make_int64(env, sqlite3_column_int64(stmt, col));
        return 1;

    case SQLITE_FLOAT:
        *term = enif_make_double(env, sqlite3_column_double(stmt, col));
        return 1;

    case SQLITE_TEXT: {
        int len = sqlite3_column_bytes(stmt, col);
        const unsigned char *text = sqlite3_column_text(stmt, col);
        return make_binary_term(env, text, (size_t)len, term);
    }

    case SQLITE_BLOB: {
        int len = sqlite3_column_bytes(stmt, col);
        const void *blob = sqlite3_column_blob(stmt, col);
        return make_binary_term(env, blob, (size_t)len, term);
    }

    case SQLITE_NULL:
    default:
        *term = atom_nil;
        return 1;
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
        for (int i = 0; i < ncols; i++) {
            if (!make_column(env, s->stmt, i, &cols[i])) {
                enif_free(cols);
                sqlite3_reset(s->stmt);
                enif_mutex_unlock(conn->mutex);
                return make_error(env, "alloc failed");
            }
        }
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
/* NIF: write_entry(db, kv_stmt, keyref_stmt, oplog_stmt, kv_args,     */
/*                  oplog_args, local_origin, reject_cas_managed)      */
/*   -> {:ok, true, origin_seq} | {:ok, false}                         */
/*   -> {:ok, false, origin_seq} | {:error, msg}                       */
/*                                                                     */
/* Single dirty IO bounce: BEGIN IMMEDIATE, bind+step kv upsert,       */
/* check sqlite3_changes() for LWW result. If 0 (LWW lost), ROLLBACK  */
/* and return {:ok, false}. Otherwise bind+step oplog, advance the     */
/* contiguous local origin progress, COMMIT, return. When              */
/* reject_cas_managed is true, abort early if kv_paxos already has     */
/* state for the key.                                                  */
/* {:ok, applied?, origin_seq, local_progress_seq}.                    */
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

    statement_t *keyref_s;
    if (!enif_get_resource(env, argv[2], statement_type, (void **)&keyref_s))
        return enif_make_badarg(env);

    statement_t *oplog_s;
    if (!enif_get_resource(env, argv[3], statement_type, (void **)&oplog_s))
        return enif_make_badarg(env);

    if (kv_s->conn != conn || keyref_s->conn != conn || oplog_s->conn != conn)
        return make_error(env, "statement does not belong to this connection");

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "database closed");
    }
    if (!kv_s->stmt || !keyref_s->stmt || !oplog_s->stmt) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "statement finalized");
    }

    ERL_NIF_TERM kv_origin_term;
    ERL_NIF_TERM kv_origin_seq_term;
    ERL_NIF_TERM key_term;
    if (!list_nth_term(env, argv[4], 0, &key_term) ||
        !list_nth_term(env, argv[4], 3, &kv_origin_term) ||
        !list_nth_term(env, argv[4], 4, &kv_origin_seq_term)) {
        enif_mutex_unlock(conn->mutex);
        return enif_make_badarg(env);
    }

    ErlNifBinary key_bin;
    if (!enif_inspect_iolist_as_binary(env, key_term, &key_bin)) {
        enif_mutex_unlock(conn->mutex);
        return enif_make_badarg(env);
    }

    ErlNifBinary origin_bin;
    if (!enif_inspect_iolist_as_binary(env, kv_origin_term, &origin_bin)) {
        enif_mutex_unlock(conn->mutex);
        return enif_make_badarg(env);
    }

    sqlite3_int64 origin_seq = 0;
    sqlite3_int64 local_progress_seq = 0;
    int origin_seq_provided = !enif_is_identical(kv_origin_seq_term, atom_nil);
    int local_origin = enif_is_identical(argv[6], atom_true);
    int reject_cas_managed = enif_is_identical(argv[7], atom_true);
    if (origin_seq_provided) {
        if (!enif_get_int64(env, kv_origin_seq_term, (ErlNifSInt64 *)&origin_seq)) {
            enif_mutex_unlock(conn->mutex);
            return enif_make_badarg(env);
        }
    }

    /* 2. BEGIN IMMEDIATE */
    int rc = sqlite3_exec(conn->db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_reset(kv_s->stmt);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    if (reject_cas_managed) {
        sqlite3_stmt *cas_stmt = NULL;
        int step_rc = SQLITE_OK;

        rc = ensure_cached_stmt(
            conn->db,
            &conn->cas_managed_key_check_stmt,
            "SELECT 1 FROM kv_paxos WHERE key = ?1 LIMIT 1"
        );
        if (rc != SQLITE_OK) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
            enif_mutex_unlock(conn->mutex);
            return err;
        }

        cas_stmt = conn->cas_managed_key_check_stmt;
        sqlite3_reset(cas_stmt);
        sqlite3_clear_bindings(cas_stmt);
        rc = sqlite3_bind_text(cas_stmt, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);
        if (rc == SQLITE_OK) {
            step_rc = sqlite3_step(cas_stmt);
        } else {
            step_rc = rc;
        }

        if (step_rc != SQLITE_ROW && step_rc != SQLITE_DONE) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_reset(cas_stmt);
            sqlite3_clear_bindings(cas_stmt);
            sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
            enif_mutex_unlock(conn->mutex);
            return err;
        }

        sqlite3_reset(cas_stmt);
        sqlite3_clear_bindings(cas_stmt);

        if (step_rc == SQLITE_ROW) {
            sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
            enif_mutex_unlock(conn->mutex);
            return enif_make_tuple2(env, atom_error, atom_cas_managed_key);
        }
    }

    if (!origin_seq_provided) {
        rc = next_local_origin_seq(conn, &origin_seq);
        if (rc != SQLITE_OK) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
            enif_mutex_unlock(conn->mutex);
            return err;
        }
    }

    /* 3. Bind + step kv upsert */
    int br = bind_args(env, kv_s->stmt, argv[4]);
    if (br != 0) {
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return (br == -1) ? enif_make_badarg(env)
                          : make_sqlite_error(env, conn->db);
    }
    sqlite3_bind_int64(kv_s->stmt, 5, origin_seq);

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
        /* Local-origin LWW loss creates no replay history or progress. */
        if (!origin_seq_provided) {
            sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
            enif_mutex_unlock(conn->mutex);
            return enif_make_tuple2(env, atom_ok, atom_false);
        }
    }

    /* 5. Ensure keyref exists and marks current-state presence. */
    int step_rc = SQLITE_OK;
    rc = bind_and_step_single_text(
        keyref_s->stmt,
        (const char *)key_bin.data,
        (int)key_bin.size,
        &step_rc
    );
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_reset(keyref_s->stmt);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }
    sqlite3_reset(keyref_s->stmt);

    /* 6. Bind + step oplog */
    br = bind_args(env, oplog_s->stmt, argv[5]);
    if (br != 0) {
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return (br == -1) ? enif_make_badarg(env)
                          : make_sqlite_error(env, conn->db);
    }
    sqlite3_bind_int64(oplog_s->stmt, 5, origin_seq);

    rc = sqlite3_step(oplog_s->stmt);
    if (rc != SQLITE_DONE) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_reset(oplog_s->stmt);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }
    sqlite3_reset(oplog_s->stmt);

    if (local_origin && !origin_seq_provided) {
        rc = write_local_origin_progress(
            conn,
            (const char *)origin_bin.data,
            (int)origin_bin.size,
            origin_seq
        );
        local_progress_seq = origin_seq;
    } else {
        rc = advance_local_origin_progress(
            conn,
            (const char *)origin_bin.data,
            (int)origin_bin.size,
            origin_seq,
            &local_progress_seq
        );
    }
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    /* 6. COMMIT */
    rc = sqlite3_exec(conn->db, "COMMIT", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    enif_mutex_unlock(conn->mutex);
    if (changes > 0) {
        return enif_make_tuple4(
            env,
            atom_ok,
            atom_true,
            enif_make_int64(env, (ErlNifSInt64)origin_seq),
            enif_make_int64(env, (ErlNifSInt64)local_progress_seq)
        );
    }

    return enif_make_tuple4(
        env,
        atom_ok,
        atom_false,
        enif_make_int64(env, (ErlNifSInt64)origin_seq),
        enif_make_int64(env, (ErlNifSInt64)local_progress_seq)
    );
}

static ERL_NIF_TERM ekv_write_entries_batch(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    statement_t *kv_s;
    statement_t *keyref_s;
    statement_t *oplog_s;
    ErlNifBinary origin_bin;
    unsigned int entry_count = 0;
    int *applied_flags = NULL;
    ERL_NIF_TERM list;
    ERL_NIF_TERM head;
    sqlite3_int64 last_origin_seq = 0;
    sqlite3_int64 local_progress_seq = 0;
    unsigned int idx = 0;

    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    if (!enif_get_resource(env, argv[1], statement_type, (void **)&kv_s))
        return enif_make_badarg(env);

    if (!enif_get_resource(env, argv[2], statement_type, (void **)&keyref_s))
        return enif_make_badarg(env);

    if (!enif_get_resource(env, argv[3], statement_type, (void **)&oplog_s))
        return enif_make_badarg(env);

    if (kv_s->conn != conn || keyref_s->conn != conn || oplog_s->conn != conn)
        return make_error(env, "statement does not belong to this connection");

    if (!enif_inspect_iolist_as_binary(env, argv[4], &origin_bin))
        return enif_make_badarg(env);

    if (!enif_get_list_length(env, argv[5], &entry_count) || entry_count == 0)
        return enif_make_badarg(env);

    list = argv[5];
    applied_flags = enif_alloc(sizeof(int) * entry_count);
    if (!applied_flags)
        return make_error(env, "alloc failed");

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        enif_free(applied_flags);
        return make_error(env, "database closed");
    }
    if (!kv_s->stmt || !keyref_s->stmt || !oplog_s->stmt) {
        enif_mutex_unlock(conn->mutex);
        enif_free(applied_flags);
        return make_error(env, "statement finalized");
    }

    int rc = begin_immediate(conn->db);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        enif_mutex_unlock(conn->mutex);
        enif_free(applied_flags);
        return err;
    }

    while (enif_get_list_cell(env, list, &head, &list)) {
        ErlNifBinary key_bin;
        ErlNifBinary value_bin;
        ErlNifSInt64 timestamp = 0;
        ErlNifSInt64 origin_seq = 0;
        ErlNifSInt64 expires_at = 0;
        ErlNifSInt64 deleted_at = 0;
        int has_value = 0;
        int has_expires_at = 0;
        int has_deleted_at = 0;
        int changes = 0;
        int bind_rc;
        int step_rc = SQLITE_OK;

        if (!get_replication_batch_entry(
                env,
                head,
                &key_bin,
                &has_value,
                &value_bin,
                &timestamp,
                &origin_seq,
                &has_expires_at,
                &expires_at,
                &has_deleted_at,
                &deleted_at
            )) {
            sqlite3_reset(kv_s->stmt);
            sqlite3_reset(keyref_s->stmt);
            sqlite3_reset(oplog_s->stmt);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(applied_flags);
            return enif_make_badarg(env);
        }

        sqlite3_reset(kv_s->stmt);
        sqlite3_clear_bindings(kv_s->stmt);
        bind_rc = sqlite3_bind_text(kv_s->stmt, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);
        if (bind_rc == SQLITE_OK) {
            bind_rc = has_value
                ? sqlite3_bind_text(kv_s->stmt, 2, (const char *)value_bin.data, (int)value_bin.size, SQLITE_TRANSIENT)
                : sqlite3_bind_null(kv_s->stmt, 2);
        }
        if (bind_rc == SQLITE_OK) bind_rc = sqlite3_bind_int64(kv_s->stmt, 3, (sqlite3_int64)timestamp);
        if (bind_rc == SQLITE_OK) bind_rc = sqlite3_bind_text(kv_s->stmt, 4, (const char *)origin_bin.data, (int)origin_bin.size, SQLITE_TRANSIENT);
        if (bind_rc == SQLITE_OK) bind_rc = sqlite3_bind_int64(kv_s->stmt, 5, (sqlite3_int64)origin_seq);
        if (bind_rc == SQLITE_OK) {
            bind_rc = has_expires_at
                ? sqlite3_bind_int64(kv_s->stmt, 6, (sqlite3_int64)expires_at)
                : sqlite3_bind_null(kv_s->stmt, 6);
        }
        if (bind_rc == SQLITE_OK) {
            bind_rc = has_deleted_at
                ? sqlite3_bind_int64(kv_s->stmt, 7, (sqlite3_int64)deleted_at)
                : sqlite3_bind_null(kv_s->stmt, 7);
        }
        if (bind_rc != SQLITE_OK) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_reset(kv_s->stmt);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(applied_flags);
            return err;
        }

        rc = sqlite3_step(kv_s->stmt);
        if (rc != SQLITE_DONE) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_reset(kv_s->stmt);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(applied_flags);
            return err;
        }
        sqlite3_reset(kv_s->stmt);
        changes = sqlite3_changes(conn->db);
        applied_flags[idx] = changes > 0;

        rc = bind_and_step_single_text(
            keyref_s->stmt,
            (const char *)key_bin.data,
            (int)key_bin.size,
            &step_rc
        );
        if (rc != SQLITE_OK) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_reset(keyref_s->stmt);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(applied_flags);
            return err;
        }
        sqlite3_reset(keyref_s->stmt);

        sqlite3_reset(oplog_s->stmt);
        sqlite3_clear_bindings(oplog_s->stmt);
        bind_rc = sqlite3_bind_text(oplog_s->stmt, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);
        if (bind_rc == SQLITE_OK) {
            bind_rc = has_value
                ? sqlite3_bind_text(oplog_s->stmt, 2, (const char *)value_bin.data, (int)value_bin.size, SQLITE_TRANSIENT)
                : sqlite3_bind_null(oplog_s->stmt, 2);
        }
        if (bind_rc == SQLITE_OK) bind_rc = sqlite3_bind_int64(oplog_s->stmt, 3, (sqlite3_int64)timestamp);
        if (bind_rc == SQLITE_OK) bind_rc = sqlite3_bind_text(oplog_s->stmt, 4, (const char *)origin_bin.data, (int)origin_bin.size, SQLITE_TRANSIENT);
        if (bind_rc == SQLITE_OK) bind_rc = sqlite3_bind_int64(oplog_s->stmt, 5, (sqlite3_int64)origin_seq);
        if (bind_rc == SQLITE_OK) {
            bind_rc = has_expires_at
                ? sqlite3_bind_int64(oplog_s->stmt, 6, (sqlite3_int64)expires_at)
                : sqlite3_bind_null(oplog_s->stmt, 6);
        }
        if (bind_rc == SQLITE_OK) {
            bind_rc = sqlite3_bind_int(oplog_s->stmt, 7, has_deleted_at ? 1 : 0);
        }
        if (bind_rc != SQLITE_OK) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_reset(oplog_s->stmt);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(applied_flags);
            return err;
        }

        rc = sqlite3_step(oplog_s->stmt);
        if (rc != SQLITE_DONE) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_reset(oplog_s->stmt);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(applied_flags);
            return err;
        }
        sqlite3_reset(oplog_s->stmt);

        last_origin_seq = (sqlite3_int64)origin_seq;
        idx++;
    }

    rc = refresh_local_origin_progress(
        conn,
        (const char *)origin_bin.data,
        (int)origin_bin.size,
        &local_progress_seq
    );
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        enif_free(applied_flags);
        return err;
    }

    rc = commit_tx(conn->db);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        enif_free(applied_flags);
        return err;
    }

    enif_mutex_unlock(conn->mutex);

    ERL_NIF_TERM applied_flags_list = enif_make_list(env, 0);
    while (idx > 0) {
        idx--;
        applied_flags_list =
            enif_make_list_cell(env, applied_flags[idx] ? atom_true : atom_false, applied_flags_list);
    }
    enif_free(applied_flags);

    return enif_make_tuple4(
        env,
        atom_ok,
        applied_flags_list,
        enif_make_int64(env, (ErlNifSInt64)last_origin_seq),
        enif_make_int64(env, (ErlNifSInt64)local_progress_seq)
    );
}

static ERL_NIF_TERM ekv_write_local_entries_batch(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    statement_t *kv_s;
    statement_t *keyref_s;
    statement_t *oplog_s;
    ErlNifBinary origin_bin;
    ErlNifSInt64 starting_origin_seq = 0;
    unsigned int entry_count = 0;
    int *result_kinds = NULL;
    sqlite3_int64 *result_seqs = NULL;
    ERL_NIF_TERM list;
    ERL_NIF_TERM head;
    sqlite3_int64 next_origin_seq = 0;
    unsigned int idx = 0;
    int reject_cas_managed = 0;

    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);
    if (!enif_get_resource(env, argv[1], statement_type, (void **)&kv_s))
        return enif_make_badarg(env);
    if (!enif_get_resource(env, argv[2], statement_type, (void **)&keyref_s))
        return enif_make_badarg(env);
    if (!enif_get_resource(env, argv[3], statement_type, (void **)&oplog_s))
        return enif_make_badarg(env);
    if (kv_s->conn != conn || keyref_s->conn != conn || oplog_s->conn != conn)
        return make_error(env, "statement does not belong to this connection");
    if (!enif_inspect_iolist_as_binary(env, argv[4], &origin_bin))
        return enif_make_badarg(env);
    if (!enif_get_int64(env, argv[5], &starting_origin_seq) || starting_origin_seq < 0)
        return enif_make_badarg(env);
    if (!enif_get_list_length(env, argv[6], &entry_count) || entry_count == 0)
        return enif_make_badarg(env);

    reject_cas_managed = enif_is_identical(argv[7], atom_true);
    list = argv[6];
    next_origin_seq = (sqlite3_int64)starting_origin_seq;

    result_kinds = enif_alloc(sizeof(int) * entry_count);
    result_seqs = enif_alloc(sizeof(sqlite3_int64) * entry_count);
    if (!result_kinds || !result_seqs) {
        if (result_kinds) enif_free(result_kinds);
        if (result_seqs) enif_free(result_seqs);
        return make_error(env, "alloc failed");
    }

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        enif_free(result_kinds);
        enif_free(result_seqs);
        return make_error(env, "database closed");
    }
    if (!kv_s->stmt || !keyref_s->stmt || !oplog_s->stmt) {
        enif_mutex_unlock(conn->mutex);
        enif_free(result_kinds);
        enif_free(result_seqs);
        return make_error(env, "statement finalized");
    }

    int rc = begin_immediate(conn->db);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        enif_mutex_unlock(conn->mutex);
        enif_free(result_kinds);
        enif_free(result_seqs);
        return err;
    }

    while (enif_get_list_cell(env, list, &head, &list)) {
        ErlNifBinary key_bin;
        ErlNifBinary value_bin;
        ErlNifSInt64 timestamp = 0;
        ErlNifSInt64 expires_at = 0;
        ErlNifSInt64 deleted_at = 0;
        int has_value = 0;
        int has_expires_at = 0;
        int has_deleted_at = 0;
        int bind_rc;
        int changes = 0;

        if (!get_local_batch_entry(
                env,
                head,
                &key_bin,
                &has_value,
                &value_bin,
                &timestamp,
                &has_expires_at,
                &expires_at,
                &has_deleted_at,
                &deleted_at
            )) {
            sqlite3_reset(kv_s->stmt);
            sqlite3_reset(keyref_s->stmt);
            sqlite3_reset(oplog_s->stmt);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(result_kinds);
            enif_free(result_seqs);
            return enif_make_badarg(env);
        }

        result_kinds[idx] = 0;
        result_seqs[idx] = 0;

        if (reject_cas_managed) {
            sqlite3_stmt *cas_stmt = NULL;
            int step_rc = SQLITE_OK;

            rc = ensure_cached_stmt(
                conn->db,
                &conn->cas_managed_key_check_stmt,
                "SELECT 1 FROM kv_paxos WHERE key = ?1 LIMIT 1"
            );
            if (rc != SQLITE_OK) {
                ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
                rollback_tx(conn->db);
                enif_mutex_unlock(conn->mutex);
                enif_free(result_kinds);
                enif_free(result_seqs);
                return err;
            }

            cas_stmt = conn->cas_managed_key_check_stmt;
            sqlite3_reset(cas_stmt);
            sqlite3_clear_bindings(cas_stmt);
            rc = sqlite3_bind_text(cas_stmt, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);
            if (rc == SQLITE_OK) {
                step_rc = sqlite3_step(cas_stmt);
            } else {
                step_rc = rc;
            }

            if (step_rc != SQLITE_ROW && step_rc != SQLITE_DONE) {
                ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
                sqlite3_reset(cas_stmt);
                sqlite3_clear_bindings(cas_stmt);
                rollback_tx(conn->db);
                enif_mutex_unlock(conn->mutex);
                enif_free(result_kinds);
                enif_free(result_seqs);
                return err;
            }

            sqlite3_reset(cas_stmt);
            sqlite3_clear_bindings(cas_stmt);

            if (step_rc == SQLITE_ROW) {
                result_kinds[idx] = 2;
                idx++;
                continue;
            }
        }

        sqlite3_int64 origin_seq = next_origin_seq + 1;

        sqlite3_reset(kv_s->stmt);
        sqlite3_clear_bindings(kv_s->stmt);
        bind_rc = sqlite3_bind_text(kv_s->stmt, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);
        if (bind_rc == SQLITE_OK) {
            bind_rc = has_value
                ? sqlite3_bind_text(kv_s->stmt, 2, (const char *)value_bin.data, (int)value_bin.size, SQLITE_TRANSIENT)
                : sqlite3_bind_null(kv_s->stmt, 2);
        }
        if (bind_rc == SQLITE_OK) bind_rc = sqlite3_bind_int64(kv_s->stmt, 3, (sqlite3_int64)timestamp);
        if (bind_rc == SQLITE_OK) bind_rc = sqlite3_bind_text(kv_s->stmt, 4, (const char *)origin_bin.data, (int)origin_bin.size, SQLITE_TRANSIENT);
        if (bind_rc == SQLITE_OK) bind_rc = sqlite3_bind_int64(kv_s->stmt, 5, origin_seq);
        if (bind_rc == SQLITE_OK) {
            bind_rc = has_expires_at
                ? sqlite3_bind_int64(kv_s->stmt, 6, (sqlite3_int64)expires_at)
                : sqlite3_bind_null(kv_s->stmt, 6);
        }
        if (bind_rc == SQLITE_OK) {
            bind_rc = has_deleted_at
                ? sqlite3_bind_int64(kv_s->stmt, 7, (sqlite3_int64)deleted_at)
                : sqlite3_bind_null(kv_s->stmt, 7);
        }
        if (bind_rc != SQLITE_OK) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_reset(kv_s->stmt);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(result_kinds);
            enif_free(result_seqs);
            return err;
        }

        rc = sqlite3_step(kv_s->stmt);
        if (rc != SQLITE_DONE) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_reset(kv_s->stmt);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(result_kinds);
            enif_free(result_seqs);
            return err;
        }
        sqlite3_reset(kv_s->stmt);
        changes = sqlite3_changes(conn->db);

        if (changes == 0) {
            idx++;
            continue;
        }

        int step_rc = SQLITE_OK;
        rc = bind_and_step_single_text(
            keyref_s->stmt,
            (const char *)key_bin.data,
            (int)key_bin.size,
            &step_rc
        );
        if (rc != SQLITE_OK) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_reset(keyref_s->stmt);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(result_kinds);
            enif_free(result_seqs);
            return err;
        }
        sqlite3_reset(keyref_s->stmt);

        sqlite3_reset(oplog_s->stmt);
        sqlite3_clear_bindings(oplog_s->stmt);
        bind_rc = sqlite3_bind_text(oplog_s->stmt, 1, (const char *)key_bin.data, (int)key_bin.size, SQLITE_TRANSIENT);
        if (bind_rc == SQLITE_OK) {
            bind_rc = has_value
                ? sqlite3_bind_text(oplog_s->stmt, 2, (const char *)value_bin.data, (int)value_bin.size, SQLITE_TRANSIENT)
                : sqlite3_bind_null(oplog_s->stmt, 2);
        }
        if (bind_rc == SQLITE_OK) bind_rc = sqlite3_bind_int64(oplog_s->stmt, 3, (sqlite3_int64)timestamp);
        if (bind_rc == SQLITE_OK) bind_rc = sqlite3_bind_text(oplog_s->stmt, 4, (const char *)origin_bin.data, (int)origin_bin.size, SQLITE_TRANSIENT);
        if (bind_rc == SQLITE_OK) bind_rc = sqlite3_bind_int64(oplog_s->stmt, 5, origin_seq);
        if (bind_rc == SQLITE_OK) {
            bind_rc = has_expires_at
                ? sqlite3_bind_int64(oplog_s->stmt, 6, (sqlite3_int64)expires_at)
                : sqlite3_bind_null(oplog_s->stmt, 6);
        }
        if (bind_rc == SQLITE_OK) {
            bind_rc = sqlite3_bind_int(oplog_s->stmt, 7, has_deleted_at ? 1 : 0);
        }
        if (bind_rc != SQLITE_OK) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_reset(oplog_s->stmt);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(result_kinds);
            enif_free(result_seqs);
            return err;
        }

        rc = sqlite3_step(oplog_s->stmt);
        if (rc != SQLITE_DONE) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_reset(oplog_s->stmt);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(result_kinds);
            enif_free(result_seqs);
            return err;
        }
        sqlite3_reset(oplog_s->stmt);

        next_origin_seq = origin_seq;
        result_kinds[idx] = 1;
        result_seqs[idx] = origin_seq;
        idx++;
    }

    if (next_origin_seq != (sqlite3_int64)starting_origin_seq) {
        rc = write_local_origin_seq(conn, next_origin_seq);
        if (rc != SQLITE_OK) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(result_kinds);
            enif_free(result_seqs);
            return err;
        }

        rc = write_local_origin_progress(
            conn,
            (const char *)origin_bin.data,
            (int)origin_bin.size,
            next_origin_seq
        );
        if (rc != SQLITE_OK) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            rollback_tx(conn->db);
            enif_mutex_unlock(conn->mutex);
            enif_free(result_kinds);
            enif_free(result_seqs);
            return err;
        }
    }

    rc = commit_tx(conn->db);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        enif_free(result_kinds);
        enif_free(result_seqs);
        return err;
    }

    enif_mutex_unlock(conn->mutex);

    ERL_NIF_TERM results_list = enif_make_list(env, 0);
    while (idx > 0) {
        ERL_NIF_TERM result_term;
        idx--;

        if (result_kinds[idx] == 1) {
            result_term =
                enif_make_tuple2(env, atom_applied, enif_make_int64(env, (ErlNifSInt64)result_seqs[idx]));
        } else if (result_kinds[idx] == 2) {
            result_term = atom_cas_managed_key;
        } else {
            result_term = atom_ignored;
        }

        results_list = enif_make_list_cell(env, result_term, results_list);
    }

    enif_free(result_kinds);
    enif_free(result_seqs);

    return enif_make_tuple3(
        env,
        atom_ok,
        results_list,
        enif_make_int64(env, (ErlNifSInt64)next_origin_seq)
    );
}

/* ------------------------------------------------------------------ */
/* NIF: write_snapshot_entry(db, kv_stmt, kv_args)                    */
/*   -> {:ok, true} | {:ok, false} | {:error, msg}                    */
/*                                                                     */
/* Full-sync apply updates current state only. It does not append to   */
/* kv_oplog or advance replay progress.                                */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_write_snapshot_entry(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    (void)argc;
    connection_t *conn;
    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    statement_t *kv_s;
    if (!enif_get_resource(env, argv[1], statement_type, (void **)&kv_s))
        return enif_make_badarg(env);

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "database closed");
    }
    if (!kv_s->stmt) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "statement finalized");
    }

    int rc = sqlite3_exec(conn->db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    int br = bind_args(env, kv_s->stmt, argv[2]);
    if (br != 0) {
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return (br == -1) ? enif_make_badarg(env)
                          : make_sqlite_error(env, conn->db);
    }

    rc = sqlite3_step(kv_s->stmt);
    if (rc != SQLITE_DONE) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_reset(kv_s->stmt);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }
    sqlite3_reset(kv_s->stmt);

    int changes = sqlite3_changes(conn->db);

    rc = sqlite3_exec(conn->db, "COMMIT", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    enif_mutex_unlock(conn->mutex);
    return enif_make_tuple2(env, atom_ok, changes > 0 ? atom_true : atom_false);
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
        for (int i = 0; i < ncols; i++) {
            if (!make_column(env, s->stmt, i, &cols[i])) {
                enif_free(cols);
                sqlite3_reset(s->stmt);
                enif_mutex_unlock(conn->mutex);
                return make_error(env, "alloc failed");
            }
        }
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
            for (int i = 0; i < ncols; i++) {
                if (!make_column(env, stmt, i, &cols[i])) {
                    enif_free(cols);
                    sqlite3_finalize(stmt);
                    enif_free(rows);
                    enif_mutex_unlock(conn->mutex);
                    return make_error(env, "alloc failed");
                }
            }
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
/* NIF: merge_local_progress_summary(db, [{origin_node, seq}...])      */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_merge_local_progress_summary(
    ErlNifEnv *env,
    int argc,
    const ERL_NIF_TERM argv[]
)
{
    (void)argc;
    connection_t *conn;
    sqlite3_stmt *stmt = NULL;

    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "database closed");
    }

    int rc = begin_immediate(conn->db);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    rc = sqlite3_prepare_v3(
        conn->db,
        "INSERT INTO kv_origin_progress (origin_node, last_seq) VALUES (?1, ?2) "
        "ON CONFLICT(origin_node) DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)",
        -1,
        0,
        &stmt,
        NULL
    );

    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    rc = step_progress_entries(env, stmt, argv[1], NULL, 0);
    sqlite3_finalize(stmt);

    if (rc == -1) {
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return enif_make_badarg(env);
    }

    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    rc = commit_tx(conn->db);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    enif_mutex_unlock(conn->mutex);
    return atom_ok;
}

/* ------------------------------------------------------------------ */
/* NIF: replace_local_progress_summary(db, [{origin_node, seq}...])    */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_replace_local_progress_summary(
    ErlNifEnv *env,
    int argc,
    const ERL_NIF_TERM argv[]
)
{
    (void)argc;
    connection_t *conn;
    sqlite3_stmt *stmt = NULL;

    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "database closed");
    }

    int rc = begin_immediate(conn->db);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    rc = sqlite3_exec(conn->db, "DELETE FROM kv_origin_progress", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    rc = sqlite3_prepare_v3(
        conn->db,
        "INSERT INTO kv_origin_progress (origin_node, last_seq) VALUES (?1, ?2)",
        -1,
        0,
        &stmt,
        NULL
    );

    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    rc = step_progress_entries(env, stmt, argv[1], NULL, 0);
    sqlite3_finalize(stmt);

    if (rc == -1) {
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return enif_make_badarg(env);
    }

    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    rc = commit_tx(conn->db);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    enif_mutex_unlock(conn->mutex);
    return atom_ok;
}

/* ------------------------------------------------------------------ */
/* NIF: replace_peer_progress(db, member_node, [{origin_node, seq}...]) */
/* ------------------------------------------------------------------ */

static ERL_NIF_TERM ekv_replace_peer_progress(
    ErlNifEnv *env,
    int argc,
    const ERL_NIF_TERM argv[]
)
{
    (void)argc;
    connection_t *conn;
    ErlNifBinary member_node;
    sqlite3_stmt *delete_stmt = NULL;
    sqlite3_stmt *insert_stmt = NULL;

    if (!enif_get_resource(env, argv[0], connection_type, (void **)&conn))
        return enif_make_badarg(env);

    if (!enif_inspect_iolist_as_binary(env, argv[1], &member_node))
        return enif_make_badarg(env);

    enif_mutex_lock(conn->mutex);
    if (!conn->db) {
        enif_mutex_unlock(conn->mutex);
        return make_error(env, "database closed");
    }

    int rc = begin_immediate(conn->db);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    rc = sqlite3_prepare_v3(
        conn->db,
        "DELETE FROM kv_member_progress WHERE member_node = ?1",
        -1,
        0,
        &delete_stmt,
        NULL
    );
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    sqlite3_bind_text(
        delete_stmt,
        1,
        (const char *)member_node.data,
        (int)member_node.size,
        SQLITE_TRANSIENT
    );

    rc = sqlite3_step(delete_stmt);
    sqlite3_finalize(delete_stmt);
    if (rc != SQLITE_DONE) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    rc = sqlite3_prepare_v3(
        conn->db,
        "INSERT INTO kv_member_progress (member_node, origin_node, last_seq) VALUES (?1, ?2, ?3)",
        -1,
        0,
        &insert_stmt,
        NULL
    );
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    rc = step_progress_entries(
        env,
        insert_stmt,
        argv[2],
        (const char *)member_node.data,
        (int)member_node.size
    );
    sqlite3_finalize(insert_stmt);

    if (rc == -1) {
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return enif_make_badarg(env);
    }

    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    rc = commit_tx(conn->db);
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        rollback_tx(conn->db);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    enif_mutex_unlock(conn->mutex);
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

        /* Check if there's a pending accepted state (acc_c > 0).
         * Accepted deletes have value=NULL but deleted_at!=NULL,
         * so we must not require value to be non-NULL. */
        if (accepted_c > 0) {
            has_accepted_value = 1;
            for (int i = 0; i < 5; i++) {
                if (!make_column(env, sel, 4 + i, &paxos_value_cols[i])) {
                    sqlite3_finalize(sel);
                    sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
                    enif_mutex_unlock(conn->mutex);
                    enif_free(ballot_n_str);
                    return make_error(env, "alloc failed");
                }
            }
        }
    }

    /* Copy accepted_n before finalizing (data owned by stmt) */
    char *accepted_n_copy = enif_alloc(accepted_n_len + 1);
    if (!accepted_n_copy) {
        sqlite3_finalize(sel);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return make_error(env, "alloc failed");
    }
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
        if (!prom_n_copy) {
            sqlite3_finalize(sel);
            sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
            enif_mutex_unlock(conn->mutex);
            enif_free(ballot_n_str);
            enif_free(accepted_n_copy);
            return make_error(env, "alloc failed");
        }
        memcpy(prom_n_copy, promised_n_str, prom_n_len_copy);
        prom_n_copy[prom_n_len_copy] = '\0';

        sqlite3_finalize(sel);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);

        ERL_NIF_TERM promised_n_term;
        {
            if (!make_binary_term(env, prom_n_copy, (size_t)prom_n_len_copy, &promised_n_term)) {
                enif_free(prom_n_copy);
                enif_free(ballot_n_str);
                enif_free(accepted_n_copy);
                return make_error(env, "alloc failed");
            }
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
            for (int i = 0; i < ncols && i < 5; i++) {
                if (!make_column(env, kv_sel, i, &cols[i])) {
                    sqlite3_finalize(kv_sel);
                    sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
                    enif_mutex_unlock(conn->mutex);
                    enif_free(ballot_n_str);
                    enif_free(accepted_n_copy);
                    return make_error(env, "alloc failed");
                }
            }
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
        if (!make_binary_term(env, accepted_n_copy, (size_t)accepted_n_len, &accepted_n_term)) {
            enif_free(ballot_n_str);
            enif_free(accepted_n_copy);
            return make_error(env, "alloc failed");
        }
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
            if (!enif_inspect_binary(env, val_elems[i], &bin)) {
                sqlite3_finalize(pax_ups);
                sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
                enif_mutex_unlock(conn->mutex);
                enif_free(ballot_n_str);
                return enif_make_badarg(env);
            }
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
/* NIF: paxos_promote(db, kv_force_stmt, keyref_stmt, oplog_stmt,     */
/*                    key,                                            */
/*                    ballot_c, ballot_n, origin_seq)                  */
/*   -> {:ok, value, ts, origin, expires, deleted, prev_value|nil, origin_seq, local_progress_seq}*/
/*   -> {:ok, :stale}                                                  */
/*   -> {:error, msg}                                                  */
/*                                                                     */
/* Single dirty IO bounce. Promotes accepted value from kv_paxos to    */
/* kv + oplog on commit confirmation. Keeps accepted state in kv_paxos.*/
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

    statement_t *keyref_s;
    if (!enif_get_resource(env, argv[2], statement_type, (void **)&keyref_s))
        return enif_make_badarg(env);

    statement_t *oplog_s;
    if (!enif_get_resource(env, argv[3], statement_type, (void **)&oplog_s))
        return enif_make_badarg(env);

    if (kv_s->conn != conn || keyref_s->conn != conn || oplog_s->conn != conn)
        return make_error(env, "statement does not belong to this connection");

    ErlNifBinary key_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[4], &key_bin))
        return enif_make_badarg(env);

    ErlNifSInt64 ballot_c;
    if (!enif_get_int64(env, argv[5], &ballot_c))
        return enif_make_badarg(env);

    ErlNifBinary ballot_n_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[6], &ballot_n_bin))
        return enif_make_badarg(env);

    sqlite3_int64 origin_seq = 0;
    sqlite3_int64 local_progress_seq = 0;
    int origin_seq_provided = !enif_is_identical(argv[7], atom_nil);
    if (origin_seq_provided) {
        if (!enif_get_int64(env, argv[7], (ErlNifSInt64 *)&origin_seq))
            return enif_make_badarg(env);
    }

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
    if (!kv_s->stmt || !keyref_s->stmt || !oplog_s->stmt) {
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

    if (!origin_seq_provided) {
        rc = next_local_origin_seq(conn, &origin_seq);
        if (rc != SQLITE_OK) {
            ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
            sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
            enif_mutex_unlock(conn->mutex);
            enif_free(ballot_n_str);
            return err;
        }
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
    ERL_NIF_TERM value_term;
    ERL_NIF_TERM ts_term;
    ERL_NIF_TERM origin_term;
    ERL_NIF_TERM expires_term;
    ERL_NIF_TERM deleted_term;
    if (!make_column(env, sel, 2, &value_term) ||
        !make_column(env, sel, 3, &ts_term) ||
        !make_column(env, sel, 4, &origin_term) ||
        !make_column(env, sel, 5, &expires_term) ||
        !make_column(env, sel, 6, &deleted_term)) {
        sqlite3_finalize(sel);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return make_error(env, "alloc failed");
    }

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
    if (!copy_alloc(value_data, (size_t)value_len, &value_copy)) {
        sqlite3_finalize(sel);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return make_error(env, "alloc failed");
    }
    char *origin_copy = NULL;
    if (!copy_alloc(origin_data, (size_t)origin_len, (void **)&origin_copy)) {
        if (value_copy) enif_free(value_copy);
        sqlite3_finalize(sel);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        enif_free(ballot_n_str);
        return make_error(env, "alloc failed");
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
            if (!make_column(env, prev_sel, 0, &prev_value)) {
                sqlite3_finalize(prev_sel);
                if (value_copy) enif_free(value_copy);
                if (origin_copy) enif_free(origin_copy);
                enif_free(ballot_n_str);
                sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
                enif_mutex_unlock(conn->mutex);
                return make_error(env, "alloc failed");
            }
        }
        sqlite3_finalize(prev_sel);
    }

    /* 5. Bind + step kv_force_upsert: [key, value, ts, origin, origin_seq, expires, deleted] */
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
    sqlite3_bind_int64(kv_s->stmt, 5, origin_seq);
    if (has_expires)
        sqlite3_bind_int64(kv_s->stmt, 6, expires_at);
    else
        sqlite3_bind_null(kv_s->stmt, 6);
    if (has_deleted)
        sqlite3_bind_int64(kv_s->stmt, 7, deleted_at);
    else
        sqlite3_bind_null(kv_s->stmt, 7);

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

    /* 6. Ensure keyref exists and marks current-state presence. */
    int step_rc = SQLITE_OK;
    rc = bind_and_step_single_text(
        keyref_s->stmt,
        (const char *)key_bin.data,
        (int)key_bin.size,
        &step_rc
    );
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        sqlite3_reset(keyref_s->stmt);
        if (value_copy) enif_free(value_copy);
        if (origin_copy) enif_free(origin_copy);
        enif_free(ballot_n_str);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }
    sqlite3_reset(keyref_s->stmt);

    /* 7. Bind + step oplog_insert: [key, value, ts, origin, origin_seq, expires, is_delete] */
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
    sqlite3_bind_int64(oplog_s->stmt, 5, origin_seq);
    if (has_expires)
        sqlite3_bind_int64(oplog_s->stmt, 6, expires_at);
    else
        sqlite3_bind_null(oplog_s->stmt, 6);
    sqlite3_bind_int64(oplog_s->stmt, 7, has_deleted ? 1 : 0);  /* is_delete */

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

    if (!origin_seq_provided) {
        rc = write_local_origin_progress(
            conn,
            origin_copy,
            origin_len,
            origin_seq
        );
        local_progress_seq = origin_seq;
    } else {
        rc = advance_local_origin_progress(
            conn,
            origin_copy,
            origin_len,
            origin_seq,
            &local_progress_seq
        );
    }
    if (rc != SQLITE_OK) {
        ERL_NIF_TERM err = make_sqlite_error(env, conn->db);
        if (value_copy) enif_free(value_copy);
        if (origin_copy) enif_free(origin_copy);
        enif_free(ballot_n_str);
        sqlite3_exec(conn->db, "ROLLBACK", NULL, NULL, NULL);
        enif_mutex_unlock(conn->mutex);
        return err;
    }

    /* 7. COMMIT */
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

    /* {:ok, value, timestamp, origin, expires_at, deleted_at, prev_value_or_nil, origin_seq, local_progress_seq} */
    return enif_make_tuple(
        env,
        9,
        atom_ok,
        value_term,
        ts_term,
        origin_term,
        expires_term,
        deleted_term,
        prev_value,
        enif_make_int64(env, (ErlNifSInt64)origin_seq),
        enif_make_int64(env, (ErlNifSInt64)local_progress_seq)
    );
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
    {"ekv_write_entry",   8, ekv_write_entry,   ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_write_entries_batch", 6, ekv_write_entries_batch, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_write_local_entries_batch", 8, ekv_write_local_entries_batch, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_write_snapshot_entry", 3, ekv_write_snapshot_entry, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_read_entry",    3, ekv_read_entry,    ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_fetch_all",     3, ekv_fetch_all,     ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_backup",        2, ekv_backup,        ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_merge_local_progress_summary", 2, ekv_merge_local_progress_summary,
        ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_replace_local_progress_summary", 2, ekv_replace_local_progress_summary,
        ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_replace_peer_progress", 3, ekv_replace_peer_progress,
        ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_paxos_prepare", 4, ekv_paxos_prepare, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_paxos_accept",  5, ekv_paxos_accept,  ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ekv_paxos_promote", 8, ekv_paxos_promote, ERL_NIF_DIRTY_JOB_IO_BOUND},
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
    atom_applied = make_atom(env, "applied");
    atom_ignored = make_atom(env, "ignored");
    atom_cas_managed_key = make_atom(env, "cas_managed_key");
    atom_promise = make_atom(env, "promise");
    atom_nack    = make_atom(env, "nack");
    atom_stale   = make_atom(env, "stale");

    return 0;
}

ERL_NIF_INIT(Elixir.EKV.Sqlite3NIF, nif_funcs, on_load, NULL, NULL, NULL)
