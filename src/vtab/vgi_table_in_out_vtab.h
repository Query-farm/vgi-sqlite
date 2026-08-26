// © Copyright 2026 Query Farm LLC - https://query.farm
//
// vgi_table_in_out: the sqlite3_module backing every blended table_in_out
// (RowTransformFunction) function vgi_attach() registers - the
// per-row-correlated table-valued-function mapping this driver's
// CLAUDE.md/plan file originally called out-of-scope ("SQLite has no
// table-function-taking-a-relation concept"), revisited once it became
// clear the real, narrower gap is "no relation-valued argument", not "no
// per-row correlation at all" (SQLite's own `FROM t, json_each(t.x)`
// support - no LATERAL keyword needed - already does exactly that via
// ordinary xBestIndex/xFilter constraint pushdown).
//
// CREATE VIRTUAL TABLE "<catalog>_<function>" USING vgi_table_in_out(
//     location='<worker argv>', catalog='<name>', schema='<schema>',
//     function='<function>');
//
// Declared schema: the function's real (bind-resolved) OUTPUT columns
// first, then one HIDDEN column per declared INPUT argument, in argument
// order - SQLite's table-valued-function calling convention binds a call
// site's positional arguments against a table's HIDDEN columns in
// declaration order (sqlite.org/vtab.html's "Table-Valued Functions"
// section), which is what makes `SELECT * FROM name(1, 2)` and, more to
// the point, `SELECT * FROM t, name(t.x, t.y)` (correlated - SQLite
// re-invokes xFilter once per outer row exactly like json_each(t.x))
// both work with zero extra plumbing beyond an ordinary vtab.
//
// Deliberately out of scope for v1, all documented rather than silently
// missing (see vgi_table_in_out_vtab.cpp's file comment for the full
// reasoning on each):
//   - Projection pushdown (every output column is always fetched, even
//     when FunctionInfo.projection_pushdown says the worker could narrow
//     it - CatalogTableInOutFunction::projection_pushdown is decoded and
//     carried through for a future pass, just not acted on yet).
//   - WHERE-constraint pushdown on the OUTPUT columns (an ordinary
//     unclaimed constraint, re-verified by SQLite itself as always).
//   - INSERT/UPDATE/DELETE (this vtab is read-only - a table_in_out
//     function is a computation, not writable storage).
//   - Transactions (xBegin/xSync/xCommit/xRollback all null - this vtab
//     performs no writes and needs no read-consistency coordination
//     beyond what one bind()/exchange() sequence already gives a single
//     query; see the .cpp file comment).
//   - The classic (TABLE-typed-argument) table_in_out shape and plain
//     no-input table functions (e.g. split_sequence) - neither is
//     representable as a fixed set of HIDDEN scalar columns the way the
//     blended/RowTransformFunction shape is; see
//     CatalogTableInOutFunction's own file comment in catalog_client.h.
//   - A function whose OUTPUT schema shares a column name with one of its
//     own declared arguments - the DDL this module's xConnect/xCreate
//     builds declares both in one `CREATE TABLE`, and SQLite rejects a
//     duplicate column name outright. Not a crash: `sqlite3_declare_vtab`
//     fails cleanly, and vgi_attach() already treats any one function's
//     CREATE VIRTUAL TABLE failure as a per-function skip (logged, not
//     fatal to the rest of the attach) - same handling as an ordinary
//     table's xConnect failure. Not worth disambiguating (e.g. renaming
//     colliding hidden columns) automatically for v1 - a real function
//     hitting this can just pick a non-colliding argument name.
#pragma once

struct sqlite3;
struct sqlite3_module;

namespace vgi_sqlite {

class ConnectionPool;

// Registers the "vgi_table_in_out" module on `db`, sharing `pool` with
// whatever else already owns it (RegisterVgiWorkerModule's own pool, via
// extension.cpp - one ConnectionPool per db connection, not one per
// module, matching every other vtab/function registration in this
// driver).
int RegisterVgiTableInOutModule(sqlite3* db, ConnectionPool* pool);

}  // namespace vgi_sqlite
