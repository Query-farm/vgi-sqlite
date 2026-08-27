// © Copyright 2026 Query Farm LLC - https://query.farm
//
// vgi_table_function: the sqlite3_module backing every plain, standalone
// table (generator) function vgi_attach() registers - a genuine SQL
// table-valued-function call (`SELECT * FROM catalog_fn(args)`, literal
// or per-outer-row correlated, same HIDDEN-column call-syntax mechanism
// vgi_table_in_out already uses - see that module's own file comment for
// why SQLite needs no LATERAL keyword for this) for a function that takes
// NO per-row streaming input at all (unlike table_in_out) - the shape
// `split_sequence` and similar functions use
// (`TableFunctionGenerator`/`CatalogPlainTableFunction` - see
// catalog_client.h's file comment on the exact discovery filter).
//
// CREATE VIRTUAL TABLE "<catalog>_<function>" USING vgi_table_function(
//     location='<worker argv>', catalog='<name>', schema='<schema>',
//     function='<function>');
//
// Architecturally this is much closer to vgi_worker (src/vtab/vgi_vtab.h)
// than to vgi_table_in_out: a plain table function's whole lifecycle is
// TableScanner's ordinary bind -> init -> producer-tick scan, identical to
// scanning a real table's own backing scan_function - the ONLY difference
// is that the arguments driving that bind() come from THIS call's own
// HIDDEN-column values (built fresh per xFilter call, matching
// tools/vgi-split-probe.cpp's own hand-built proof of this exact call
// shape) instead of being inlined once by TableInfo. So, unlike
// TableInOutCaller (which binds ONCE and holds one exchange stream across
// a cursor's WHOLE lifetime - see table_in_out_caller.h's file comment on
// why that's required there), this module needs no persistent per-cursor
// caller at all: xFilter constructs a fresh TableScanner and does its own
// bind+init, exactly like vgi_vtab.cpp's own xFilter already does for a
// real table - a correlated scan's LATER xFilter calls (one per outer
// row) each get their own independent scan, no shared state required
// between them.
//
// Declared schema: the function's real (bind()-resolved) OUTPUT columns
// first, then one HIDDEN column per declared ARGUMENT, in declared order -
// see vgi_table_in_out_vtab.h's own file comment for the shared reasoning
// (positional call-syntax binding, output/hidden name-collision
// disambiguation - this module hits the identical DDL-declaration
// concerns table_in_out already solved, and reuses the exact same fix).
//
// Deliberately out of scope for v1, matching vgi_table_in_out's own scope
// decisions for the identical reasons (see that module's file comment for
// the full rationale on each): no projection/filter pushdown on the
// output side, no INSERT/UPDATE/DELETE (this vtab is read-only), no
// transaction coordination (xBegin/etc. all null).
//
// Splits (CatalogPlainTableFunction::supports_splits): honored
// transparently - TableScanner already plans/redeems splits internally
// whenever the bound ScanFunction says so (see catalog_table_plan.h);
// this module's xFilter needs no split-specific code at all, exactly like
// vgi_vtab.cpp's own xFilter doesn't.
//
// Not yet handled: arity overloading (two functions sharing one SQL name
// with different argument counts, e.g. vgi-python's `geo_encode`/
// `geo_encode3` fixtures) - vgi_attach() would try to CREATE VIRTUAL TABLE
// twice under the same generated name for two such functions; the second
// attempt fails (a name collision) and is skipped exactly like any other
// per-function registration failure, not a crash, but only ONE of the two
// overloads ends up actually callable. A real, documented gap - not
// attempted here.
#pragma once

struct sqlite3;
struct sqlite3_module;

namespace vgi_sqlite {

class ConnectionPool;

// Registers the "vgi_table_function" module on `db`, sharing `pool` with
// whatever else already owns it (mirrors vgi_table_in_out_vtab.h's
// RegisterVgiTableInOutModule - this module doesn't own/destroy the pool
// itself either).
int RegisterVgiTableFunctionModule(sqlite3* db, ConnectionPool* pool);

}  // namespace vgi_sqlite
