// © Copyright 2026 Query Farm LLC - https://query.farm
//
// vgi_worker: the sqlite3_module backing every table vgi_attach() creates.
//
// CREATE VIRTUAL TABLE "schema.table" USING vgi_worker(
//     location='<worker argv, space-separated>', catalog='<name>',
//     schema='<schema>', table='<table>');
//
// xConnect resolves the (location, catalog)'s shared ConnectionPool entry
// (spawning + attaching on first use, reused by every other table from
// the same pair - see connection_pool.h) and calls catalog_table_get to
// declare the schema, so the table is queryable immediately after CREATE
// VIRTUAL TABLE, at the cost of one worker round trip per table the first
// time it's (re)connected (once per db-handle lifetime, not per query).
// Deferring even that - e.g. by baking the column list into the CREATE
// VIRTUAL TABLE arguments at vgi_attach() time so xConnect needs no
// network call at all - is a documented follow-up (see the plan's
// Production Hardening phase), not done here.
//
// MVP: full scan only, no xBestIndex pushdown yet.
#pragma once

struct sqlite3;
struct sqlite3_module;

namespace vgi_sqlite {

class ConnectionPool;

// Registers the "vgi_worker" module on `db`, owning a new ConnectionPool
// as the module's client data (destroyed when the module is torn down -
// see sqlite3_create_module_v2's destructor argument). Returns the pool
// so the caller can share it with other registrations on the same `db`
// (e.g. vgi_attach(), so its discovery connection isn't yet another
// spawned worker), or null on failure.
ConnectionPool* RegisterVgiWorkerModule(sqlite3* db);

}  // namespace vgi_sqlite
