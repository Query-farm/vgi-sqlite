// © Copyright 2026 Query Farm LLC - https://query.farm
//
// ScalarFunctionCaller: calls a VGI scalar function via VGI's "exchange
// mode" (client writes an input batch, worker returns the matching output
// batch - distinct from table functions' producer mode, which this repo's
// TableScanner drives).
//
// Acquires its own ConnectionPool checkout for every single call - bind,
// init, and the exchange itself all happen on that one freshly-acquired
// connection, then it's released back to the pool - rather than sharing a
// connection with (or caching bind results across calls that might land
// on a different physical connection than) any table scan. Two real bugs
// were found by testing before landing on this design:
//
//   1. A persistent exchange stream held open across calls, on the same
//      *shared* connection every vgi_worker table from the same
//      (location, catalog) also used, collided with a concurrent table
//      scan's own open producer stream ("raw RPC client already has an
//      active call") - vgi_rpc::RpcClient allows exactly one live call per
//      connection. Found via `SELECT scalar_fn(col) FROM some_table`
//      (evaluated once per row, interleaved with the table's own stream).
//   2. Fixing that by simply open-and-closing a stream per call, but still
//      on that same shared connection, only moved the collision: the
//      table scan's producer stream is open for the *entire* scan (every
//      xNext up to eof), not just around one call, so a scalar function
//      evaluated mid-scan still collided with it. Confirmed by re-running
//      the same query after the first fix and seeing the identical error,
//      just attributed to the table's `init` instead of the function's.
//
// The real fix is architectural, not a smarter stream discipline: this
// caller must never contend with a table scan (or another concurrent
// scalar caller) for the *same physical connection* at all - see
// connection_pool.h's file comment for why ConnectionPool now hands out
// per-use checkouts instead of one shared connection per (location,
// catalog). Binding fresh on every call (rather than caching a bind
// result from one connection and reusing it on whatever connection a
// later call happens to acquire) is the deliberately conservative choice
// here: it doesn't depend on whether a worker's bind-returned opaque data
// is safe to replay against a *different* physical connection/process
// than the one that produced it - a question this driver doesn't rely on
// an answer to. The extra bind round trip per call is the accepted cost,
// matching the plan's "ship simple per-call round trips first" scope
// decision - revisit (e.g. caching per-connection bind results, reused
// only when a call happens to reacquire that same connection) only if
// profiling shows this is a real bottleneck.
//
// Argument types are inferred fresh from *every call's own actual values*,
// not from catalog metadata: FunctionInfo.arguments (the catalog-declared
// argument schema) turned out to carry Arrow's null/"any" type for a
// function like vgi-fixture-worker's add_values(a, b) - VGI resolves the
// real per-argument types dynamically per call site, the same way
// vgi's DuckDB extension builds its own BindRequest.input_schema from the
// caller's actually-bound argument types rather than any catalog
// declaration (VgiScalarFunctionBind, src/vgi_scalar_function_impl.cpp in
// that repo) - not documented anywhere read ahead of time; found by
// testing. An earlier version of this caller locked that schema in from
// the first call and CastTo'd every later call's arguments into it -
// which sounds conservative but was a real, silent correctness bug:
// Arrow's Scalar::CastTo between compatible-but-different numeric types
// (e.g. double -> int64) *succeeds* with silent truncation rather than
// failing, so a genuinely polymorphic function called with different
// argument types across a session (e.g. add_values(1, 2) followed by
// add_values(1.5, 2.5)) got its later call's real values quietly
// truncated to the first call's locked types instead of throwing or
// re-resolving - confirmed against vgi's own sqllogictest corpus
// (test/sql/integration/scalar/add_values.test): 1.5/2.5 truncated to
// 1/2 before ever reaching the worker, computing 3.0 instead of 4.0.
// Recomputing the schema unconditionally on every call (this file's
// current behavior) fixes that with no offsetting performance cost to
// weigh against: a full bind/init/exchange RPC round trip already
// happens fresh on every single Call() regardless (see above) - there
// was never a cheaper path being protected by caching the schema alone.
//
// Scoped to plain (non-const) positional arguments only for now: every
// argument becomes a per-row exchange column named "col_N" (VGI's fixed
// convention - never the SQL argument's own name), and
// BindRequest.arguments carries the empty (0-field) const-args struct,
// since no argument here is bound as a constant. A function that declares
// ConstParam/named arguments isn't callable through this yet - a
// documented gap, not silently wrong (registration in extension.cpp skips
// functions this can't represent).
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "vtab/connection_pool.h"

namespace vgi_sqlite {

class ScalarFunctionCaller {
public:
    // `num_args` is fixed at construction (known reliably from
    // FunctionInfo.arguments.num_fields(), even when the per-argument
    // types themselves aren't) - it's what SQLite's own
    // sqlite3_create_function_v2 needs up front. Argument *types* are
    // re-derived fresh from each Call()'s own actual values (see the file
    // comment). `pool`/`location`/`catalog_name` identify which (location,
    // catalog) to Acquire() a fresh checkout from on every call - see the
    // file comment on why this caller never holds one connection across
    // calls the way TableScanner does.
    ScalarFunctionCaller(ConnectionPool& pool, std::string location, std::string catalog_name,
                          std::string function_name, int num_args,
                          std::optional<std::string> schema_name);

    int num_args() const { return num_args_; }

    // Acquires a fresh connection, binds and calls on it, releases it -
    // every single call (see the file comment on why). `args` must have
    // exactly num_args() elements. Argument types are re-inferred fresh
    // from this call's own actual values every time (see the file
    // comment).
    std::shared_ptr<arrow::RecordBatch> Call(const std::vector<std::shared_ptr<arrow::Scalar>>& args);

private:
    ConnectionPool& pool_;
    std::string location_;
    std::string catalog_name_;
    std::string function_name_;
    int num_args_;
    std::optional<std::string> schema_name_;
};

}  // namespace vgi_sqlite
