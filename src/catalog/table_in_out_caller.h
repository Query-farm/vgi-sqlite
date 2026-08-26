// © Copyright 2026 Query Farm LLC - https://query.farm
//
// TableInOutCaller: drives one VGI "blended"/RowTransformFunction
// table_in_out function's bind -> init(phase="INPUT") -> exchange* ->
// close lifecycle, correlated per outer row (SQLite's own json_each(t.x)-
// style table-valued-function mechanism - see vtab/vgi_table_in_out_vtab.h
// for the full design this class is the catalog/RPC half of).
//
// Ported from the DuckDB extension's client design (research done against
// vgi's src/vgi_table_in_out_impl.cpp/vgi_function_connection.cpp before
// writing this - see the plan file's Milestone 9 notes for the full
// research writeup), scoped down to exactly the "blended" shape:
// FunctionInfo.input_from_args=true (the function's declared positional
// arguments ARE its per-row input columns - no relation-valued/TABLE-typed
// argument at all, which SQLite's table-valued-function calling
// convention has no way to express) and has_finalize=false (guaranteed by
// the worker's own resolve_metadata for this shape, per vgi-python's
// table_in_out_function.py - a blended function can never declare a
// finalize stage, so this class never drives one).
//
// Lifetime: bind ONCE, init ONCE, then Exchange() called once per
// correlated outer row for as long as this caller is alive - unlike
// TableWriter (a fresh bind/init/exchange/close round trip per call, since
// each write is logically independent) or ScalarFunctionCaller (fresh
// bind/init/exchange per call, same reasoning), this class holds ONE
// connection checkout and ONE open exchange stream for its WHOLE
// lifetime, matching the DuckDB client's own "retain BindResult, don't
// re-bind per row - bind is expensive relative to exchange" design intent
// (vgi_table_in_out_impl.cpp's comments, per the research pass). In this
// driver, one TableInOutCaller instance's lifetime is exactly one SQLite
// cursor's lifetime (constructed lazily on that cursor's first xFilter,
// destroyed in xClose) - SQLite reuses the same cursor object across every
// xFilter call in a correlated scan (one call per outer row), never
// reopening a fresh cursor per row, so this maps cleanly onto "persist
// the connection across every xFilter this cursor sees."
//
// Wire-protocol facts this class's Exchange() relies on (see the plan
// file's Milestone 9 research notes for the full citations):
//   - Exactly one WriteInputBatch (one input row) per Exchange() call,
//     always answered by exactly one ReadDataBatch response - a strict
//     1:1 lockstep, never more or fewer.
//   - That one response batch may hold 0, 1, or many output rows -
//     there's no second round trip to get "more of the same input row's
//     output"; the caller (xFilter's cursor) buffers the whole batch and
//     drains it row-by-row before the next xFilter/Exchange() call.
//   - A present-but-0-row response is NOT end-of-stream (it just means
//     this particular input row produced no output) - the connection
//     stays open for more input either way.
//   - The worker must never close the stream (VgiStream::Exchange()
//     returning nullopt) while this caller is still feeding it more input
//     rows - that's a protocol violation, not a normal termination
//     signal, and Exchange() throws rather than silently treating it as
//     an empty result.
//   - Never send pushdown_filters on this function's InitRequest (SQLite's
//     own outer-loop correlation already does all the "filtering" that
//     matters - each Exchange() call's input row IS the constraint), and
//     never send projection_ids either for v1 (see the .cpp file comment
//     on why projection pushdown is scoped out for now even though
//     FunctionInfo.projection_pushdown is decoded and available).
//
// NAMED vs POSITIONAL declared arguments (VGI_ARG_KEY="vgi_arg"/
// VGI_ARG_NAMED="named" field metadata, vgi-python's argument_spec.py) -
// found via a real end-to-end mismatch against a live worker (open-meteo,
// a Cloudflare Worker), not from the protocol docs alone: a declared
// argument's `vgi_arg: named` metadata (VGI's general model for a DuckDB
// `:=`-style keyword argument, not table_in_out-specific) means its VALUE
// belongs in BindRequest's `arguments` struct - read by the worker as
// `params.args` - not as a per-row streaming column at all. The initial
// version of this class put every declared argument, named or not, into
// input_schema and always sent an empty arguments struct - which doesn't
// error, doesn't warn, just silently means the worker's own `process()`
// never sees a named argument's value and falls back to its own default
// (confirmed: `forecast_hourly(..., temperature_unit := 'fahrenheit')`
// against the real worker returned identical values whether 'fahrenheit'
// or 'celsius' was supplied - the worker was never told either way).
// Fixed by splitting input_schema (computed once in the constructor, from
// each field's own metadata) into `stream_schema_` (true positional
// fields - what actually flows through Exchange(), unchanged) and the
// named subset (extracted from the FIRST real Exchange() call's row and
// sent as this caller's bind()-time `arguments`, matching
// TableScanner::WrapAsArgsStruct's "positional_N"/"named_<name>"-prefixed
// struct-field wire convention - see this file's own .cpp for the
// `named_<name>` variant). This means a named argument's value is fixed
// for this caller's WHOLE lifetime (one bind, reused across every later
// Exchange() call on the same cursor) - a caller supplying a DIFFERENT
// named-arg value on a later correlated row has no way to make it take
// effect, which matches VGI's own bind-time-fixed semantics for named
// arguments generally (not a limitation specific to this class).
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/record_batch.h>

#include "rpc/vgi_stream.h"
#include "vtab/connection_pool.h"

namespace vgi_sqlite {

class TableInOutCaller {
public:
    // `input_schema` is the function's FULL declared argument schema
    // (CatalogTableInOutFunction::input_schema, both positional and
    // named fields together, in declared order) - field names/types the
    // vtab's HIDDEN columns are built from, and what every Exchange()
    // call's input_row parameter must match exactly (the vtab's xFilter
    // builds it that way from argv). Internally split once, in the
    // constructor, into `stream_schema_` (true positional fields - what
    // actually flows through the exchange stream) and the named subset
    // (routed to bind()'s `arguments` instead) - see the file comment.
    TableInOutCaller(ConnectionPool& pool, std::string location, std::string catalog_name,
                     std::string schema_name, std::string function_name,
                     std::shared_ptr<arrow::Schema> input_schema);

    // Best-effort stream close + connection release, mirroring
    // TableScanner's destructor - runs even if this caller was only ever
    // used for its output_schema() (a throwaway xConnect-time probe) or
    // if an xFilter-driven Exchange() call left the stream mid-scan (a
    // SQLite error can abandon a cursor without a clean xClose either).
    ~TableInOutCaller();

    TableInOutCaller(const TableInOutCaller&) = delete;
    TableInOutCaller& operator=(const TableInOutCaller&) = delete;

    // Forces bind() to happen now (idempotent past the first call, using
    // an EMPTY arguments struct - no real named-arg values are available
    // yet at this call site) and returns the worker-resolved output
    // schema - the real one, not FunctionInfo.output_schema (empty/
    // not-authoritative for a table-shaped function per vgi-c++'s own
    // encode_table_function_info comment: "A table function's output
    // schema is settled at bind"). Used by the vtab's xConnect/xCreate to
    // declare its DDL before any cursor/row (and hence no real named-arg
    // values) exists; a cursor's own caller never calls this directly -
    // its first Exchange() call does the REAL bind, threading that row's
    // actual named-arg values through (see Exchange()'s own comment).
    const std::shared_ptr<arrow::Schema>& Bind();

    // Sends `input_row` (must match the FULL declared input_schema this
    // caller was constructed with - the vtab's xFilter is responsible for
    // building it that way from argv) as this call's one input row and
    // returns the worker's response batch (0+ rows, output_schema()'s
    // schema) - see the file comment on the exact 1:1 lockstep/never-EOS-
    // mid-scan contract. On the FIRST call, binds using THIS row's actual
    // named-argument values (see the file comment on why - discards
    // whatever Bind() may have already probed, since that used an empty
    // arguments struct); every later call just exchanges its own row's
    // stream (positional-only) subset on the already-open stream, ignoring
    // that later row's named-argument values entirely (they can't take
    // effect after bind - also see the file comment). Throws on a
    // protocol violation (the stream closing mid-scan) or an RPC failure.
    std::shared_ptr<arrow::RecordBatch> Exchange(const std::shared_ptr<arrow::RecordBatch>& input_row);

private:
    // Binds (if not already bound) with `args_bytes` as BindRequest's
    // arguments and `stream_schema_` as its input_schema, then opens the
    // exchange stream - shared by Bind() (empty args, probe-only) and
    // Exchange()'s first-call path (real args, from the row's own named
    // columns).
    const std::shared_ptr<arrow::Schema>& EnsureBoundWithArgs(std::vector<uint8_t> args_bytes);

    ConnectionPool& pool_;
    std::string location_;
    std::string catalog_name_;
    std::string schema_name_;
    std::string function_name_;
    std::shared_ptr<arrow::Schema> input_schema_;  // full declared schema (positional + named)

    // Computed once in the constructor from input_schema_'s own field
    // metadata (VGI_ARG_KEY="vgi_arg"=="named") - see the file comment.
    std::shared_ptr<arrow::Schema> stream_schema_;  // positional-only subset, in original relative order
    std::vector<int> stream_field_indices_;         // indices into input_schema_ that are positional
    std::vector<int> named_field_indices_;          // indices into input_schema_ that are named

    bool bound_ = false;
    std::optional<ConnectionPool::Checkout> checkout_;
    std::unique_ptr<VgiStream> stream_;
    std::shared_ptr<arrow::Schema> output_schema_;
};

}  // namespace vgi_sqlite
