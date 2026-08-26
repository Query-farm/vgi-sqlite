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
#pragma once

#include <memory>
#include <optional>
#include <string>

#include <arrow/record_batch.h>

#include "rpc/vgi_stream.h"
#include "vtab/connection_pool.h"

namespace vgi_sqlite {

class TableInOutCaller {
public:
    // `input_schema` is the function's declared argument schema
    // (CatalogTableInOutFunction::input_schema) - field names/types this
    // caller sends verbatim as InitRequest's input_schema and as every
    // Exchange() call's 1-row input batch's schema. Binding doesn't
    // happen until the first Exchange() call (EnsureBound(), lazy - a
    // caller only used to probe output_schema, e.g. the vtab's own
    // xConnect/xCreate schema-declaration step, still needs to force it -
    // see Bind()).
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

    // Forces bind() to happen now (idempotent past the first call) and
    // returns the worker-resolved output schema - the real one, not
    // FunctionInfo.output_schema (empty/not-authoritative for a
    // table-shaped function per vgi-c++'s own encode_table_function_info
    // comment: "A table function's output schema is settled at bind").
    // Used by the vtab's xConnect/xCreate to declare its DDL before any
    // cursor/row exists; a cursor's own caller calls this implicitly via
    // its first Exchange() rather than calling it separately.
    const std::shared_ptr<arrow::Schema>& Bind();

    // Sends `input_row` (must match input_schema's schema exactly - the
    // vtab's xFilter is responsible for building it that way from argv)
    // as this call's one input row and returns the worker's response
    // batch (0+ rows, output_schema()'s schema) - see the file comment on
    // the exact 1:1 lockstep/never-EOS-mid-scan contract. Binds lazily on
    // the first call. Throws on a protocol violation (the stream closing
    // mid-scan) or an RPC failure.
    std::shared_ptr<arrow::RecordBatch> Exchange(const std::shared_ptr<arrow::RecordBatch>& input_row);

private:
    ConnectionPool& pool_;
    std::string location_;
    std::string catalog_name_;
    std::string schema_name_;
    std::string function_name_;
    std::shared_ptr<arrow::Schema> input_schema_;

    bool bound_ = false;
    std::optional<ConnectionPool::Checkout> checkout_;
    std::unique_ptr<VgiStream> stream_;
    std::shared_ptr<arrow::Schema> output_schema_;
};

}  // namespace vgi_sqlite
