// © Copyright 2026 Query Farm LLC - https://query.farm
//
// TableWriter: drives one VGI table-in-out write call (insert, update, or
// delete - the bind/init/exchange lifecycle is identical for all three,
// only the write function name, input row shape, and InitRequest.phase
// differ, see vgi_vtab.cpp's xUpdate) for exactly one row per call.
//
// One full bind -> init(phase="INPUT") -> exchange(1 row) -> close round
// trip per SQLite xUpdate invocation, not one connection reused/batched
// across a whole statement: SQLite's own xUpdate contract calls this once
// per affected row with no batching hook, and the same "ship simple
// per-call round trips first" scope decision ScalarFunctionCaller
// documents applies here too - revisit only if profiling shows it's a
// real bottleneck. A fresh ConnectionPool checkout per call also sidesteps
// the same shared-connection stream conflicts that motivated
// ScalarFunctionCaller's own per-call checkout (see connection_pool.h).
//
// return_chunks is always false: no RETURNING support. SQLite's xUpdate
// contract has no channel to hand modified row *data* back into the
// query the way an SQL RETURNING clause would - it only wants a
// success/failure and, for an INSERT, the new rowid via pRowid. VGI's
// write functions default their bind() response to a plain
// {count: int64} output schema when return_chunks=false (confirmed by
// reading vgi-fixture-worker's simple_writable fixture, e.g. SimpleInsert.
// on_bind), so that's what this class always requests and parses.
//
// A table-in-out function that declares has_finalize=true (checked by
// vgi's own DuckDB extension via TableFunctionInfo.has_finalize before
// deciding whether to re-init with phase="FINALIZE" and drain more
// batches after the input stream closes) isn't supported here - every
// vgi-fixture-worker write function tested against so far
// (simple_writable's SimpleInsert/Update/Delete) doesn't set it, so a
// single exchange()+close() per row is a complete, correct round trip for
// them. A has_finalize=true function would need the FINALIZE phase this
// class doesn't drive - a documented gap, not silently wrong (Write()
// doesn't attempt to detect or handle it).
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/record_batch.h>

#include "catalog/catalog_client.h"  // ScanFunction
#include "vtab/connection_pool.h"

namespace vgi_sqlite {

class TableWriter {
public:
    TableWriter(ConnectionPool& pool, std::string location, std::string catalog_name);

    // Writes exactly one row and returns the worker's reported count (see
    // the file comment on why this is always the {count: int64} response,
    // never RETURNING rows). `write_function` comes from
    // VgiCatalogClient::Table{Insert,Update,Delete}FunctionGet;
    // `input_row` must already be shaped to what that write function
    // expects (see vgi_vtab.cpp's xUpdate for how each op's row is built).
    // transaction_opaque_data: see TableScanner::Bind's doc comment - same
    // contract, same ConnectionPool::CurrentTransactionOpaqueData source.
    int64_t Write(const ScanFunction& write_function, const std::optional<std::string>& schema_name,
                  const std::shared_ptr<arrow::RecordBatch>& input_row,
                  const std::optional<std::vector<uint8_t>>& transaction_opaque_data = std::nullopt);

private:
    ConnectionPool& pool_;
    std::string location_;
    std::string catalog_name_;
};

}  // namespace vgi_sqlite
