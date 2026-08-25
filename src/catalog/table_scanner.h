// © Copyright 2026 Query Farm LLC - https://query.farm
//
// TableScanner: the bind -> init -> producer-tick lifecycle for reading a
// table's rows, VGI's table_function protocol driven directly (a VGI
// "table" doesn't scan itself - TableInfo.scan_function names the actual
// table function to bind/init/scan; see CatalogTable::scan_function).
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/record_batch.h>

#include "catalog/catalog_client.h"  // ScanFunction
#include "rpc/vgi_connection.h"

namespace vgi_sqlite {

class TableScanner {
public:
    TableScanner(VgiConnection& connection, std::string attach_opaque_data);
    // Explicitly closes any live stream (see the .cpp: an abandoned,
    // never-closed ClientStream aborts the whole shared connection on
    // destruction, per vgi-rpc-c++'s contract - even one that already
    // reached natural end-of-stream via Next() returning nullopt).
    ~TableScanner();
    TableScanner(const TableScanner&) = delete;
    TableScanner& operator=(const TableScanner&) = delete;

    // bind: resolves the function's output schema for this arguments/schema
    // shape. Must be called once before Init().
    void Bind(const ScanFunction& scan_function, const std::optional<std::string>& schema_name = std::nullopt);

    const std::shared_ptr<arrow::Schema>& output_schema() const { return bind_.output_schema; }

    // init: opens the producer stream. projection_ids, if non-empty, asks
    // the worker to emit only those column indices (into output_schema());
    // pushdown_filters, if set, is an IPC-encoded WHERE-constraint batch
    // in VGI's hybrid JSON+Arrow format (see vtab/filter_pushdown.h);
    // row_limit, if set, is a plain "stop after this many rows" hint
    // (InitRequest.row_limit). None of the three is a correctness
    // guarantee, only a hint a function may ignore (see xColumn's
    // width-check comment in vgi_vtab.cpp for what that means for
    // projection in practice) - and row_limit specifically must only be
    // passed by a caller that has independently confirmed early
    // termination can't drop rows the query still needs (see
    // vgi_vtab.cpp's xBestIndex comment).
    void Init(const std::vector<int64_t>& projection_ids = {},
              const std::optional<std::string>& pushdown_filters = std::nullopt,
              std::optional<int64_t> row_limit = std::nullopt);

    // Pull the next output batch, or nullopt when the scan is exhausted.
    std::optional<std::shared_ptr<arrow::RecordBatch>> Next();

private:
    VgiConnection& connection_;
    std::string attach_opaque_data_;
    std::vector<uint8_t> bind_call_bytes_;
    struct {
        std::shared_ptr<arrow::Schema> output_schema;
        std::vector<uint8_t> opaque_data;
    } bind_;
    std::optional<vgi_rpc::ClientStream> stream_;
};

}  // namespace vgi_sqlite
