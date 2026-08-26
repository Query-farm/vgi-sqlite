// © Copyright 2026 Query Farm LLC - https://query.farm
//
// TableScanner: the bind -> init -> producer-tick lifecycle for reading a
// table's rows, VGI's table_function protocol driven directly (a VGI
// "table" doesn't scan itself - TableInfo.scan_function names the actual
// table function to bind/init/scan; see CatalogTable::scan_function).
//
// Splits (ScanFunction::supports_splits - see catalog_table_plan.h for the
// full design): transparent to every caller of this class. When the bound
// function opts in, Init() plans the scan into a sequence of tokens
// instead of one whole-scan init, and Next() redeems them one at a time -
// closing the exhausted split's stream and opening the next one internally
// whenever the current stream's tick returns real end-of-stream (nullopt,
// never a present-but-empty batch - see Next()'s own comment), until every
// split is exhausted. xFilter/xNext/xEof in vgi_vtab.cpp need no knowledge
// of any of this: Next() still returns nullopt exactly once, at the true
// end of the whole scan, whether that scan was one init or many.
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
    // never-closed stream aborts the whole shared connection on
    // destruction, per vgi-rpc-c++'s contract - even one that already
    // reached natural end-of-stream via Next() returning nullopt).
    ~TableScanner();
    TableScanner(const TableScanner&) = delete;
    TableScanner& operator=(const TableScanner&) = delete;

    // bind: resolves the function's output schema for this arguments/schema
    // shape. Must be called once before Init(). transaction_opaque_data,
    // when set, is VGI's own transaction handle - read from
    // ConnectionPool::CurrentTransactionOpaqueData by the caller (see
    // connection_pool.h's file comment) when a SQL transaction is
    // currently open on this table's (location, catalog); nullopt
    // otherwise (including: that catalog doesn't support transactions).
    void Bind(const ScanFunction& scan_function, const std::optional<std::string>& schema_name = std::nullopt,
              const std::optional<std::vector<uint8_t>>& transaction_opaque_data = std::nullopt);

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
    // Opens the producer stream for pending_splits_[next_split_index_] and
    // advances next_split_index_ - the split-mode equivalent of the
    // ordinary (non-split) half of Init(), reusing the same stored
    // projection/filter/row_limit values every split redemption needs.
    void OpenNextSplit();

    VgiConnection& connection_;
    std::string attach_opaque_data_;
    std::vector<uint8_t> bind_call_bytes_;
    struct {
        std::shared_ptr<arrow::Schema> output_schema;
        std::vector<uint8_t> opaque_data;
    } bind_;
    bool supports_splits_ = false;
    bool inited_ = false;
    std::unique_ptr<VgiStream> stream_;

    // Splits-mode state - unused (and pending_splits_ always empty) when
    // supports_splits_ is false. Set by Init(); OpenNextSplit() consumes
    // pending_splits_/next_split_index_/split_init_opaque_data_, and
    // Next() drives which is called when.
    std::vector<std::vector<uint8_t>> pending_splits_;
    size_t next_split_index_ = 0;
    // Replaces bind_.opaque_data as every split's init bind_opaque_data -
    // a plan is itself a kind of re-bind (see catalog_table_plan.h).
    std::vector<uint8_t> split_init_opaque_data_;
    std::vector<int64_t> init_projection_ids_;
    std::optional<std::string> init_pushdown_filters_;
    std::optional<int64_t> init_row_limit_;
};

}  // namespace vgi_sqlite
