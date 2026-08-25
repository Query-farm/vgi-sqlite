// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/table_writer.h"

#include <stdexcept>

#include <arrow/api.h>

#include "catalog/scan_requests.h"
#include "catalog/write_requests.h"
#include "generated/vgi_request_builders.hpp"
#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {
namespace gen = ::vgi::generated;

std::vector<uint8_t> to_bytes(const std::string& s) { return {s.begin(), s.end()}; }

std::shared_ptr<arrow::RecordBatch> CallUnary(VgiConnection& connection, const std::string& method,
                                              const std::shared_ptr<arrow::RecordBatch>& params) {
    auto response = connection.CallUnary(method, params);
    return wire::get_ipc(response.batch, "result");
}

}  // namespace

TableWriter::TableWriter(ConnectionPool& pool, std::string location, std::string catalog_name)
    : pool_(pool), location_(std::move(location)), catalog_name_(std::move(catalog_name)) {}

int64_t TableWriter::Write(const ScanFunction& write_function,
                           const std::optional<std::string>& schema_name,
                           const std::shared_ptr<arrow::RecordBatch>& input_row,
                           const std::optional<std::vector<uint8_t>>& transaction_opaque_data) {
    auto checkout = pool_.Acquire(location_, catalog_name_);

    // return_chunks=false, on_conflict="throw": see the header's file
    // comment on why no RETURNING support, and table_writer.h/vgi_vtab.cpp
    // on why ON CONFLICT translation isn't implemented (SQLite's own
    // UNIQUE-constraint enforcement on the underlying table already
    // surfaces a conflict as an error either way).
    auto write_options_bytes = BuildWriteOptionsBytes(/*return_chunks=*/false, /*on_conflict=*/"throw");
    auto args_bytes = BuildWriteArgsStruct(write_function.arguments_ipc_bytes, write_options_bytes);
    auto input_schema_bytes = to_bytes(wire::encode_schema(input_row->schema()));

    auto bind_inner = BuildBindRequest(write_function.function_name, args_bytes, /*function_type=*/"TABLE",
                                       input_schema_bytes, /*settings_bytes=*/std::nullopt,
                                       /*secrets_bytes=*/std::nullopt, to_bytes(checkout->attach_opaque_data),
                                       transaction_opaque_data,
                                       /*resolved_secrets_provided=*/false, schema_name);
    auto bind_call_bytes = to_bytes(wire::encode_ipc(bind_inner));
    auto bind_params = gen::BuildBindParams(bind_call_bytes);
    auto bind_result = CallUnary(checkout->connection, "bind", bind_params);
    auto parsed = ParseBindResponse(bind_result);
    if (!parsed.output_schema || parsed.output_schema->num_fields() != 1 ||
        parsed.output_schema->field(0)->name() != "count") {
        throw std::runtime_error("write function '" + write_function.function_name +
                                  "': bind() returned an unexpected output schema (expected a plain "
                                  "{count: int64} response - RETURNING isn't requested/supported here)");
    }

    auto output_schema_bytes = to_bytes(wire::encode_schema(parsed.output_schema));
    auto init_inner = BuildInitRequest(bind_call_bytes, output_schema_bytes,
                                       parsed.opaque_data.empty()
                                           ? std::nullopt
                                           : std::optional<std::vector<uint8_t>>(parsed.opaque_data),
                                       /*projection_ids=*/{}, /*pushdown_filters=*/std::nullopt,
                                       /*join_keys=*/{}, /*row_limit=*/std::nullopt, /*phase=*/"INPUT");
    auto init_bytes = to_bytes(wire::encode_ipc(init_inner));
    auto init_params = gen::BuildInitParams(init_bytes);
    auto stream = checkout->connection.OpenExchange("init", init_params, input_row->schema(),
                                                     parsed.output_schema, /*has_header=*/true);
    auto response = stream->Exchange(input_row);
    if (!response || !response->batch) {
        stream->Close();
        throw std::runtime_error("write function '" + write_function.function_name +
                                  "': worker closed the stream without a response");
    }
    auto result_batch = response->batch;
    stream->Close();

    auto count_col = std::dynamic_pointer_cast<arrow::Int64Array>(result_batch->GetColumnByName("count"));
    if (!count_col || count_col->length() == 0 || count_col->IsNull(0)) {
        throw std::runtime_error("write function '" + write_function.function_name +
                                  "': response batch missing a valid 'count' value");
    }
    return count_col->Value(0);
}

}  // namespace vgi_sqlite
