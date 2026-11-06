// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/scalar_function_caller.h"

#include <stdexcept>

#include "catalog/scan_requests.h"
#include "generated/vgi_request_builders.hpp"
#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {
namespace gen = ::vgi::generated;

std::vector<uint8_t> to_bytes(const std::string& s) { return {s.begin(), s.end()}; }

nano::BatchPtr CallUnary(VgiConnection& connection, const std::string& method,
                                              const nano::BatchPtr& params) {
    auto response = connection.CallUnary(method, params);
    return wire::get_ipc(response.batch, "result");
}

// The 0-field const-args struct BindRequest.arguments needs for a
// function with no ConstParam/const-positional arguments (every argument
// here is a normal per-row Param) - see scalar_function_caller.h's file
// comment and the plain-vs-const bind protocol this mirrors from
// table_scanner.cpp's WrapAsArgsStruct (same construction, simpler: always
// exactly zero fields here, never inferred from a flat source batch).
std::vector<uint8_t> BuildEmptyArgsBytes() {
    auto args_schema = nano::MakeSchema(
        {nano::Field("args", nano::StructType({}), /*nullable=*/false)});
    auto args_batch = nano::Batch::Create(args_schema);
    args_batch->SetStruct("args", {});
    args_batch->FinishRow();
    return to_bytes(wire::encode_ipc(args_batch));
}

// VGI's fixed exchange-column naming for scalar function arguments -
// "col_0", "col_1", ... regardless of the SQL argument's own name (see
// scalar_function_caller.h's file comment). Types come from the actual
// argument scalars passed to the first Call(), not from any catalog
// declaration - see the header's file comment on why.
nano::SchemaPtr SchemaFromArgs(const std::vector<nano::ScalarPtr>& args) {
    std::vector<nano::SchemaPtr> fields;
    for (size_t i = 0; i < args.size(); ++i) {
        fields.push_back(nano::Field("col_" + std::to_string(i), args[i]->type, /*nullable=*/true));
    }
    return nano::MakeSchema(fields);
}

}  // namespace

ScalarFunctionCaller::ScalarFunctionCaller(ConnectionPool& pool, std::string location,
                                           std::string catalog_name, std::string function_name,
                                           int num_args, std::optional<std::string> schema_name)
    : pool_(pool),
      location_(std::move(location)),
      catalog_name_(std::move(catalog_name)),
      function_name_(std::move(function_name)),
      num_args_(num_args),
      schema_name_(std::move(schema_name)) {}

nano::BatchPtr ScalarFunctionCaller::Call(
    const std::vector<nano::ScalarPtr>& args) {
    if (static_cast<int>(args.size()) != num_args_) {
        throw std::runtime_error("scalar function '" + function_name_ + "': expected " +
                                  std::to_string(num_args_) + " arguments, got " +
                                  std::to_string(args.size()));
    }
    // Re-derive the argument schema fresh from this call's own actual
    // values every time - see the file comment on why there's nothing to
    // cache/lock here.
    auto arg_types = SchemaFromArgs(args);

    auto input_batch = nano::Batch::FromScalars(arg_types, args);

    // Acquire, bind, init, exchange, close, release - all on one freshly
    // checked-out connection, every call. See the header's file comment on
    // why nothing here is cached across calls at all, including the
    // argument types.
    auto checkout = pool_.Acquire(location_, catalog_name_);
    auto input_schema_bytes = to_bytes(wire::encode_schema(arg_types));
    auto inner = BuildBindRequest(function_name_, BuildEmptyArgsBytes(), /*function_type=*/"SCALAR",
                                   input_schema_bytes, /*settings_bytes=*/std::nullopt,
                                   /*secrets_bytes=*/std::nullopt, to_bytes(checkout->attach_opaque_data),
                                   /*transaction_opaque_data=*/std::nullopt,
                                   /*resolved_secrets_provided=*/false, schema_name_);
    auto bind_call_bytes = to_bytes(wire::encode_ipc(inner));
    auto bind_params = gen::BuildBindParams(bind_call_bytes);
    auto bind_result = CallUnary(checkout->connection, "bind", bind_params);
    auto parsed = ParseBindResponse(bind_result);
    if (!parsed.output_schema || parsed.output_schema->num_fields() != 1) {
        throw std::runtime_error("scalar function '" + function_name_ +
                                  "': bind() returned an unexpected output schema");
    }

    auto output_schema_bytes = to_bytes(wire::encode_schema(parsed.output_schema));
    auto init_inner = BuildInitRequest(bind_call_bytes, output_schema_bytes,
                                        parsed.opaque_data.empty()
                                            ? std::nullopt
                                            : std::optional<std::vector<uint8_t>>(parsed.opaque_data));
    auto init_bytes = to_bytes(wire::encode_ipc(init_inner));
    auto init_params = gen::BuildInitParams(init_bytes);
    auto stream = checkout->connection.OpenExchange("init", init_params, arg_types, parsed.output_schema,
                                                     /*has_header=*/true);
    auto response = stream->Exchange(input_batch);
    if (!response || !response->batch) {
        stream->Close();
        throw std::runtime_error("scalar function '" + function_name_ + "': worker closed the stream");
    }
    auto result = response->batch;
    stream->Close();  // see TableScanner::~TableScanner() for why this matters even on success
    return result;
}

}  // namespace vgi_sqlite
