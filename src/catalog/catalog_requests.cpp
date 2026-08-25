// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/catalog_requests.h"

#include <arrow/api.h>

#include "wire/wire_builders.h"

namespace vgi_sqlite {

std::shared_ptr<arrow::RecordBatch> BuildCatalogAttachRequest(
    const std::string& name, const std::optional<std::vector<uint8_t>>& options_ipc_bytes,
    const std::optional<std::string>& data_version_spec,
    const std::optional<std::string>& implementation_version) {
    // Matches vgi-python's CatalogAttachRequest (vgi/protocol.py). The
    // pyarrow-inferred wire schema marks every field but `name` nullable;
    // client_capabilities is left null (a later phase's concern).
    auto request_schema = arrow::schema({
        arrow::field("name", arrow::utf8(), /*nullable=*/false),
        arrow::field("options", arrow::binary(), /*nullable=*/true),
        arrow::field("data_version_spec", arrow::utf8(), /*nullable=*/true),
        arrow::field("implementation_version", arrow::utf8(), /*nullable=*/true),
        arrow::field("client_capabilities", arrow::binary(), /*nullable=*/true),
    });
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.push_back(vgi::BuildStringScalar(name));
    arrays.push_back(vgi::BuildOptionalBinaryScalar(options_ipc_bytes));
    arrays.push_back(vgi::BuildOptionalStringScalar(data_version_spec));
    arrays.push_back(vgi::BuildOptionalStringScalar(implementation_version));
    arrays.push_back(vgi::BuildOptionalBinaryScalar(std::nullopt));
    return arrow::RecordBatch::Make(request_schema, 1, arrays);
}

}  // namespace vgi_sqlite
