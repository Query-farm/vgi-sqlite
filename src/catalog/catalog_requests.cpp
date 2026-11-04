// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/catalog_requests.h"

#include <arrow/api.h>

#include "generated/vgi_request_builders.hpp"
#include "wire/wire_readers.h"

namespace vgi_sqlite {

std::shared_ptr<arrow::RecordBatch> BuildCatalogAttachRequest(
    const std::string& name, const std::optional<std::vector<uint8_t>>& options_ipc_bytes,
    const std::optional<std::string>& data_version_spec,
    const std::optional<std::string>& implementation_version) {
    // Matches vgi-python's CatalogAttachRequest (vgi/protocol.py). The
    // pyarrow-inferred wire schema marks every field but `name` nullable.
    //
    // Reading constraint records is offered because a worker refuses to serve
    // a table carrying them otherwise; this driver has no use for them beyond
    // that, and reading a table does not depend on them. Enforcing them is
    // deliberately not offered, since this driver doesn't, so such a table is
    // served to it without its write functions.
    auto catalog_capabilities = arrow::RecordBatch::Make(
        vgi::generated::CatalogCapabilitiesSchema(), 1,
        {vgi::BuildStringListScalar({"vgi.constraint.records.read.v1", "vgi.expression.duckdb.scalar.v1"}),
         vgi::BuildStringListScalar({}), vgi::BuildStringListScalar({}), vgi::BuildStringListScalar({})});
    auto catalog_bytes = wire::encode_ipc(catalog_capabilities);
    auto capabilities = vgi::generated::BuildClientCapabilities(
        /*engine=*/"sqlite", /*native_formats=*/{}, /*catalogs=*/{}, /*can_stream=*/false,
        /*filter_encodings=*/{"vgi.filters.v2"},
        /*catalog=*/std::vector<uint8_t>(catalog_bytes.begin(), catalog_bytes.end()),
        /*query_pushdown=*/nullptr);
    auto capabilities_bytes = wire::encode_ipc(capabilities);
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
    arrays.push_back(vgi::BuildOptionalBinaryScalar(
        std::vector<uint8_t>(capabilities_bytes.begin(), capabilities_bytes.end())));
    return arrow::RecordBatch::Make(request_schema, 1, arrays);
}

}  // namespace vgi_sqlite
