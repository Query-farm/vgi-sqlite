// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/catalog_requests.h"

#include "generated/vgi_request_builders.hpp"
#include "wire/wire_readers.h"

namespace vgi_sqlite {

nano::BatchPtr BuildCatalogAttachRequest(
    const std::string& name, const std::optional<std::vector<uint8_t>>& options_ipc_bytes,
    const std::optional<std::string>& data_version_spec,
    const std::optional<std::string>& implementation_version) {
    auto catalog_capabilities = nano::Batch::Named("CatalogCapabilities");
    catalog_capabilities->SetStringList(
        "read",
        std::vector<std::string>{"vgi.constraint.records.read.v1",
                                 "vgi.expression.duckdb.scalar.v1"});
    catalog_capabilities->SetStringList("ddl", std::vector<std::string>{});
    catalog_capabilities->SetStringList("write", std::vector<std::string>{});
    catalog_capabilities->SetStringList("transaction", std::vector<std::string>{});
    catalog_capabilities->FinishRow();
    auto catalog_bytes = wire::encode_ipc(catalog_capabilities);

    auto capabilities = vgi::generated::BuildClientCapabilities(
        "sqlite", {}, {}, false, {"vgi.filters.v2"},
        std::vector<uint8_t>(catalog_bytes.begin(), catalog_bytes.end()), false);
    auto capabilities_bytes = wire::encode_ipc(capabilities);

    auto request = nano::Batch::Named("CatalogAttachRequest");
    request->SetString("name", name);
    request->SetBytes("options", options_ipc_bytes);
    request->SetString("data_version_spec", data_version_spec);
    request->SetString("implementation_version", implementation_version);
    request->SetBytes("client_capabilities",
                      std::vector<uint8_t>(capabilities_bytes.begin(), capabilities_bytes.end()));
    request->FinishRow();
    return request;
}

}  // namespace vgi_sqlite
