// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Usage: vgi-catalog-probe <catalog-name> <worker argv...>
//
// Milestone-2 proof: attaches a catalog and walks it - catalog_attach,
// catalog_schemas, catalog_schema_contents_tables per schema - printing
// every table found and its column schema. First real exercise of the
// generated request builders (generated/vgi_request_builders.hpp), the
// hand-coded CatalogAttachRequest inner builder, and the response readers.
#include <cstdio>
#include <exception>
#include <string>
#include <vector>

#include "catalog/catalog_client.h"
#include "rpc/vgi_connection.h"

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: %s <catalog-name> <worker argv...>\n", argv[0]);
        return 2;
    }
    const std::string catalog_name = argv[1];
    std::vector<std::string> worker_argv(argv + 2, argv + argc);

    try {
        auto conn = vgi_sqlite::VgiConnection::spawn(worker_argv);
        vgi_sqlite::VgiCatalogClient client(conn);

        auto attach = client.Attach(catalog_name);
        std::printf("attached catalog '%s'\n", catalog_name.c_str());
        std::printf("  default_schema:      %s\n", attach.default_schema.c_str());
        std::printf("  supports_transactions: %s\n", attach.supports_transactions ? "true" : "false");
        std::printf("  supports_time_travel:  %s\n", attach.supports_time_travel ? "true" : "false");

        for (const auto& schema : client.Schemas(attach.attach_opaque_data)) {
            std::printf("schema '%s'\n", schema.name.c_str());
            for (const auto& table : client.SchemaContentsTables(attach.attach_opaque_data, schema.name)) {
                std::printf("  table '%s'", table.name.c_str());
                if (table.cardinality_estimate >= 0) {
                    std::printf(" (~%lld rows)", static_cast<long long>(table.cardinality_estimate));
                }
                std::printf("\n");
                if (table.columns) {
                    for (int64_t i = 0; i < table.columns->num_fields(); ++i) {
                        const auto* field = table.columns->field(i);
                        std::printf("    %-24s %s%s\n", field->name ? field->name : "",
                                    vgi_sqlite::nano::TypeName(*field).c_str(),
                                    (field->flags & ARROW_FLAG_NULLABLE) ? "" : " NOT NULL");
                    }
                }
            }
        }
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
