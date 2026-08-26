// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Usage: vgi-http-probe <location> <catalog-name>
//
// Ad hoc diagnostic: connects via VgiConnection::Connect() (so it can hit
// http(s):// locations, unlike every other probe here which hardcodes
// spawn()), prints describe()'s method inventory, then walks
// catalog_attach -> catalog_schemas -> catalog_schema_contents_tables/
// functions with verbose output at every step.
#include <cstdio>
#include <exception>
#include <string>

#include <arrow/type.h>

#include "catalog/catalog_client.h"
#include "rpc/vgi_connection.h"

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <location> <catalog-name>\n", argv[0]);
        return 2;
    }
    const std::string location = argv[1];
    const std::string catalog_name = argv[2];

    try {
        auto conn = vgi_sqlite::VgiConnection::Connect(location);
        auto desc = conn.describe();
        std::printf("describe(): %zu methods\n", desc.methods.size());

        vgi_sqlite::VgiCatalogClient client(conn);
        auto catalogs = client.Catalogs();
        std::printf("catalogs(): %zu\n", catalogs.size());
        for (const auto& c : catalogs) std::printf("  '%s'\n", c.c_str());

        auto attach = client.Attach(catalog_name);
        std::printf("attached '%s': default_schema=%s supports_txn=%d\n", catalog_name.c_str(),
                    attach.default_schema.c_str(), attach.supports_transactions);

        auto schemas = client.Schemas(attach.attach_opaque_data);
        std::printf("schemas: %zu\n", schemas.size());
        for (const auto& schema : schemas) {
            std::printf("  schema '%s'\n", schema.name.c_str());
            auto tables = client.SchemaContentsTables(attach.attach_opaque_data, schema.name);
            std::printf("    tables: %zu\n", tables.size());
            for (const auto& t : tables) std::printf("      table '%s'\n", t.name.c_str());
            auto scalars = client.SchemaContentsScalarFunctions(attach.attach_opaque_data, schema.name);
            std::printf("    scalar functions: %zu\n", scalars.size());
            for (const auto& f : scalars) std::printf("      fn '%s'\n", f.name.c_str());
            auto tio = client.SchemaContentsTableInOutFunctions(attach.attach_opaque_data, schema.name);
            std::printf("    table_in_out functions (blended only): %zu\n", tio.size());
            for (const auto& f : tio)
                std::printf("      fn '%s' input_schema=%s\n", f.function_name.c_str(),
                            f.input_schema->ToString().c_str());
        }
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
