// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Usage: vgi-scan-probe <catalog> <schema> <table> <worker argv...>
//
// Milestone-2 proof: attaches, resolves a table's scan function, binds and
// inits it, and prints every row the producer stream yields - the first
// real end-to-end data scan (not just catalog metadata).
#include <cstdio>
#include <exception>
#include <string>
#include <vector>

#include <arrow/type.h>

#include "catalog/catalog_client.h"
#include "catalog/table_scanner.h"
#include "rpc/vgi_connection.h"

int main(int argc, char** argv) {
    if (argc < 5) {
        std::fprintf(stderr, "usage: %s <catalog> <schema> <table> <worker argv...>\n", argv[0]);
        return 2;
    }
    const std::string catalog_name = argv[1];
    const std::string schema_name = argv[2];
    const std::string table_name = argv[3];
    std::vector<std::string> worker_argv(argv + 4, argv + argc);

    try {
        auto conn = vgi_sqlite::VgiConnection::spawn(worker_argv);
        vgi_sqlite::VgiCatalogClient catalog(conn);

        auto attach = catalog.Attach(catalog_name);
        auto table = catalog.TableGet(attach.attach_opaque_data, schema_name, table_name);
        std::printf("table '%s.%s': %d columns\n", schema_name.c_str(), table_name.c_str(),
                    table.columns ? table.columns->num_fields() : 0);
        if (!table.scan_function) {
            std::fprintf(stderr, "error: table has no inlined scan_function (fallback RPC not implemented)\n");
            return 1;
        }

        vgi_sqlite::TableScanner scanner(conn, attach.attach_opaque_data);
        // Not schema_name: see vgi_vtab.cpp's xFilter comment on why the
        // backing function's schema isn't assumed to match the table's.
        scanner.Bind(*table.scan_function);
        std::printf("bind resolved output schema: %s\n", scanner.output_schema()->ToString().c_str());
        scanner.Init();

        int64_t total_rows = 0;
        int batch_count = 0;
        while (auto batch = scanner.Next()) {
            ++batch_count;
            total_rows += (*batch)->num_rows();
            if (batch_count <= 3) {
                std::printf("batch %d: %lld rows\n%s\n", batch_count, static_cast<long long>((*batch)->num_rows()),
                            (*batch)->ToString().c_str());
            }
        }
        std::printf("done: %d batches, %lld total rows\n", batch_count, static_cast<long long>(total_rows));
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
