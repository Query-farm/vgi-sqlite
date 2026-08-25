// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Usage: vgi-write-probe <catalog> <schema> <table> <id> <name> -- <worker argv...>
//
// Milestone-4 proof: resolves a table's insert function, builds a 1-row
// {id: int64, name: string} input batch (items_insert_only's user schema
// minus its rowid column - see TableWriter's file comment), inserts it,
// prints the reported count, then re-scans the table via TableScanner to
// independently confirm the row is actually there - proves the write wire
// protocol (bind -> init(phase=INPUT) -> exchange -> close) works in
// isolation before wiring it into the SQLite extension's xUpdate proper.
#include <cstdio>
#include <exception>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "catalog/catalog_client.h"
#include "catalog/table_scanner.h"
#include "catalog/table_writer.h"
#include "vtab/connection_pool.h"

int main(int argc, char** argv) {
    if (argc < 7) {
        std::fprintf(stderr, "usage: %s <catalog> <schema> <table> <id> <name> -- <worker argv...>\n",
                     argv[0]);
        return 2;
    }
    const std::string catalog_name = argv[1];
    const std::string schema_name = argv[2];
    const std::string table_name = argv[3];
    const int64_t id = std::stoll(argv[4]);
    const std::string name = argv[5];
    if (std::string(argv[6]) != "--") {
        std::fprintf(stderr, "error: missing -- separator before worker argv\n");
        return 2;
    }
    std::vector<std::string> worker_argv(argv + 7, argv + argc);
    std::string location;
    for (size_t i = 0; i < worker_argv.size(); ++i) {
        if (i) location += ' ';
        location += worker_argv[i];
    }

    try {
        vgi_sqlite::ConnectionPool pool;
        auto checkout = pool.Acquire(location, catalog_name);
        vgi_sqlite::VgiCatalogClient catalog(checkout->connection);
        auto insert_fn = catalog.TableInsertFunctionGet(checkout->attach_opaque_data, schema_name, table_name);
        std::printf("insert function: %s\n", insert_fn.function_name.c_str());

        auto input_schema = arrow::schema({
            arrow::field("id", arrow::int64(), false),
            arrow::field("name", arrow::utf8(), false),
        });
        auto id_arr = arrow::MakeArrayFromScalar(*arrow::MakeScalar(id), 1).ValueOrDie();
        auto name_arr = arrow::MakeArrayFromScalar(*arrow::MakeScalar(name), 1).ValueOrDie();
        auto input_row = arrow::RecordBatch::Make(input_schema, 1, {id_arr, name_arr});

        vgi_sqlite::TableWriter writer(pool, location, catalog_name);
        auto count = writer.Write(insert_fn, /*schema_name=*/std::nullopt, input_row);
        std::printf("insert reported count: %lld\n", (long long)count);

        // Independently confirm via a fresh scan.
        auto checkout2 = pool.Acquire(location, catalog_name);
        auto table = catalog.TableGet(checkout2->attach_opaque_data, schema_name, table_name);
        vgi_sqlite::TableScanner scanner(checkout2->connection, checkout2->attach_opaque_data);
        scanner.Bind(*table.scan_function);
        scanner.Init();
        int64_t total_rows = 0;
        bool found = false;
        while (auto batch = scanner.Next()) {
            total_rows += (*batch)->num_rows();
            auto id_col = std::dynamic_pointer_cast<arrow::Int64Array>((*batch)->GetColumnByName("id"));
            auto name_col = std::dynamic_pointer_cast<arrow::StringArray>((*batch)->GetColumnByName("name"));
            if (id_col && name_col) {
                for (int64_t i = 0; i < (*batch)->num_rows(); ++i) {
                    if (!id_col->IsNull(i) && id_col->Value(i) == id && !name_col->IsNull(i) &&
                        name_col->GetString(i) == name) {
                        found = true;
                    }
                }
            }
        }
        std::printf("post-insert scan: %lld total rows, inserted row found=%d\n", (long long)total_rows,
                    found ? 1 : 0);
        return found ? 0 : 1;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
