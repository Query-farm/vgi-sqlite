// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Usage: vgi-table-in-out-probe <catalog> <schema> <function> <arg1> <arg2> ... [-- <more xFilter rows> ...] -- <worker argv...>
//
// Milestone-9 proof: discovers a blended table_in_out (RowTransformFunction)
// function via VgiCatalogClient::TableInOutFunctionGet, binds it once
// (learning its real output schema, exactly like vgi_table_in_out_vtab's
// xConnect will), then drives one TableInOutCaller through MULTIPLE
// Exchange() calls on the SAME bound connection/stream - one per `--`-
// separated group of argument values - proving the "bind once, exchange
// many times, one call per correlated outer row" lifecycle before wiring
// it into the SQLite virtual table module. Only integer-typed positional
// arguments are supported (matches this driver's other probes).
#include <cstdio>
#include <exception>
#include <sstream>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "catalog/catalog_client.h"
#include "catalog/table_in_out_caller.h"
#include "vtab/connection_pool.h"

int main(int argc, char** argv) {
    if (argc < 6) {
        std::fprintf(stderr,
                     "usage: %s <catalog> <schema> <function> <arg1> [arg2 ...] "
                     "[-- <arg1> [arg2...]]... -- <worker argv...>\n",
                     argv[0]);
        return 2;
    }
    const std::string catalog_name = argv[1];
    const std::string schema_name = argv[2];
    const std::string function_name = argv[3];

    // Parse one or more "--"-separated groups of int64 args, then the
    // final "--" before the worker argv.
    std::vector<std::vector<int64_t>> rows;
    std::vector<int64_t> current;
    int i = 4;
    std::vector<std::string> groups_raw;  // for finding the LAST "--" (worker argv separator)
    for (; i < argc; ++i) {
        std::string tok = argv[i];
        if (tok == "--") {
            rows.push_back(current);
            current.clear();
            // Peek: is the rest of argv all worker argv (no more "--" ahead)?
            bool more_dashdash = false;
            for (int j = i + 1; j < argc; ++j) {
                if (std::string(argv[j]) == "--") more_dashdash = true;
            }
            if (!more_dashdash) {
                ++i;
                break;
            }
        } else {
            current.push_back(std::stoll(tok));
        }
    }
    if (i >= argc) {
        std::fprintf(stderr, "error: missing -- separator before worker argv\n");
        return 2;
    }
    std::vector<std::string> worker_argv(argv + i, argv + argc);
    std::string location;
    for (size_t j = 0; j < worker_argv.size(); ++j) {
        if (j) location += ' ';
        location += worker_argv[j];
    }

    try {
        vgi_sqlite::ConnectionPool pool;

        // Discover + bind once (mirrors xConnect's schema-declaration
        // probe) via a throwaway checkout, then build the real
        // per-cursor caller against the SAME declared input_schema.
        auto discover_checkout = pool.Acquire(location, catalog_name);
        vgi_sqlite::VgiCatalogClient catalog(discover_checkout->connection);
        auto fn = catalog.TableInOutFunctionGet(discover_checkout->attach_opaque_data, schema_name, function_name);
        std::printf("discovered '%s.%s': input_schema=%s\n", fn.schema_name.c_str(), fn.function_name.c_str(),
                    fn.input_schema->ToString().c_str());

        vgi_sqlite::TableInOutCaller caller(pool, location, catalog_name, fn.schema_name, fn.function_name,
                                            fn.input_schema);
        auto output_schema = caller.Bind();
        std::printf("bound: output_schema=%s\n", output_schema->ToString().c_str());

        int row_num = 0;
        for (const auto& row_args : rows) {
            if (static_cast<int>(row_args.size()) != fn.input_schema->num_fields()) {
                std::fprintf(stderr, "error: row %d has %zu args, function expects %d\n", row_num,
                            row_args.size(), fn.input_schema->num_fields());
                return 1;
            }
            std::vector<std::shared_ptr<arrow::Array>> columns;
            for (size_t k = 0; k < row_args.size(); ++k) {
                auto scalar = arrow::MakeScalar(row_args[k]);
                auto casted = scalar->CastTo(fn.input_schema->field(static_cast<int>(k))->type());
                if (!casted.ok()) throw std::runtime_error("casting arg " + std::to_string(k) + ": " +
                                                             casted.status().ToString());
                auto array = arrow::MakeArrayFromScalar(*casted.ValueUnsafe(), 1);
                if (!array.ok()) throw std::runtime_error(array.status().ToString());
                columns.push_back(array.ValueUnsafe());
            }
            auto input_row = arrow::RecordBatch::Make(fn.input_schema, 1, columns);
            auto result = caller.Exchange(input_row);
            std::printf("row %d: exchange -> %lld output row(s)\n", row_num, (long long)result->num_rows());
            for (int64_t r = 0; r < result->num_rows(); ++r) {
                std::ostringstream oss;
                for (int c = 0; c < result->num_columns(); ++c) {
                    if (c) oss << ", ";
                    auto scalar_result = result->column(c)->GetScalar(r);
                    oss << result->schema()->field(c)->name() << "=" << (scalar_result.ok() ? (*scalar_result)->ToString() : "<err>");
                }
                std::printf("  [%lld] %s\n", (long long)r, oss.str().c_str());
            }
            ++row_num;
        }
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
