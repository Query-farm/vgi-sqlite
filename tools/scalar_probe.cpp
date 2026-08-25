// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Usage: vgi-scalar-probe <catalog> <schema> <function> <arg1> <arg2> ... <worker argv...>
//
// Milestone-3 proof: binds a plain scalar function and calls it once via
// exchange mode, printing the result - proves the bind/exchange wire
// protocol for scalar functions (distinct from TableScanner's producer
// mode) before wiring it into the SQLite extension proper. Arguments are
// parsed as int64 - this tool only exercises integer-typed functions.
#include <cstdio>
#include <exception>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "catalog/catalog_client.h"
#include "catalog/scalar_function_caller.h"
#include "rpc/vgi_connection.h"
#include "vtab/connection_pool.h"

int main(int argc, char** argv) {
    if (argc < 6) {
        std::fprintf(stderr, "usage: %s <catalog> <schema> <function> <arg1> [arg2 ...] -- <worker argv...>\n",
                     argv[0]);
        return 2;
    }
    const std::string catalog_name = argv[1];
    const std::string schema_name = argv[2];
    const std::string function_name = argv[3];

    std::vector<int64_t> args;
    int i = 4;
    for (; i < argc && std::string(argv[i]) != "--"; ++i) args.push_back(std::stoll(argv[i]));
    if (i >= argc) {
        std::fprintf(stderr, "error: missing -- separator before worker argv\n");
        return 2;
    }
    std::vector<std::string> worker_argv(argv + i + 1, argv + argc);

    try {
        // ScalarFunctionCaller now Acquire()s its own connection per call
        // (see its file comment) rather than taking one directly - build
        // it against a real ConnectionPool, same as the extension does,
        // to exercise the real code path. `location` is the pool's
        // space-separated argv convention (see connection_pool.cpp's
        // SplitWhitespace), reconstructed from worker_argv.
        vgi_sqlite::ConnectionPool pool;
        std::string location;
        for (size_t i = 0; i < worker_argv.size(); ++i) {
            if (i) location += ' ';
            location += worker_argv[i];
        }

        vgi_sqlite::ScalarFunctionCaller caller(pool, location, catalog_name, function_name,
                                                 static_cast<int>(args.size()), schema_name);
        std::vector<std::shared_ptr<arrow::Scalar>> scalars;
        for (auto v : args) scalars.push_back(arrow::MakeScalar(v));

        auto result = caller.Call(scalars);
        std::printf("result batch: rows=%lld cols=%d schema=%s\n", (long long)result->num_rows(),
                    result->num_columns(), result->schema()->ToString().c_str());
        auto col = result->column(0);
        std::printf("col length=%lld null_count=%lld type=%s\n", (long long)col->length(),
                    (long long)col->null_count(), col->type()->ToString().c_str());
        if (col->length() > 0) {
            auto int_col = std::static_pointer_cast<arrow::Int64Array>(col);
            std::printf("is_null(0)=%d value(0)=%lld\n", int_col->IsNull(0) ? 1 : 0,
                        int_col->IsNull(0) ? 0 : (long long)int_col->Value(0));
        }
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
