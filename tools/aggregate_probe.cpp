// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Usage: vgi-aggregate-probe <catalog> <schema> <function> <arg1> <arg2> ... -- <worker argv...>
//
// Milestone-4 proof: binds a single-argument aggregate function, steps
// each given integer value as its own row (one Step() per value, matching
// how SQLite's own xStep fires once per row), finalizes, and prints the
// result - proves the bind/update*/finalize/destructor wire protocol
// before wiring it into the SQLite extension's xStep/xFinal bridge. Only
// exercises single-argument integer-typed aggregates (e.g. vgi_sum(x));
// pass `nullary:<rowcount>` instead of values for a nullary aggregate
// (e.g. vgi_count()), stepped that many times with no arguments.
#include <cstdio>
#include <exception>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "catalog/aggregate_caller.h"
#include "vtab/connection_pool.h"

int main(int argc, char** argv) {
    if (argc < 5) {
        std::fprintf(stderr,
                     "usage: %s <catalog> <schema> <function> <arg1> [arg2 ...] -- <worker argv...>\n"
                     "       %s <catalog> <schema> <function> nullary:<rowcount> -- <worker argv...>\n",
                     argv[0], argv[0]);
        return 2;
    }
    const std::string catalog_name = argv[1];
    const std::string schema_name = argv[2];
    const std::string function_name = argv[3];

    std::vector<int64_t> args;
    int64_t nullary_rows = -1;
    int i = 4;
    for (; i < argc && std::string(argv[i]) != "--"; ++i) {
        std::string tok = argv[i];
        if (tok.rfind("nullary:", 0) == 0) {
            nullary_rows = std::stoll(tok.substr(8));
        } else {
            args.push_back(std::stoll(tok));
        }
    }
    if (i >= argc) {
        std::fprintf(stderr, "error: missing -- separator before worker argv\n");
        return 2;
    }
    std::vector<std::string> worker_argv(argv + i + 1, argv + argc);
    std::string location;
    for (size_t j = 0; j < worker_argv.size(); ++j) {
        if (j) location += ' ';
        location += worker_argv[j];
    }

    try {
        vgi_sqlite::ConnectionPool pool;
        int num_args = nullary_rows >= 0 ? 0 : 1;  // single-argument aggregates only, see the file comment
        vgi_sqlite::AggregateCaller caller(pool, location, catalog_name, function_name, num_args, schema_name);

        if (nullary_rows >= 0) {
            for (int64_t r = 0; r < nullary_rows; ++r) caller.Step({});
        } else {
            for (auto v : args) caller.Step({arrow::MakeScalar(v)});
        }

        auto result = caller.Finalize();
        std::printf("result batch: rows=%lld cols=%d schema=%s\n", (long long)result->num_rows(),
                    result->num_columns(), result->schema()->ToString().c_str());
        auto col = result->column(0);
        std::printf("col type=%s is_null(0)=%d\n", col->type()->ToString().c_str(), col->IsNull(0) ? 1 : 0);
        if (!col->IsNull(0)) {
            auto scalar_result = col->GetScalar(0);
            if (scalar_result.ok()) std::printf("value(0)=%s\n", (*scalar_result)->ToString().c_str());
        }
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
