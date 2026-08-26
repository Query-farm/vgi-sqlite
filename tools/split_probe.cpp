// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Usage: vgi-split-probe <catalog> <function_name> <n> <splits> <worker argv...>
//
// Ad hoc verification tool for TableScanner's split support
// (catalog/catalog_table_plan.h) - VGI's split-capable fixtures (splits.py)
// are only exposed as directly-callable table FUNCTIONS with SQL-visible
// arguments (`example.main.split_sequence(n := 100, splits := 1)`), not as
// plain fixed-argument tables the way every other fixture this driver's
// vtab model already handles is - so there is no CREATE VIRTUAL TABLE path
// to exercise this through yet (that's the DuckDB-only "catalog-qualified
// table-function-call" gap, tracked separately - see
// test/sqllogictest/run_sqllogictest.py's STRUCTURAL_SKIP_CATEGORIES).
// This probe binds the named function directly, constructing its own
// positional (n, splits) argument batch, matching every prior milestone's
// "probe tool first" verification discipline.
#include <cstdio>
#include <exception>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "catalog/catalog_client.h"
#include "catalog/table_scanner.h"
#include "rpc/vgi_connection.h"
#include "wire/wire_readers.h"

int main(int argc, char** argv) {
    if (argc < 6) {
        std::fprintf(stderr, "usage: %s <catalog> <function_name> <n> <splits> <worker argv...>\n", argv[0]);
        return 2;
    }
    const std::string catalog_name = argv[1];
    const std::string function_name = argv[2];
    const int64_t n = std::stoll(argv[3]);
    const int64_t splits = std::stoll(argv[4]);
    std::vector<std::string> worker_argv(argv + 5, argv + argc);

    try {
        auto conn = vgi_sqlite::VgiConnection::spawn(worker_argv);
        vgi_sqlite::VgiCatalogClient catalog(conn);
        auto attach = catalog.Attach(catalog_name);

        // split_sequence's own arguments are declared
        // Annotated[int, Arg("n", ...)] / Arg("splits", ...) - confirmed
        // (the hard way: "KeyError: Argument 'n': not found" against a
        // plain positional_0/positional_1 encoding first) that this
        // worker-side argument resolver keys strictly by declared NAME,
        // not position. So build the exact wire shape
        // BindRequest.arguments needs directly - a one-row {args:
        // struct<named_n, named_splits>} batch - and hand it to Bind()
        // via arguments_already_wrapped rather than the flat encoding
        // WrapAsArgsStruct would turn into positional_0/positional_1.
        auto struct_type = arrow::struct_({
            arrow::field("named_n", arrow::int64(), false),
            arrow::field("named_splits", arrow::int64(), false),
        });
        auto n_arr = arrow::MakeArrayFromScalar(*arrow::MakeScalar(n), 1).ValueOrDie();
        auto splits_arr = arrow::MakeArrayFromScalar(*arrow::MakeScalar(splits), 1).ValueOrDie();
        auto struct_arr = arrow::StructArray::Make({n_arr, splits_arr}, struct_type->fields()).ValueOrDie();
        auto args_schema = arrow::schema({arrow::field("args", struct_type, false)});
        auto args_batch = arrow::RecordBatch::Make(args_schema, 1, {struct_arr});
        auto args_ipc = vgi_sqlite::wire::encode_ipc(args_batch);

        vgi_sqlite::ScanFunction fn;
        fn.function_name = function_name;
        fn.arguments_ipc_bytes.assign(args_ipc.begin(), args_ipc.end());
        fn.supports_splits = true;
        fn.arguments_already_wrapped = true;

        vgi_sqlite::TableScanner scanner(conn, attach.attach_opaque_data);
        scanner.Bind(fn);
        std::printf("bind resolved output schema: %s\n", scanner.output_schema()->ToString().c_str());
        scanner.Init();

        int64_t total_rows = 0;
        int batch_count = 0;
        while (auto batch = scanner.Next()) {
            batch_count++;
            total_rows += (*batch)->num_rows();
            std::printf("batch %d: %lld rows\n", batch_count, static_cast<long long>((*batch)->num_rows()));
        }
        std::printf("TOTAL: %d batches, %lld rows\n", batch_count, static_cast<long long>(total_rows));
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
    return 0;
}
