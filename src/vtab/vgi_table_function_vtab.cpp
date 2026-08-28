// © Copyright 2026 Query Farm LLC - https://query.farm
//
// See vgi_table_function_vtab.h for the module's overall design. This
// file comment covers the mechanics: how a HIDDEN-column argument row
// becomes a real ScanFunction call, and why xConnect's schema probe uses
// placeholder values while xFilter's real scans use the actual bound
// ones.
//
// xConnect/xCreate needs the function's real (bind()-resolved) output
// schema to declare this table's DDL, but has no real argument VALUES
// yet (no query has run). Binds once with PLACEHOLDER values (zero/
// empty/false per declared type) purely to learn output_schema, then
// discards that TableScanner entirely - matching vgi_table_in_out_vtab's
// own "throwaway xConnect-time probe, real bind happens per-call" split.
// Safe because VGI's own bind()/on_bind() contract is documented (and,
// for the correlated-argument case, provably has no other option) to
// resolve a function's output schema from argument TYPES, not values - a
// correlated argument's real value isn't even knowable at DuckDB's own
// bind/plan time either, so a worker's on_bind() can't be relying on
// genuine values in the first place.
#include "vtab/vgi_table_function_vtab.h"

#include <algorithm>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <arrow/api.h>

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3

#include "catalog/catalog_client.h"
#include "catalog/table_scanner.h"
#include "sql_quote.h"
#include "types/type_mapping.h"
#include "vtab/connection_pool.h"
#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {

std::map<std::string, std::string> ParseModuleArgs(int argc, const char* const* argv) {
    std::map<std::string, std::string> args;
    for (int i = 3; i < argc; ++i) {
        std::string token(argv[i]);
        auto eq = token.find('=');
        if (eq == std::string::npos) continue;
        std::string key = token.substr(0, eq);
        std::string value = token.substr(eq + 1);
        auto trim = [](std::string& s) {
            size_t b = s.find_first_not_of(" \t\n");
            size_t e = s.find_last_not_of(" \t\n");
            s = (b == std::string::npos) ? "" : s.substr(b, e - b + 1);
        };
        trim(key);
        trim(value);
        if (value.size() >= 2 && (value.front() == '\'' || value.front() == '"') &&
            value.back() == value.front()) {
            value = value.substr(1, value.size() - 2);
        }
        args[key] = value;
    }
    return args;
}

// True iff `field` carries VGI's "vgi_arg: named" metadata - see
// table_in_out_caller.cpp's identical helper/file comment for the full
// background (this is a general VGI argument-model concept, not specific
// to either table_in_out or plain table functions).
bool IsNamedArgument(const std::shared_ptr<arrow::Field>& field) {
    auto metadata = field->metadata();
    if (!metadata) return false;
    auto value = metadata->Get("vgi_arg");
    return value.ok() && *value == "named";
}

// {args: struct<positional_N | named_<name>, ...>}, one row, built from
// `values` (one scalar per field of `argument_schema`, same order) -
// VGI's own BindRequest.arguments wire convention (vgi-python's
// Arguments.decode() recognizes both prefixes on the same struct).
// Positional numbering uses each field's own declared index directly
// (not a separately-tracked counter): VGI's own schema serialization
// (argument_spec.py) always sorts positional args first, by index,
// before any named ones, so a positional field's declared index already
// equals its true positional_N number.
std::vector<uint8_t> BuildScanFunctionArgs(const std::shared_ptr<arrow::Schema>& argument_schema,
                                           const std::vector<std::shared_ptr<arrow::Scalar>>& values) {
    arrow::FieldVector fields;
    fields.reserve(argument_schema->num_fields());
    for (int i = 0; i < argument_schema->num_fields(); ++i) {
        const auto& field = argument_schema->field(i);
        std::string wire_name =
            (IsNamedArgument(field) ? "named_" : "positional_") + (IsNamedArgument(field) ? field->name() : std::to_string(i));
        fields.push_back(arrow::field(wire_name, field->type(), field->nullable()));
    }

    // Same StructBuilder/AppendScalar construction as
    // TableScanner::WrapAsArgsStruct and TableInOutCaller::
    // BuildNamedArgsStruct - always exactly one row, regardless of source.
    auto struct_type = arrow::struct_(fields);
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto make_status = arrow::MakeBuilder(arrow::default_memory_pool(), struct_type, &builder);
    if (!make_status.ok()) throw std::runtime_error("building args struct builder: " + make_status.ToString());
    auto* struct_builder = static_cast<arrow::StructBuilder*>(builder.get());
    if (auto status = struct_builder->Append(); !status.ok()) {
        throw std::runtime_error("appending args row: " + status.ToString());
    }
    for (size_t i = 0; i < values.size(); ++i) {
        auto* field_builder = struct_builder->field_builder(static_cast<int>(i));
        if (auto status = field_builder->AppendScalar(*values[i]); !status.ok()) {
            throw std::runtime_error("appending argument " + std::to_string(i) + ": " + status.ToString());
        }
    }
    auto finish_result = struct_builder->Finish();
    if (!finish_result.ok()) throw std::runtime_error("finishing args struct: " + finish_result.status().ToString());

    auto args_schema = arrow::schema({arrow::field("args", struct_type, false)});
    auto args_batch = arrow::RecordBatch::Make(args_schema, 1, {finish_result.ValueUnsafe()});
    std::string encoded = wire::encode_ipc(args_batch);
    return {encoded.begin(), encoded.end()};
}

// A placeholder value for `type`, used only for xConnect's throwaway
// output-schema probe (see the file comment on why real values aren't
// needed/available there).
std::shared_ptr<arrow::Scalar> PlaceholderScalar(const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
        case arrow::Type::BOOL:
            return arrow::MakeScalar(false);
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
            return arrow::MakeScalar(static_cast<int64_t>(0))->CastTo(type).ValueOrDie();
        case arrow::Type::UINT8:
        case arrow::Type::UINT16:
        case arrow::Type::UINT32:
        case arrow::Type::UINT64:
            return arrow::MakeScalar(static_cast<uint64_t>(0))->CastTo(type).ValueOrDie();
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE:
            return arrow::MakeScalar(0.0)->CastTo(type).ValueOrDie();
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
            return arrow::MakeScalar(std::string(""));
        default:
            // Best-effort null of the declared type for anything else
            // (binary, temporal, ...) - a probe-only value, never sent to
            // real data logic.
            return arrow::MakeNullScalar(type);
    }
}

// One vgi_table_function table. Doesn't hold a connection long-term (same
// reasoning as VgiVtab/VgiTableInOutVtab) - argument_schema/output_schema
// are resolved once at xConnect/xCreate and cached here; every real query
// gets its own fresh TableScanner per xFilter call (no persistent caller
// needed - see the header's file comment on why this module doesn't need
// TableInOutCaller's "bind once, exchange many" design).
struct VgiTableFunctionVtab {
    sqlite3_vtab base;  // must be first member (sqlite3 vtab ABI)
    ConnectionPool* pool = nullptr;
    std::string location;
    std::string catalog_name;
    std::string schema_name;    // where the FUNCTION is registered
    std::string function_name;
    std::shared_ptr<arrow::Schema> argument_schema;  // declared arg names+types; HIDDEN column order
    std::shared_ptr<arrow::Schema> output_schema;    // bind()-resolved; real output column order
    bool supports_splits = false;
};

struct VgiTableFunctionCursor {
    sqlite3_vtab_cursor base;  // must be first member
    // Declared before `scanner` so it's destroyed *after* scanner (same
    // field-order contract as VgiCursor in vgi_vtab.cpp).
    std::optional<ConnectionPool::Checkout> checkout;
    std::unique_ptr<TableScanner> scanner;
    std::shared_ptr<arrow::RecordBatch> current_batch;
    std::shared_ptr<arrow::RecordBatch> current_args_row;  // for reading a HIDDEN column back
    int64_t row_in_batch = 0;
    int64_t rowid = 0;
    // AdvanceBatch (below) only ever sets this true, on a genuine
    // end-of-stream Next() - it never resets it false on a successful
    // fetch (matching vgi_vtab.cpp's own AdvanceBatch contract, which
    // this one is a direct copy of), so this must default false, not
    // true - a stale true default here left every scan reporting EOF
    // immediately even after a real, non-empty first batch. Confirmed
    // the hard way: a debug trace showed AdvanceBatch correctly fetching
    // a 25-row batch while cursor->eof was already (wrongly) true.
    bool eof = false;
};

int SetVtabError(sqlite3_vtab* vtab, const std::string& message) {
    if (vtab->zErrMsg) sqlite3_free(vtab->zErrMsg);
    vtab->zErrMsg = sqlite3_mprintf("%s", message.c_str());
    return SQLITE_ERROR;
}

int ConnectImpl(sqlite3* db, void* pAux, int argc, const char* const* argv, sqlite3_vtab** vtab_out,
                char** err) {
    auto* pool = reinterpret_cast<ConnectionPool*>(pAux);
    auto args = ParseModuleArgs(argc, argv);
    auto require = [&](const char* key) -> std::optional<std::string> {
        auto it = args.find(key);
        if (it == args.end() || it->second.empty()) return std::nullopt;
        return it->second;
    };
    auto location = require("location");
    auto catalog_name = require("catalog");
    auto schema_name = require("schema");
    auto function_name = require("function");
    if (!location || !catalog_name || !schema_name || !function_name) {
        *err = sqlite3_mprintf(
            "vgi_table_function requires location=, catalog=, schema=, and function= arguments");
        return SQLITE_ERROR;
    }
    // Optional: disambiguates which overload of `function_name` this
    // particular vtab instance means - see vgi_table_in_out_vtab.cpp's
    // matching comment and catalog_client.h's PlainTableFunctionGet
    // comment for the full story (the identical bug class, same fix).
    std::optional<int> arity;
    if (auto arity_str = require("arity")) {
        arity = std::stoi(*arity_str);
    }

    auto vtab = std::make_unique<VgiTableFunctionVtab>();
    std::memset(&vtab->base, 0, sizeof(vtab->base));
    vtab->pool = pool;
    vtab->location = *location;
    vtab->catalog_name = *catalog_name;
    vtab->schema_name = *schema_name;
    vtab->function_name = *function_name;
    try {
        auto checkout = pool->Acquire(*location, *catalog_name);
        VgiCatalogClient catalog(checkout->connection);
        auto fn = catalog.PlainTableFunctionGet(checkout->attach_opaque_data, *schema_name, *function_name, arity);
        vtab->argument_schema = fn.argument_schema;
        vtab->supports_splits = fn.supports_splits;

        std::vector<std::shared_ptr<arrow::Scalar>> placeholders;
        for (int i = 0; i < vtab->argument_schema->num_fields(); ++i) {
            placeholders.push_back(PlaceholderScalar(vtab->argument_schema->field(i)->type()));
        }
        ScanFunction scan_function;
        scan_function.function_name = fn.function_name;
        scan_function.arguments_ipc_bytes = BuildScanFunctionArgs(vtab->argument_schema, placeholders);
        scan_function.supports_splits = fn.supports_splits;
        scan_function.arguments_already_wrapped = true;

        TableScanner scanner(checkout->connection, checkout->attach_opaque_data);
        scanner.Bind(scan_function, fn.schema_name);
        vtab->output_schema = scanner.output_schema();
    } catch (const std::exception& e) {
        *err = sqlite3_mprintf("vgi_table_function: %s", e.what());
        return SQLITE_ERROR;
    }

    // Output/hidden column name collisions are common in practice (see
    // vgi_table_in_out_vtab.cpp's ConnectImpl for the exact same
    // disambiguation, found against a real deployed worker) - identical
    // handling here.
    std::vector<std::string> declared_names;
    for (int i = 0; i < vtab->output_schema->num_fields(); ++i) {
        declared_names.push_back(vtab->output_schema->field(i)->name());
    }
    auto disambiguate = [&](const std::string& name) {
        if (std::find(declared_names.begin(), declared_names.end(), name) == declared_names.end()) return name;
        for (int suffix = 1;; ++suffix) {
            std::string candidate = name + "_arg" + (suffix == 1 ? "" : std::to_string(suffix));
            if (std::find(declared_names.begin(), declared_names.end(), candidate) == declared_names.end()) {
                return candidate;
            }
        }
    };

    std::ostringstream ddl;
    ddl << "CREATE TABLE x(";
    bool first = true;
    for (int i = 0; i < vtab->output_schema->num_fields(); ++i) {
        if (!first) ddl << ", ";
        first = false;
        const auto& field = vtab->output_schema->field(i);
        ddl << SqlQuoteIdentifier(field->name()) << " " << SqliteDeclaredType(field->type());
    }
    for (int i = 0; i < vtab->argument_schema->num_fields(); ++i) {
        if (!first) ddl << ", ";
        first = false;
        const auto& field = vtab->argument_schema->field(i);
        std::string hidden_name = disambiguate(field->name());
        declared_names.push_back(hidden_name);
        ddl << SqlQuoteIdentifier(hidden_name) << " " << SqliteDeclaredType(field->type()) << " HIDDEN";
    }
    ddl << ")";
    if (int rc = sqlite3_declare_vtab(db, ddl.str().c_str()); rc != SQLITE_OK) {
        *err = sqlite3_mprintf("vgi_table_function: sqlite3_declare_vtab failed: %s", ddl.str().c_str());
        return rc;
    }

    *vtab_out = reinterpret_cast<sqlite3_vtab*>(vtab.release());
    return SQLITE_OK;
}

int xCreate(sqlite3* db, void* pAux, int argc, const char* const* argv, sqlite3_vtab** vtab_out,
            char** err) {
    return ConnectImpl(db, pAux, argc, argv, vtab_out, err);
}

int xConnect(sqlite3* db, void* pAux, int argc, const char* const* argv, sqlite3_vtab** vtab_out,
             char** err) {
    return ConnectImpl(db, pAux, argc, argv, vtab_out, err);
}

int xDisconnect(sqlite3_vtab* vtab) {
    delete reinterpret_cast<VgiTableFunctionVtab*>(vtab);
    return SQLITE_OK;
}

int xDestroy(sqlite3_vtab* vtab) { return xDisconnect(vtab); }

// Same required-EQ-on-every-hidden-column / fixed-argvIndex-order /
// SQLITE_CONSTRAINT-on-missing design as vgi_table_in_out_vtab.cpp's own
// xBestIndex - see that file's comment for the full reasoning (equally
// applicable here: a table function call with a missing argument can't
// be bound at all, any more than a missing scalar-function argument
// could).
int xBestIndex(sqlite3_vtab* base_vtab, sqlite3_index_info* info) {
    auto* vtab = reinterpret_cast<VgiTableFunctionVtab*>(base_vtab);
    const int num_output = vtab->output_schema->num_fields();
    const int num_hidden = vtab->argument_schema->num_fields();

    for (int hidden_i = 0; hidden_i < num_hidden; ++hidden_i) {
        const int declared_col = num_output + hidden_i;
        bool found = false;
        for (int j = 0; j < info->nConstraint; ++j) {
            const auto& constraint = info->aConstraint[j];
            if (constraint.usable && constraint.iColumn == declared_col &&
                constraint.op == SQLITE_INDEX_CONSTRAINT_EQ) {
                info->aConstraintUsage[j].argvIndex = hidden_i + 1;
                info->aConstraintUsage[j].omit = 1;
                found = true;
                break;
            }
        }
        if (!found) return SQLITE_CONSTRAINT;
    }

    // A per-call RPC round trip (bind+init+at least one tick) is real
    // work - same modest placeholder as vgi_table_in_out_vtab.cpp's own
    // xBestIndex, for the identical reason (no worker-declared cardinality
    // exists for a function call the way a real table has one).
    info->estimatedCost = 100.0;
    info->estimatedRows = 1;
    return SQLITE_OK;
}

int xOpen(sqlite3_vtab*, sqlite3_vtab_cursor** cursor_out) {
    auto cursor = std::make_unique<VgiTableFunctionCursor>();
    std::memset(&cursor->base, 0, sizeof(cursor->base));
    *cursor_out = reinterpret_cast<sqlite3_vtab_cursor*>(cursor.release());
    return SQLITE_OK;
}

int xClose(sqlite3_vtab_cursor* cursor) {
    delete reinterpret_cast<VgiTableFunctionCursor*>(cursor);
    return SQLITE_OK;
}

// Pull the next non-empty batch (or set eof) - same pattern as
// vgi_vtab.cpp's own AdvanceBatch.
void AdvanceBatch(VgiTableFunctionCursor* cursor) {
    auto next = cursor->scanner->Next();
    if (!next) {
        cursor->eof = true;
        cursor->current_batch = nullptr;
        return;
    }
    cursor->current_batch = *next;
    cursor->row_in_batch = 0;
}

int xFilter(sqlite3_vtab_cursor* base_cursor, int, const char*, int argc, sqlite3_value** argv) {
    auto* cursor = reinterpret_cast<VgiTableFunctionCursor*>(base_cursor);
    auto* vtab = reinterpret_cast<VgiTableFunctionVtab*>(base_cursor->pVtab);
    const int num_hidden = vtab->argument_schema->num_fields();
    if (argc != num_hidden) {
        return SetVtabError(base_cursor->pVtab, "vgi_table_function: internal error - argument count mismatch");
    }
    try {
        std::vector<std::shared_ptr<arrow::Scalar>> values;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        for (int i = 0; i < num_hidden; ++i) {
            auto scalar = BuildArrowScalarFromSqliteValue(argv[i], vtab->argument_schema->field(i)->type());
            if (!scalar) {
                return SetVtabError(base_cursor->pVtab, "vgi_table_function: argument \"" +
                                                             vtab->argument_schema->field(i)->name() +
                                                             "\" doesn't match its declared type");
            }
            values.push_back(scalar);
            auto array_result = arrow::MakeArrayFromScalar(*scalar, 1);
            if (!array_result.ok()) {
                return SetVtabError(base_cursor->pVtab, "vgi_table_function: building argument \"" +
                                                             vtab->argument_schema->field(i)->name() +
                                                             "\": " + array_result.status().ToString());
            }
            columns.push_back(array_result.ValueUnsafe());
        }
        cursor->current_args_row = arrow::RecordBatch::Make(vtab->argument_schema, 1, columns);

        // Fresh checkout + scanner every call - no persistence needed
        // across xFilter calls on the same cursor (see the header's file
        // comment). Assigning over any prior checkout/scanner releases
        // the previous ones (scanner destructs - and closes its stream -
        // before checkout releases the connection, same field-order
        // contract as VgiCursor).
        cursor->checkout = vtab->pool->Acquire(vtab->location, vtab->catalog_name);
        cursor->scanner = std::make_unique<TableScanner>((*cursor->checkout)->connection,
                                                          (*cursor->checkout)->attach_opaque_data);

        ScanFunction scan_function;
        scan_function.function_name = vtab->function_name;
        scan_function.arguments_ipc_bytes = BuildScanFunctionArgs(vtab->argument_schema, values);
        scan_function.supports_splits = vtab->supports_splits;
        scan_function.arguments_already_wrapped = true;
        cursor->scanner->Bind(scan_function, vtab->schema_name,
                             vtab->pool->CurrentTransactionOpaqueData(vtab->location, vtab->catalog_name));
        // No projection/filter/limit pushdown for v1 - see the header's
        // file comment.
        cursor->scanner->Init();
        cursor->rowid = 0;
        // Reset explicitly, not left to AdvanceBatch's own contract
        // (which only ever SETS eof true, on a genuine end-of-stream - it
        // never clears it) - unlike a plain table's cursor (xFilter called
        // once per cursor lifetime), THIS cursor can be re-filtered many
        // times on the same cursor object for a correlated scan (one call
        // per outer row - see the header's file comment), so a PRIOR
        // scan's completion (eof left true from ITS OWN last AdvanceBatch)
        // would otherwise stay stuck true forever, silently reporting
        // every subsequent correlated row as producing zero results.
        // Found the hard way: `FROM nums, fn(nums.n, ...)` correctly
        // scanned the FIRST outer row but silently returned nothing for
        // every row after it.
        cursor->eof = false;
        AdvanceBatch(cursor);
    } catch (const std::exception& e) {
        return SetVtabError(base_cursor->pVtab, e.what());
    }
    return SQLITE_OK;
}

int xNext(sqlite3_vtab_cursor* base_cursor) {
    auto* cursor = reinterpret_cast<VgiTableFunctionCursor*>(base_cursor);
    ++cursor->rowid;
    ++cursor->row_in_batch;
    if (!cursor->current_batch || cursor->row_in_batch >= cursor->current_batch->num_rows()) {
        try {
            AdvanceBatch(cursor);
        } catch (const std::exception& e) {
            return SetVtabError(base_cursor->pVtab, e.what());
        }
    }
    return SQLITE_OK;
}

int xEof(sqlite3_vtab_cursor* base_cursor) {
    return reinterpret_cast<VgiTableFunctionCursor*>(base_cursor)->eof ? 1 : 0;
}

int xColumn(sqlite3_vtab_cursor* base_cursor, sqlite3_context* ctx, int col_idx) {
    auto* cursor = reinterpret_cast<VgiTableFunctionCursor*>(base_cursor);
    auto* vtab = reinterpret_cast<VgiTableFunctionVtab*>(base_cursor->pVtab);
    const int num_output = vtab->output_schema->num_fields();
    if (col_idx < num_output) {
        if (!cursor->current_batch || col_idx >= cursor->current_batch->num_columns()) {
            sqlite3_result_null(ctx);
            return SQLITE_OK;
        }
        SetSqliteResultFromArrow(ctx, *cursor->current_batch->column(col_idx), cursor->row_in_batch);
        return SQLITE_OK;
    }
    const int hidden_i = col_idx - num_output;
    if (!cursor->current_args_row || hidden_i >= cursor->current_args_row->num_columns()) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    SetSqliteResultFromArrow(ctx, *cursor->current_args_row->column(hidden_i), 0);
    return SQLITE_OK;
}

int xRowid(sqlite3_vtab_cursor* base_cursor, sqlite3_int64* rowid_out) {
    *rowid_out = reinterpret_cast<VgiTableFunctionCursor*>(base_cursor)->rowid;
    return SQLITE_OK;
}

const sqlite3_module kVgiTableFunctionModule = {
    /* iVersion */ 0,
    /* xCreate */ xCreate,
    /* xConnect */ xConnect,
    /* xBestIndex */ xBestIndex,
    /* xDisconnect */ xDisconnect,
    /* xDestroy */ xDestroy,
    /* xOpen */ xOpen,
    /* xClose */ xClose,
    /* xFilter */ xFilter,
    /* xNext */ xNext,
    /* xEof */ xEof,
    /* xColumn */ xColumn,
    /* xRowid */ xRowid,
    /* xUpdate */ nullptr,  // read-only - see the header's file comment
    /* xBegin */ nullptr,   // no transaction coordination - see the header's file comment
    /* xSync */ nullptr,
    /* xCommit */ nullptr,
    /* xRollback */ nullptr,
    /* xFindFunction */ nullptr,
    /* xRename */ nullptr,
    /* xSavepoint */ nullptr,
    /* xRelease */ nullptr,
    /* xRollbackTo */ nullptr,
    /* xShadowName */ nullptr,
    /* xIntegrity */ nullptr,
};

}  // namespace

int RegisterVgiTableFunctionModule(sqlite3* db, ConnectionPool* pool) {
    return sqlite3_create_module_v2(db, "vgi_table_function", &kVgiTableFunctionModule, pool, nullptr);
}

}  // namespace vgi_sqlite
