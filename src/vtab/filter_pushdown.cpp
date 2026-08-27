// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vtab/filter_pushdown.h"

#include <algorithm>

#include <arrow/api.h>
#include <nlohmann/json.hpp>

// See vgi_vtab.cpp's file comment: only ever compiled into vgi_extension,
// must resolve sqlite3_* through the host's function table.
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3

#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {

// eq/ne/gt/ge/lt/le per docs/filter-pushdown.md; nullopt for anything not
// pushed (LIKE/GLOB/REGEXP/MATCH/IS/ISNOT/FUNCTION/LIMIT/OFFSET - left for
// SQLite to evaluate itself, same as it always does for a constraint this
// vtab doesn't claim at all).
std::optional<std::string> OpName(unsigned char op) {
    switch (op) {
        case SQLITE_INDEX_CONSTRAINT_EQ: return "eq";
        case SQLITE_INDEX_CONSTRAINT_NE: return "ne";
        case SQLITE_INDEX_CONSTRAINT_GT: return "gt";
        case SQLITE_INDEX_CONSTRAINT_GE: return "ge";
        case SQLITE_INDEX_CONSTRAINT_LT: return "lt";
        case SQLITE_INDEX_CONSTRAINT_LE: return "le";
        default: return std::nullopt;
    }
}

bool IsNullCheck(unsigned char op) {
    return op == SQLITE_INDEX_CONSTRAINT_ISNULL || op == SQLITE_INDEX_CONSTRAINT_ISNOTNULL;
}

// One 1-row Arrow array holding `value`, typed to match `column_type` as
// closely as this encoder handles - nullptr for a column/value type
// combination it doesn't (temporal, decimal, nested, or a value SQLite
// can't coerce into the column's declared storage class), which the
// caller treats as "don't push this one".
std::shared_ptr<arrow::Array> BuildValueArray(sqlite3_value* value,
                                               const std::shared_ptr<arrow::DataType>& column_type) {
    switch (column_type->id()) {
        case arrow::Type::BOOL:
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::UINT8:
        case arrow::Type::UINT16:
        case arrow::Type::UINT32:
        case arrow::Type::UINT64: {
            if (sqlite3_value_type(value) != SQLITE_INTEGER) return nullptr;
            arrow::Int64Builder builder;
            if (!builder.Append(sqlite3_value_int64(value)).ok()) return nullptr;
            auto result = builder.Finish();
            return result.ok() ? result.ValueUnsafe() : nullptr;
        }
        case arrow::Type::HALF_FLOAT:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE: {
            auto vtype = sqlite3_value_type(value);
            if (vtype != SQLITE_FLOAT && vtype != SQLITE_INTEGER) return nullptr;
            arrow::DoubleBuilder builder;
            if (!builder.Append(sqlite3_value_double(value)).ok()) return nullptr;
            auto result = builder.Finish();
            return result.ok() ? result.ValueUnsafe() : nullptr;
        }
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING: {
            if (sqlite3_value_type(value) != SQLITE_TEXT) return nullptr;
            const auto* text = reinterpret_cast<const char*>(sqlite3_value_text(value));
            arrow::StringBuilder builder;
            if (!builder.Append(text ? text : "").ok()) return nullptr;
            auto result = builder.Finish();
            return result.ok() ? result.ValueUnsafe() : nullptr;
        }
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY: {
            if (sqlite3_value_type(value) != SQLITE_BLOB) return nullptr;
            const auto* data = reinterpret_cast<const uint8_t*>(sqlite3_value_blob(value));
            int size = sqlite3_value_bytes(value);
            arrow::BinaryBuilder builder;
            if (!builder.Append(data, size).ok()) return nullptr;
            auto result = builder.Finish();
            return result.ok() ? result.ValueUnsafe() : nullptr;
        }
        default:
            return nullptr;  // temporal/decimal/nested: not pushed
    }
}

}  // namespace

std::vector<PushableConstraint> SelectPushableConstraints(sqlite3_index_info* info) {
    std::vector<PushableConstraint> selected;
    int next_argv_index = 1;
    for (int i = 0; i < info->nConstraint; ++i) {
        const auto& constraint = info->aConstraint[i];
        if (!constraint.usable || constraint.iColumn < 0) continue;  // ROWID (-1) not pushed
        if (!IsNullCheck(constraint.op) && !OpName(constraint.op)) continue;  // unsupported operator
        selected.push_back({constraint.iColumn, constraint.op});
        // Never .omit: SQLite always re-checks every pushed constraint
        // itself too - see the file comment on why that's deliberate.
        info->aConstraintUsage[i].argvIndex = next_argv_index++;
    }
    return selected;
}

std::optional<std::string> EncodePushdownFilters(const std::shared_ptr<arrow::Schema>& columns,
                                                  const std::vector<PushableConstraint>& constraints,
                                                  sqlite3_value** argv,
                                                  const std::vector<int>& projected_columns) {
    if (!columns || constraints.empty()) return std::nullopt;

    auto filter_spec = nlohmann::json::array();
    arrow::FieldVector value_fields;
    std::vector<std::shared_ptr<arrow::Array>> value_arrays;

    for (size_t i = 0; i < constraints.size(); ++i) {
        const auto& constraint = constraints[i];
        if (constraint.column_index >= columns->num_fields()) continue;
        const auto& field = columns->field(constraint.column_index);

        // See the header's file comment: the wire format wants this
        // column's position within the PROJECTED output, not its
        // declared table index - identical when nothing was narrowed
        // (empty projected_columns means "every column, declared order").
        int wire_column_index = constraint.column_index;
        if (!projected_columns.empty()) {
            auto it = std::find(projected_columns.begin(), projected_columns.end(), constraint.column_index);
            if (it == projected_columns.end()) continue;  // shouldn't happen - colUsed already covers WHERE columns
            wire_column_index = static_cast<int>(it - projected_columns.begin());
        }

        nlohmann::json entry;
        entry["column_name"] = field->name();
        entry["column_index"] = wire_column_index;

        if (IsNullCheck(constraint.op)) {
            entry["type"] = constraint.op == SQLITE_INDEX_CONSTRAINT_ISNULL ? "is_null" : "is_not_null";
        } else {
            auto value_array = BuildValueArray(argv[i], field->type());
            if (!value_array) continue;  // type mismatch/unsupported - skip this one, not the batch
            entry["type"] = "constant";
            entry["op"] = *OpName(constraint.op);
            entry["value_ref"] = value_arrays.size();
            value_fields.push_back(arrow::field("_val_" + std::to_string(value_arrays.size()), value_array->type()));
            value_arrays.push_back(value_array);
        }
        filter_spec.push_back(std::move(entry));
    }

    if (filter_spec.empty()) return std::nullopt;

    // filter_spec's field metadata carries the format version
    // (docs/filter-pushdown.md); the value it names as a plain JSON
    // string, one column, followed by the referenced _val_N columns.
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append("vgi_filter_version", "1");
    arrow::FieldVector fields = {arrow::field("filter_spec", arrow::utf8(), false, metadata)};
    fields.insert(fields.end(), value_fields.begin(), value_fields.end());

    arrow::StringBuilder spec_builder;
    if (!spec_builder.Append(filter_spec.dump()).ok()) return std::nullopt;
    auto spec_result = spec_builder.Finish();
    if (!spec_result.ok()) return std::nullopt;

    std::vector<std::shared_ptr<arrow::Array>> arrays = {spec_result.ValueUnsafe()};
    arrays.insert(arrays.end(), value_arrays.begin(), value_arrays.end());

    auto batch = arrow::RecordBatch::Make(arrow::schema(fields), 1, arrays);
    return wire::encode_ipc(batch);
}

}  // namespace vgi_sqlite
