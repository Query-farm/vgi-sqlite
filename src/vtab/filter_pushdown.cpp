// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vtab/filter_pushdown.h"

#include <nlohmann/json.hpp>

// See vgi_vtab.cpp's file comment: only ever compiled into vgi_extension,
// must resolve sqlite3_* through the host's function table.
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3

#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {

// Comparison ops per the v2 spec's section 5.4; nullopt for anything not
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
nano::ScalarPtr BuildValue(sqlite3_value* value, const ArrowSchema& column_type) {
    auto type = nano::Schema::Copy(column_type);
    switch (nano::TypeId(column_type)) {
        case NANOARROW_TYPE_BOOL: {
            // Bind-compatibility: a BOOLEAN column needs a BOOLEAN literal.
            // SQLite has no boolean class, so only 0 and 1 have a faithful one.
            if (sqlite3_value_type(value) != SQLITE_INTEGER) return nullptr;
            const auto integer = sqlite3_value_int64(value);
            if (integer != 0 && integer != 1) return nullptr;
            return nano::Scalar::Boolean(integer == 1, type);
        }
        case NANOARROW_TYPE_INT8:
        case NANOARROW_TYPE_INT16:
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_INT64: {
            if (sqlite3_value_type(value) != SQLITE_INTEGER) return nullptr;
            return nano::Scalar::Signed(sqlite3_value_int64(value), type);
        }
        case NANOARROW_TYPE_UINT8:
        case NANOARROW_TYPE_UINT16:
        case NANOARROW_TYPE_UINT32:
        case NANOARROW_TYPE_UINT64: {
            if (sqlite3_value_type(value) != SQLITE_INTEGER || sqlite3_value_int64(value) < 0)
                return nullptr;
            return nano::Scalar::Unsigned(static_cast<uint64_t>(sqlite3_value_int64(value)), type);
        }
        case NANOARROW_TYPE_HALF_FLOAT:
        case NANOARROW_TYPE_FLOAT:
        case NANOARROW_TYPE_DOUBLE: {
            auto vtype = sqlite3_value_type(value);
            if (vtype != SQLITE_FLOAT && vtype != SQLITE_INTEGER) return nullptr;
            return nano::Scalar::Real(sqlite3_value_double(value), type);
        }
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING: {
            if (sqlite3_value_type(value) != SQLITE_TEXT) return nullptr;
            const auto* text = reinterpret_cast<const char*>(sqlite3_value_text(value));
            return nano::Scalar::String(text ? text : "", type);
        }
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY: {
            if (sqlite3_value_type(value) != SQLITE_BLOB) return nullptr;
            const auto* data = reinterpret_cast<const uint8_t*>(sqlite3_value_blob(value));
            int size = sqlite3_value_bytes(value);
            return nano::Scalar::Binary(data ? std::vector<uint8_t>(data, data + size)
                                             : std::vector<uint8_t>{}, type);
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

std::optional<std::string> EncodePushdownFilters(const nano::SchemaPtr& columns,
                                                  const std::vector<PushableConstraint>& constraints,
                                                  sqlite3_value** argv) {
    if (!columns || constraints.empty()) return std::nullopt;

    auto predicates = nlohmann::json::array();
    std::vector<nano::SchemaPtr> value_fields;
    std::vector<nano::ScalarPtr> values;

    for (size_t i = 0; i < constraints.size(); ++i) {
        const auto& constraint = constraints[i];
        if (constraint.column_index >= columns->num_fields()) continue;
        const auto* field = columns->field(constraint.column_index);

        const nlohmann::json column_ref = {{"node", "column_ref"},
                                           {"column_index", constraint.column_index},
                                           {"column_name", field->name ? field->name : ""}};
        nlohmann::json expression;
        if (IsNullCheck(constraint.op)) {
            expression = {{"node", "is_null"},
                          {"expression", column_ref},
                          {"negated", constraint.op == SQLITE_INDEX_CONSTRAINT_ISNOTNULL}};
        } else {
            auto value = BuildValue(argv[i], *field);
            if (!value) continue;  // type mismatch/unsupported - skip this one, not the batch
            const auto value_ref = values.size();
            expression = {{"node", "comparison"},
                          {"op", *OpName(constraint.op)},
                          {"left", column_ref},
                          {"right", {{"node", "literal"}, {"value_ref", value_ref}}}};
            value_fields.push_back(nano::Field("value_" + std::to_string(value_ref),
                                               nano::Schema::Copy(*field)));
            values.push_back(value);
        }
        predicates.push_back({{"id", "query:" + std::to_string(predicates.size())},
                              {"revision", 0},
                              {"mode", "advisory"},
                              {"source", "query"},
                              {"expression", std::move(expression)}});
    }

    if (predicates.empty()) return std::nullopt;

    const nlohmann::json document = {{"encoding", "vgi.filters.v2"},
                                     {"semantics", "vgi.duckdb.standard.v1"},
                                     {"kind", "snapshot"},
                                     {"predicates", std::move(predicates)}};

    // Schema-level, not field-level, metadata. vgi.none.v1 is the right
    // context: every predicate is a binary comparison or NULL test, and a
    // vtab column has SQLite's default BINARY collation.
    std::vector<nano::SchemaPtr> fields = {
        nano::Field("filter_spec", nano::Type(NANOARROW_TYPE_STRING), false)};
    fields.insert(fields.end(), value_fields.begin(), value_fields.end());
    auto schema = nano::MakeSchema(fields);
    schema->SetMetadata({{"vgi_filter_encoding", "vgi.filters.v2"},
                         {"vgi_filter_version", "2"},
                         {"vgi_evaluation_context", "vgi.none.v1"}});
    std::vector<nano::ScalarPtr> scalars = {nano::Scalar::String(document.dump())};
    scalars.insert(scalars.end(), values.begin(), values.end());
    auto batch = nano::Batch::FromScalars(schema, scalars);
    return wire::encode_ipc(batch);
}

}  // namespace vgi_sqlite
