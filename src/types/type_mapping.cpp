// © Copyright 2026 Query Farm LLC - https://query.farm
#include "types/type_mapping.h"

#include <chrono>
#include <cstdio>
#include <limits>

#include <nlohmann/json.hpp>

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3

namespace vgi_sqlite {
namespace {

ArrowSchemaView View(const ArrowSchema& schema) {
    ArrowSchemaView view;
    ArrowError error;
    ArrowErrorInit(&error);
    nano::Check(ArrowSchemaViewInit(&view, &schema, &error), error, "inspect Arrow type");
    return view;
}

std::string DeclaredTypeFor(enum ArrowType type) {
    switch (type) {
        case NANOARROW_TYPE_BOOL:
        case NANOARROW_TYPE_INT8:
        case NANOARROW_TYPE_INT16:
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_INT64:
        case NANOARROW_TYPE_UINT8:
        case NANOARROW_TYPE_UINT16:
        case NANOARROW_TYPE_UINT32:
        case NANOARROW_TYPE_UINT64: return "INTEGER";
        case NANOARROW_TYPE_HALF_FLOAT:
        case NANOARROW_TYPE_FLOAT:
        case NANOARROW_TYPE_DOUBLE: return "REAL";
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY:
        case NANOARROW_TYPE_FIXED_SIZE_BINARY: return "BLOB";
        default: return "TEXT";
    }
}

std::string FormatDate32(int32_t days_since_epoch) {
    using namespace std::chrono;
    year_month_day ymd{sys_days{days{days_since_epoch}}};
    char buffer[16];
    std::snprintf(buffer, sizeof(buffer), "%04d-%02u-%02u", static_cast<int>(ymd.year()),
                  static_cast<unsigned>(ymd.month()), static_cast<unsigned>(ymd.day()));
    return buffer;
}

std::string FormatTimestamp(int64_t value, enum ArrowTimeUnit unit, bool has_timezone) {
    using namespace std::chrono;
    int64_t micros = value;
    switch (unit) {
        case NANOARROW_TIME_UNIT_SECOND: micros = value * 1'000'000; break;
        case NANOARROW_TIME_UNIT_MILLI: micros = value * 1'000; break;
        case NANOARROW_TIME_UNIT_MICRO: break;
        case NANOARROW_TIME_UNIT_NANO: micros = value / 1'000; break;
    }
    int64_t day = micros / 86'400'000'000LL;
    int64_t remainder = micros % 86'400'000'000LL;
    if (remainder < 0) {
        remainder += 86'400'000'000LL;
        --day;
    }
    year_month_day ymd{sys_days{days{static_cast<int32_t>(day)}}};
    const auto hour = remainder / 3'600'000'000LL;
    remainder %= 3'600'000'000LL;
    const auto minute = remainder / 60'000'000LL;
    remainder %= 60'000'000LL;
    const auto second = remainder / 1'000'000LL;
    const auto micro = remainder % 1'000'000LL;
    char buffer[40];
    if (micro) {
        std::snprintf(buffer, sizeof(buffer), "%04d-%02u-%02u %02lld:%02lld:%02lld.%06lld%s",
                      static_cast<int>(ymd.year()), static_cast<unsigned>(ymd.month()),
                      static_cast<unsigned>(ymd.day()), static_cast<long long>(hour),
                      static_cast<long long>(minute), static_cast<long long>(second),
                      static_cast<long long>(micro), has_timezone ? "Z" : "");
    } else {
        std::snprintf(buffer, sizeof(buffer), "%04d-%02u-%02u %02lld:%02lld:%02lld%s",
                      static_cast<int>(ymd.year()), static_cast<unsigned>(ymd.month()),
                      static_cast<unsigned>(ymd.day()), static_cast<long long>(hour),
                      static_cast<long long>(minute), static_cast<long long>(second),
                      has_timezone ? "Z" : "");
    }
    return buffer;
}

nlohmann::json ScalarToJson(const nano::Column& column, int64_t row);

nlohmann::json ListToJson(const nano::Column& column, int64_t row) {
    auto result = nlohmann::json::array();
    auto values = column.child(0);
    for (int64_t i = column.Offset(row); i < column.Offset(row + 1); ++i)
        result.push_back(ScalarToJson(values, i));
    return result;
}

nlohmann::json StructToJson(const nano::Column& column, int64_t row) {
    auto result = nlohmann::json::object();
    for (int64_t i = 0; i < column.schema->n_children; ++i) {
        const auto* field = column.schema->children[i];
        result[field->name ? field->name : std::to_string(i)] = ScalarToJson(column.child(i), row);
    }
    return result;
}

nlohmann::json MapToJson(const nano::Column& column, int64_t row) {
    auto result = nlohmann::json::object();
    auto entries = column.child(0);
    auto keys = entries.child(0);
    auto values = entries.child(1);
    for (int64_t i = column.Offset(row); i < column.Offset(row + 1); ++i) {
        auto key = ScalarToJson(keys, i);
        result[key.is_string() ? key.get<std::string>() : key.dump()] = ScalarToJson(values, i);
    }
    return result;
}

nlohmann::json ScalarToJson(const nano::Column& column, int64_t row) {
    if (column.IsNull(row)) return nullptr;
    switch (column.type_id()) {
        case NANOARROW_TYPE_BOOL: return column.Bool(row);
        case NANOARROW_TYPE_INT8:
        case NANOARROW_TYPE_INT16:
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_INT64: return column.Int(row);
        case NANOARROW_TYPE_UINT8:
        case NANOARROW_TYPE_UINT16:
        case NANOARROW_TYPE_UINT32:
        case NANOARROW_TYPE_UINT64: return column.UInt(row);
        case NANOARROW_TYPE_HALF_FLOAT:
        case NANOARROW_TYPE_FLOAT:
        case NANOARROW_TYPE_DOUBLE: return column.Double(row);
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING: return column.String(row);
        case NANOARROW_TYPE_STRUCT: return StructToJson(column, row);
        case NANOARROW_TYPE_LIST:
        case NANOARROW_TYPE_LARGE_LIST:
        case NANOARROW_TYPE_FIXED_SIZE_LIST: return ListToJson(column, row);
        case NANOARROW_TYPE_MAP: return MapToJson(column, row);
        default: return column.TypeString();
    }
}

void SetText(sqlite3_context* context, const std::string& value) {
    sqlite3_result_text64(context, value.data(), static_cast<sqlite3_uint64>(value.size()),
                          SQLITE_TRANSIENT, SQLITE_UTF8);
}

void SetBlob(sqlite3_context* context, const std::vector<uint8_t>& value) {
    sqlite3_result_blob64(context, value.data(), static_cast<sqlite3_uint64>(value.size()),
                          SQLITE_TRANSIENT);
}

std::string DecimalString(const nano::Column& column, int64_t row) {
    auto type = View(*column.schema);
    ArrowDecimal decimal;
    ArrowDecimalInit(&decimal, type.decimal_bitwidth, type.decimal_precision, type.decimal_scale);
    ArrowArrayViewGetDecimalUnsafe(column.view, row, &decimal);
    ArrowBuffer buffer;
    ArrowBufferInit(&buffer);
    nano::Check(ArrowDecimalAppendStringToBuffer(&decimal, &buffer), "format decimal");
    std::string result(reinterpret_cast<const char*>(buffer.data),
                       static_cast<size_t>(buffer.size_bytes));
    ArrowBufferReset(&buffer);
    return result;
}

}  // namespace

std::string SqliteDeclaredType(const ArrowSchema& type) {
    return DeclaredTypeFor(nano::TypeId(type));
}

void SetSqliteResultFromArrow(sqlite3_context* context, const nano::Column& column, int64_t row) {
    if (column.IsNull(row)) return sqlite3_result_null(context);
    switch (column.type_id()) {
        case NANOARROW_TYPE_BOOL: return sqlite3_result_int(context, column.Bool(row) ? 1 : 0);
        case NANOARROW_TYPE_INT8:
        case NANOARROW_TYPE_INT16:
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_INT64: return sqlite3_result_int64(context, column.Int(row));
        case NANOARROW_TYPE_UINT8:
        case NANOARROW_TYPE_UINT16:
        case NANOARROW_TYPE_UINT32:
        case NANOARROW_TYPE_UINT64: {
            auto value = column.UInt(row);
            if (value > static_cast<uint64_t>(std::numeric_limits<sqlite3_int64>::max()))
                return SetText(context, std::to_string(value));
            return sqlite3_result_int64(context, static_cast<sqlite3_int64>(value));
        }
        case NANOARROW_TYPE_HALF_FLOAT:
        case NANOARROW_TYPE_FLOAT:
        case NANOARROW_TYPE_DOUBLE: return sqlite3_result_double(context, column.Double(row));
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING: return SetText(context, column.String(row));
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY:
        case NANOARROW_TYPE_FIXED_SIZE_BINARY: return SetBlob(context, column.Bytes(row));
        case NANOARROW_TYPE_DATE32:
            return SetText(context, FormatDate32(static_cast<int32_t>(column.Int(row))));
        case NANOARROW_TYPE_DATE64:
            return SetText(context, FormatDate32(static_cast<int32_t>(column.Int(row) / 86'400'000)));
        case NANOARROW_TYPE_TIMESTAMP: {
            auto type = View(*column.schema);
            return SetText(context, FormatTimestamp(column.Int(row), type.time_unit,
                                                     type.timezone && type.timezone[0]));
        }
        case NANOARROW_TYPE_DECIMAL128:
        case NANOARROW_TYPE_DECIMAL256: return SetText(context, DecimalString(column, row));
        case NANOARROW_TYPE_STRUCT:
        case NANOARROW_TYPE_LIST:
        case NANOARROW_TYPE_LARGE_LIST:
        case NANOARROW_TYPE_FIXED_SIZE_LIST:
        case NANOARROW_TYPE_MAP: return SetText(context, ScalarToJson(column, row).dump());
        default: return sqlite3_result_error(context, "unsupported Arrow type for SQLite result", -1);
    }
}

nano::ScalarPtr BuildArrowScalarFromSqliteValue(sqlite3_value* value,
                                                 const ArrowSchema& target_type) {
    auto type = nano::Schema::Copy(target_type);
    if (sqlite3_value_type(value) == SQLITE_NULL) return nano::Scalar::Null(type);
    switch (nano::TypeId(target_type)) {
        case NANOARROW_TYPE_BOOL:
        case NANOARROW_TYPE_INT8:
        case NANOARROW_TYPE_INT16:
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_INT64:
            if (sqlite3_value_type(value) != SQLITE_INTEGER) return nullptr;
            return nano::Scalar::Signed(sqlite3_value_int64(value), type);
        case NANOARROW_TYPE_UINT8:
        case NANOARROW_TYPE_UINT16:
        case NANOARROW_TYPE_UINT32:
        case NANOARROW_TYPE_UINT64:
            if (sqlite3_value_type(value) != SQLITE_INTEGER || sqlite3_value_int64(value) < 0)
                return nullptr;
            return nano::Scalar::Unsigned(static_cast<uint64_t>(sqlite3_value_int64(value)), type);
        case NANOARROW_TYPE_HALF_FLOAT:
        case NANOARROW_TYPE_FLOAT:
        case NANOARROW_TYPE_DOUBLE:
            if (sqlite3_value_type(value) != SQLITE_FLOAT && sqlite3_value_type(value) != SQLITE_INTEGER)
                return nullptr;
            return nano::Scalar::Real(sqlite3_value_double(value), type);
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
            const int size = sqlite3_value_bytes(value);
            return nano::Scalar::Binary(data ? std::vector<uint8_t>(data, data + size)
                                             : std::vector<uint8_t>{}, type);
        }
        default: return nullptr;
    }
}

nano::ScalarPtr BuildArrowScalarFromSqliteValueNatural(sqlite3_value* value) {
    switch (sqlite3_value_type(value)) {
        case SQLITE_INTEGER: return nano::Scalar::Signed(sqlite3_value_int64(value));
        case SQLITE_FLOAT: return nano::Scalar::Real(sqlite3_value_double(value));
        case SQLITE_TEXT: {
            const auto* text = reinterpret_cast<const char*>(sqlite3_value_text(value));
            return nano::Scalar::String(text ? text : "");
        }
        case SQLITE_BLOB: {
            const auto* data = reinterpret_cast<const uint8_t*>(sqlite3_value_blob(value));
            const int size = sqlite3_value_bytes(value);
            return nano::Scalar::Binary(data ? std::vector<uint8_t>(data, data + size)
                                             : std::vector<uint8_t>{});
        }
        default: return nullptr;
    }
}

}  // namespace vgi_sqlite
