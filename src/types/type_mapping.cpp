// © Copyright 2026 Query Farm LLC - https://query.farm
#include "types/type_mapping.h"

#include <chrono>
#include <cstdio>

#include <arrow/array/array_binary.h>
#include <arrow/array/array_decimal.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <nlohmann/json.hpp>

// See vgi_vtab.cpp's file comment: this file is only ever compiled into
// the vgi_extension loadable module and must resolve sqlite3_* calls
// through the host's function-pointer table, not a second linked copy.
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3

namespace vgi_sqlite {
namespace {

// docs/type-mapping.md's table, condensed to a switch. Anything not
// explicitly listed - dictionary-encoded, extension, run-end-encoded, and
// other types not yet needed by the fixture workers this driver has been
// tested against - falls back to TEXT (its ToString() rendering), which is
// always at least readable even when it isn't semantically ideal.
std::string DeclaredTypeFor(arrow::Type::type id) {
    switch (id) {
        case arrow::Type::BOOL:
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::UINT8:
        case arrow::Type::UINT16:
        case arrow::Type::UINT32:
        case arrow::Type::UINT64:
            return "INTEGER";
        case arrow::Type::HALF_FLOAT:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE:
            return "REAL";
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
        case arrow::Type::DATE32:
        case arrow::Type::DATE64:
        case arrow::Type::TIMESTAMP:
        case arrow::Type::TIME32:
        case arrow::Type::TIME64:
        case arrow::Type::DECIMAL128:
        case arrow::Type::DECIMAL256:
            return "TEXT";
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::FIXED_SIZE_BINARY:
            return "BLOB";
        case arrow::Type::STRUCT:
        case arrow::Type::LIST:
        case arrow::Type::LARGE_LIST:
        case arrow::Type::FIXED_SIZE_LIST:
        case arrow::Type::MAP:
            return "TEXT";  // JSON-encoded, see ScalarToJson.
        default:
            return "TEXT";
    }
}

// std::chrono civil-calendar formatting - days since the epoch to
// YYYY-MM-DD, matching SQLite's own date() function's output shape so its
// date/time functions work directly against these columns.
std::string FormatDate32(int32_t days_since_epoch) {
    using namespace std::chrono;
    sys_days d{days{days_since_epoch}};
    year_month_day ymd{d};
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%04d-%02u-%02u", static_cast<int>(ymd.year()),
                  static_cast<unsigned>(ymd.month()), static_cast<unsigned>(ymd.day()));
    return buf;
}

// unit-aware: value is in `unit`'s ticks since the epoch. tz is carried in
// the type (Arrow keeps timestamps as epoch ticks regardless of tz; the tz
// string is metadata about what zone produced them) - rendered as a
// trailing "Z" when present (UTC-normalized, matching Arrow's convention
// that a tz-aware timestamp's stored value is already UTC), omitted for a
// tz-naive (local/unzoned) timestamp.
std::string FormatTimestamp(int64_t value, arrow::TimeUnit::type unit, bool has_tz) {
    using namespace std::chrono;
    int64_t micros;
    switch (unit) {
        case arrow::TimeUnit::SECOND: micros = value * 1'000'000; break;
        case arrow::TimeUnit::MILLI: micros = value * 1'000; break;
        case arrow::TimeUnit::MICRO: micros = value; break;
        case arrow::TimeUnit::NANO: micros = value / 1'000; break;
        default: micros = value;
    }
    auto total_days = micros / 86'400'000'000LL;
    auto rem_micros = micros % 86'400'000'000LL;
    if (rem_micros < 0) {
        rem_micros += 86'400'000'000LL;
        total_days -= 1;
    }
    sys_days d{days{static_cast<int32_t>(total_days)}};
    year_month_day ymd{d};
    auto us_of_day = rem_micros;
    auto h = us_of_day / 3'600'000'000LL;
    us_of_day %= 3'600'000'000LL;
    auto m = us_of_day / 60'000'000LL;
    us_of_day %= 60'000'000LL;
    auto s = us_of_day / 1'000'000LL;
    auto us = us_of_day % 1'000'000LL;

    char buf[40];
    if (us != 0) {
        std::snprintf(buf, sizeof(buf), "%04d-%02u-%02u %02lld:%02lld:%02lld.%06lld%s",
                      static_cast<int>(ymd.year()), static_cast<unsigned>(ymd.month()),
                      static_cast<unsigned>(ymd.day()), static_cast<long long>(h),
                      static_cast<long long>(m), static_cast<long long>(s), static_cast<long long>(us),
                      has_tz ? "Z" : "");
    } else {
        std::snprintf(buf, sizeof(buf), "%04d-%02u-%02u %02lld:%02lld:%02lld%s",
                      static_cast<int>(ymd.year()), static_cast<unsigned>(ymd.month()),
                      static_cast<unsigned>(ymd.day()), static_cast<long long>(h),
                      static_cast<long long>(m), static_cast<long long>(s), has_tz ? "Z" : "");
    }
    return buf;
}

nlohmann::json ScalarToJson(const arrow::Array& array, int64_t row);

nlohmann::json StructRowToJson(const arrow::StructArray& array, int64_t row) {
    nlohmann::json obj = nlohmann::json::object();
    const auto& type = static_cast<const arrow::StructType&>(*array.type());
    for (int i = 0; i < type.num_fields(); ++i) {
        obj[type.field(i)->name()] = ScalarToJson(*array.field(i), row);
    }
    return obj;
}

template <typename ListArrayType>
nlohmann::json ListRowToJson(const ListArrayType& array, int64_t row) {
    auto arr = nlohmann::json::array();
    const auto& values = *array.values();
    auto begin = array.value_offset(row);
    auto end = array.value_offset(row + 1);
    for (auto i = begin; i < end; ++i) arr.push_back(ScalarToJson(values, i));
    return arr;
}

nlohmann::json MapRowToJson(const arrow::MapArray& array, int64_t row) {
    // JSON objects need string keys; a non-string map key is stringified
    // via ToString() on that element rather than left unrepresentable.
    auto obj = nlohmann::json::object();
    const auto& keys = *array.keys();
    const auto& items = *array.items();
    auto begin = array.value_offset(row);
    auto end = array.value_offset(row + 1);
    for (auto i = begin; i < end; ++i) {
        auto key_scalar = keys.GetScalar(i);
        std::string key = key_scalar.ok() ? (*key_scalar)->ToString() : std::to_string(i);
        obj[key] = ScalarToJson(items, i);
    }
    return obj;
}

nlohmann::json ScalarToJson(const arrow::Array& array, int64_t row) {
    if (array.IsNull(row)) return nullptr;
    switch (array.type_id()) {
        case arrow::Type::BOOL: return static_cast<const arrow::BooleanArray&>(array).Value(row);
        case arrow::Type::INT8: return static_cast<const arrow::Int8Array&>(array).Value(row);
        case arrow::Type::INT16: return static_cast<const arrow::Int16Array&>(array).Value(row);
        case arrow::Type::INT32: return static_cast<const arrow::Int32Array&>(array).Value(row);
        case arrow::Type::INT64: return static_cast<const arrow::Int64Array&>(array).Value(row);
        case arrow::Type::UINT8: return static_cast<const arrow::UInt8Array&>(array).Value(row);
        case arrow::Type::UINT16: return static_cast<const arrow::UInt16Array&>(array).Value(row);
        case arrow::Type::UINT32: return static_cast<const arrow::UInt32Array&>(array).Value(row);
        case arrow::Type::UINT64: return static_cast<const arrow::UInt64Array&>(array).Value(row);
        case arrow::Type::FLOAT: return static_cast<const arrow::FloatArray&>(array).Value(row);
        case arrow::Type::DOUBLE: return static_cast<const arrow::DoubleArray&>(array).Value(row);
        case arrow::Type::STRING: return static_cast<const arrow::StringArray&>(array).GetString(row);
        case arrow::Type::LARGE_STRING:
            return static_cast<const arrow::LargeStringArray&>(array).GetString(row);
        case arrow::Type::STRUCT: return StructRowToJson(static_cast<const arrow::StructArray&>(array), row);
        case arrow::Type::LIST: return ListRowToJson(static_cast<const arrow::ListArray&>(array), row);
        case arrow::Type::LARGE_LIST:
            return ListRowToJson(static_cast<const arrow::LargeListArray&>(array), row);
        case arrow::Type::MAP: return MapRowToJson(static_cast<const arrow::MapArray&>(array), row);
        default: {
            auto scalar = array.GetScalar(row);
            return scalar.ok() ? nlohmann::json((*scalar)->ToString()) : nlohmann::json(nullptr);
        }
    }
}

void SetBlob(sqlite3_context* ctx, const void* data, int64_t size) {
    sqlite3_result_blob64(ctx, data, static_cast<sqlite3_uint64>(size), SQLITE_TRANSIENT);
}

void SetText(sqlite3_context* ctx, const std::string& s) {
    sqlite3_result_text64(ctx, s.data(), static_cast<sqlite3_uint64>(s.size()), SQLITE_TRANSIENT,
                           SQLITE_UTF8);
}

}  // namespace

std::string SqliteDeclaredType(const std::shared_ptr<arrow::DataType>& type) {
    if (!type) return "TEXT";
    return DeclaredTypeFor(type->id());
}

void SetSqliteResultFromArrow(sqlite3_context* ctx, const arrow::Array& array, int64_t row) {
    if (array.IsNull(row)) {
        sqlite3_result_null(ctx);
        return;
    }
    switch (array.type_id()) {
        case arrow::Type::BOOL:
            sqlite3_result_int(ctx, static_cast<const arrow::BooleanArray&>(array).Value(row) ? 1 : 0);
            return;
        case arrow::Type::INT8:
            sqlite3_result_int(ctx, static_cast<const arrow::Int8Array&>(array).Value(row));
            return;
        case arrow::Type::INT16:
            sqlite3_result_int(ctx, static_cast<const arrow::Int16Array&>(array).Value(row));
            return;
        case arrow::Type::INT32:
            sqlite3_result_int(ctx, static_cast<const arrow::Int32Array&>(array).Value(row));
            return;
        case arrow::Type::INT64:
            sqlite3_result_int64(ctx, static_cast<const arrow::Int64Array&>(array).Value(row));
            return;
        case arrow::Type::UINT8:
            sqlite3_result_int(ctx, static_cast<const arrow::UInt8Array&>(array).Value(row));
            return;
        case arrow::Type::UINT16:
            sqlite3_result_int(ctx, static_cast<const arrow::UInt16Array&>(array).Value(row));
            return;
        case arrow::Type::UINT32:
            sqlite3_result_int64(ctx, static_cast<const arrow::UInt32Array&>(array).Value(row));
            return;
        case arrow::Type::UINT64:
            // Overflow risk above 2^63-1: SQLite INTEGER is signed 64-bit,
            // Arrow's uint64 is not - documented in docs/type-mapping.md,
            // not silently handled here.
            sqlite3_result_int64(ctx,
                                  static_cast<sqlite3_int64>(static_cast<const arrow::UInt64Array&>(array).Value(row)));
            return;
        case arrow::Type::HALF_FLOAT:
            sqlite3_result_double(ctx, static_cast<double>(
                                            static_cast<const arrow::HalfFloatArray&>(array).Value(row)));
            return;
        case arrow::Type::FLOAT:
            sqlite3_result_double(ctx, static_cast<const arrow::FloatArray&>(array).Value(row));
            return;
        case arrow::Type::DOUBLE:
            sqlite3_result_double(ctx, static_cast<const arrow::DoubleArray&>(array).Value(row));
            return;
        case arrow::Type::STRING:
            SetText(ctx, static_cast<const arrow::StringArray&>(array).GetString(row));
            return;
        case arrow::Type::LARGE_STRING:
            SetText(ctx, static_cast<const arrow::LargeStringArray&>(array).GetString(row));
            return;
        case arrow::Type::BINARY: {
            auto view = static_cast<const arrow::BinaryArray&>(array).GetView(row);
            SetBlob(ctx, view.data(), static_cast<int64_t>(view.size()));
            return;
        }
        case arrow::Type::LARGE_BINARY: {
            auto view = static_cast<const arrow::LargeBinaryArray&>(array).GetView(row);
            SetBlob(ctx, view.data(), static_cast<int64_t>(view.size()));
            return;
        }
        case arrow::Type::FIXED_SIZE_BINARY: {
            auto view = static_cast<const arrow::FixedSizeBinaryArray&>(array).GetView(row);
            SetBlob(ctx, view.data(), static_cast<int64_t>(view.size()));
            return;
        }
        case arrow::Type::DATE32:
            SetText(ctx, FormatDate32(static_cast<const arrow::Date32Array&>(array).Value(row)));
            return;
        case arrow::Type::DATE64: {
            // ms since epoch -> days (Date64's value is always midnight-
            // aligned per the Arrow spec, so integer division is exact).
            auto ms = static_cast<const arrow::Date64Array&>(array).Value(row);
            SetText(ctx, FormatDate32(static_cast<int32_t>(ms / 86'400'000)));
            return;
        }
        case arrow::Type::TIMESTAMP: {
            const auto& type = static_cast<const arrow::TimestampType&>(*array.type());
            auto value = static_cast<const arrow::TimestampArray&>(array).Value(row);
            SetText(ctx, FormatTimestamp(value, type.unit(), !type.timezone().empty()));
            return;
        }
        case arrow::Type::DECIMAL128: {
            const auto& type = static_cast<const arrow::Decimal128Type&>(*array.type());
            const auto& arr = static_cast<const arrow::Decimal128Array&>(array);
            SetText(ctx, arrow::Decimal128(arr.GetValue(row)).ToString(type.scale()));
            return;
        }
        case arrow::Type::DECIMAL256: {
            const auto& type = static_cast<const arrow::Decimal256Type&>(*array.type());
            const auto& arr = static_cast<const arrow::Decimal256Array&>(array);
            SetText(ctx, arrow::Decimal256(arr.GetValue(row)).ToString(type.scale()));
            return;
        }
        case arrow::Type::STRUCT:
        case arrow::Type::LIST:
        case arrow::Type::LARGE_LIST:
        case arrow::Type::FIXED_SIZE_LIST:
        case arrow::Type::MAP:
            SetText(ctx, ScalarToJson(array, row).dump());
            return;
        default: {
            auto scalar = array.GetScalar(row);
            if (scalar.ok()) {
                SetText(ctx, (*scalar)->ToString());
            } else {
                sqlite3_result_error(ctx, "unsupported Arrow type for SQLite result", -1);
            }
            return;
        }
    }
}

std::shared_ptr<arrow::Scalar> BuildArrowScalarFromSqliteValue(
    sqlite3_value* value, const std::shared_ptr<arrow::DataType>& target_type) {
    if (!target_type) return nullptr;
    if (sqlite3_value_type(value) == SQLITE_NULL) {
        auto result = arrow::MakeNullScalar(target_type);
        return result;
    }
    switch (target_type->id()) {
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
            auto scalar = arrow::MakeScalar(static_cast<int64_t>(sqlite3_value_int64(value)));
            auto cast_result = scalar->CastTo(target_type);
            return cast_result.ok() ? cast_result.ValueUnsafe() : nullptr;
        }
        case arrow::Type::HALF_FLOAT:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE: {
            auto vtype = sqlite3_value_type(value);
            if (vtype != SQLITE_FLOAT && vtype != SQLITE_INTEGER) return nullptr;
            auto scalar = arrow::MakeScalar(sqlite3_value_double(value));
            auto cast_result = scalar->CastTo(target_type);
            return cast_result.ok() ? cast_result.ValueUnsafe() : nullptr;
        }
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING: {
            if (sqlite3_value_type(value) != SQLITE_TEXT) return nullptr;
            const auto* text = reinterpret_cast<const char*>(sqlite3_value_text(value));
            return std::make_shared<arrow::StringScalar>(std::string(text ? text : ""));
        }
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY: {
            if (sqlite3_value_type(value) != SQLITE_BLOB) return nullptr;
            const auto* data = reinterpret_cast<const uint8_t*>(sqlite3_value_blob(value));
            int size = sqlite3_value_bytes(value);
            return std::make_shared<arrow::BinaryScalar>(
                arrow::Buffer::Wrap(data, static_cast<size_t>(size)));
        }
        default:
            return nullptr;  // temporal/decimal/nested: not handled here
    }
}

std::shared_ptr<arrow::Scalar> BuildArrowScalarFromSqliteValueNatural(sqlite3_value* value) {
    switch (sqlite3_value_type(value)) {
        case SQLITE_INTEGER:
            return arrow::MakeScalar(static_cast<int64_t>(sqlite3_value_int64(value)));
        case SQLITE_FLOAT:
            return arrow::MakeScalar(sqlite3_value_double(value));
        case SQLITE_TEXT: {
            const auto* text = reinterpret_cast<const char*>(sqlite3_value_text(value));
            return std::make_shared<arrow::StringScalar>(std::string(text ? text : ""));
        }
        case SQLITE_BLOB: {
            const auto* data = reinterpret_cast<const uint8_t*>(sqlite3_value_blob(value));
            int size = sqlite3_value_bytes(value);
            return std::make_shared<arrow::BinaryScalar>(
                arrow::Buffer::Wrap(data, static_cast<size_t>(size)));
        }
        default:
            return nullptr;  // SQLITE_NULL: no type to infer
    }
}

}  // namespace vgi_sqlite
