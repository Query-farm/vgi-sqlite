// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Arrow <-> SQLite type/value mapping. SQLite has storage classes, not
// Arrow's rich type system (see docs/type-mapping.md for the full table
// and rationale) - this is new, from-scratch work: vgi's DuckDB extension
// leans entirely on DuckDB's own Arrow C-ABI bridge, which has no SQLite
// analog.
#pragma once

#include <memory>
#include <string>

#include <arrow/array.h>
#include <arrow/scalar.h>
#include <arrow/type.h>

struct sqlite3_context;
struct sqlite3_value;

namespace vgi_sqlite {

// The declared column type for a CREATE TABLE/CREATE VIRTUAL TABLE
// statement (sqlite3_declare_vtab), per docs/type-mapping.md's table.
// Declared type drives SQLite's type-affinity rules for that column, even
// though every value still carries its own storage class at read time.
std::string SqliteDeclaredType(const std::shared_ptr<arrow::DataType>& type);

// Set `ctx`'s result to the value at `row` of `array` (via
// sqlite3_result_*), converting per SqliteDeclaredType's mapping. Handles
// the array's own null bitmap.
void SetSqliteResultFromArrow(sqlite3_context* ctx, const arrow::Array& array, int64_t row);

// The reverse direction, for calling a VGI scalar function with a SQLite
// argument value: builds an Arrow scalar matching `target_type` from
// `value`, or nullptr if this converter doesn't handle that combination
// (temporal/decimal/nested target types, or a value SQLite can't coerce -
// e.g. TEXT into an INTEGER-typed argument). Scoped to the same types
// SqliteDeclaredType/SetSqliteResultFromArrow handle on the way out:
// bool/int8..64/uint8..32/float/double/utf8/binary.
std::shared_ptr<arrow::Scalar> BuildArrowScalarFromSqliteValue(
    sqlite3_value* value, const std::shared_ptr<arrow::DataType>& target_type);

// Builds an Arrow scalar from `value` using its own SQLite storage class
// to pick the Arrow type - int64 for SQLITE_INTEGER, double for
// SQLITE_FLOAT, utf8 for SQLITE_TEXT, binary for SQLITE_BLOB - rather than
// converting to a type decided elsewhere. For a scalar function whose
// argument types aren't reliably known ahead of the call (see
// ScalarFunctionCaller's file comment on why catalog-declared argument
// types can't be trusted). Returns nullptr for SQLITE_NULL, since no type
// can be inferred from an absent value alone.
std::shared_ptr<arrow::Scalar> BuildArrowScalarFromSqliteValueNatural(sqlite3_value* value);

}  // namespace vgi_sqlite
