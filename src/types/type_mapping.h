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
#include <arrow/type.h>

struct sqlite3_context;

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

}  // namespace vgi_sqlite
