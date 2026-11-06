// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <string>

#include "wire/nanoarrow_types.h"

struct sqlite3_context;
struct sqlite3_value;

namespace vgi_sqlite {

std::string SqliteDeclaredType(const ArrowSchema& type);
void SetSqliteResultFromArrow(sqlite3_context* ctx, const nano::Column& column, int64_t row);
nano::ScalarPtr BuildArrowScalarFromSqliteValue(sqlite3_value* value,
                                                 const ArrowSchema& target_type);
nano::ScalarPtr BuildArrowScalarFromSqliteValueNatural(sqlite3_value* value);

}  // namespace vgi_sqlite
