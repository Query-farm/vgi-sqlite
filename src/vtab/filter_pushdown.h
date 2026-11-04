// © Copyright 2026 Query Farm LLC - https://query.farm
//
// WHERE-constraint pushdown: translates SQLite's xBestIndex constraints
// into VGI Filter Encoding v2 (vgi-python's
// docs/protocol/vgi-filter-encoding-v2-spec.md) for
// InitRequest.pushdown_filters - a JSON snapshot document describing filter
// structure plus an Arrow RecordBatch carrying the typed literal values.
//
// Every predicate is "advisory": SQLite re-checks each pushed constraint
// itself (aConstraintUsage[i].omit is never set), which is exactly the
// spec's condition for that mode, so a worker that ignores a predicate, or a
// bug in this encoder, degrades to "did the pushdown work for nothing"
// rather than "wrong query results". Each constraint is its own predicate,
// which section 8.3 allows because none of these comparisons can throw. Only
// comparison operators (=, !=, <, <=, >, >=) and IS [NOT] NULL are pushed;
// LIKE/GLOB/REGEXP/MATCH/IS/ISNOT and anything on a column whose Arrow type
// isn't a simple scalar (temporal, decimal, nested) are left for SQLite to
// filter unassisted.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/type.h>

struct sqlite3_index_info;
struct sqlite3_value;

namespace vgi_sqlite {

// One constraint xBestIndex chose to push down, in the order its value
// will arrive via xFilter's argv[] (SQLite assigns argvIndex 1, 2, 3...
// in whatever order xBestIndex requested them, and hands them back in
// that same order - not reordered by original aConstraint position).
struct PushableConstraint {
    int column_index;
    unsigned char op;  // SQLITE_INDEX_CONSTRAINT_EQ/GT/LE/LT/GE/NE/ISNULL/ISNOTNULL
};

// Scans info->aConstraint for usable, supported constraints, claims each
// via aConstraintUsage[i].argvIndex (sequential, 1-based - never sets
// .omit), and returns them in argvIndex order. Call from xBestIndex.
std::vector<PushableConstraint> SelectPushableConstraints(sqlite3_index_info* info);

// Builds and IPC-encodes the pushdown_filters payload from the
// constraints SelectPushableConstraints chose plus their now-known values
// (argv, same order/length as `constraints`) - nullopt if none of them
// could actually be encoded (e.g. every candidate turned out to need a
// column type this encoder doesn't handle). `columns` is the unprojected
// schema the scan binds to: a v2 column_ref's column_index indexes that
// schema, not the projected output batch. Call from xFilter.
std::optional<std::string> EncodePushdownFilters(const std::shared_ptr<arrow::Schema>& columns,
                                                  const std::vector<PushableConstraint>& constraints,
                                                  sqlite3_value** argv);

}  // namespace vgi_sqlite
