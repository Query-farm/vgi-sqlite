// © Copyright 2026 Query Farm LLC - https://query.farm
//
// WHERE-constraint pushdown: translates SQLite's xBestIndex constraints
// into VGI's "hybrid JSON + Arrow" InitRequest.pushdown_filters wire
// format (vgi-python's docs/filter-pushdown.md) - a JSON array describing
// filter structure/operators plus an Arrow RecordBatch carrying the typed
// constant values.
//
// Conservative by design: every pushed-down constraint is still left for
// SQLite to re-check itself (aConstraintUsage[i].omit is never set) - a
// worker that doesn't apply a filter it declared support for, or a bug in
// this encoder, degrades to "did the pushdown work for nothing" rather
// than "wrong query results". Only comparison operators (=, !=, <, <=, >,
// >=) and IS [NOT] NULL are pushed; LIKE/GLOB/REGEXP/MATCH/IS/ISNOT and
// anything on a column whose Arrow type isn't a simple scalar (temporal,
// decimal, nested) are left for SQLite to filter unassisted.
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
// column type this encoder doesn't handle). `columns` is the table's
// declared Arrow schema, used to type each value column exactly like its
// target column (docs/filter-pushdown.md: "Arrow columns store filter
// values (preserves exact types)"). Call from xFilter.
//
// `projected_columns` is xFilter's own decoded projection list (empty
// means "every column, in declared order" - the same convention
// cursor->projected_columns/xColumn's FetchedPosition already use).
// Required to get `column_index` right: the wire format's
// pushdown_filters.column_index isn't the table's declared column index
// at all - it's that column's POSITION WITHIN THE PROJECTED OUTPUT the
// worker is about to emit (confirmed against vgi's own DuckDB extension,
// vgi_table_function_impl.cpp: "`col_idx` is the filter's position in the
// projected column_ids and is sent as the wire `column_index`. The
// worker applies ConstantFilter/InFilter by INDEX (`batch.column
// (column_index)`)... this only resolves correctly because the worker
// emits its output batch in the same projected order"). Found the hard
// way against a real deployed worker (earthquakes, a Cloudflare Worker):
// `SELECT mag, time FROM t WHERE mag >= 0` silently returned zero rows -
// column_index was sent as 2 (mag's DECLARED index in a ~30-column
// table) when the worker's own 2-column projected response only has
// positions 0/1, so the filter compared the wrong column (or an
// out-of-range one) against every row and never matched. `SELECT *`
// (no narrowing - projected order coincides with declared order)
// accidentally "worked" the whole time, which is why this went
// unnoticed until a genuinely narrow projection exercised it.
std::optional<std::string> EncodePushdownFilters(const std::shared_ptr<arrow::Schema>& columns,
                                                  const std::vector<PushableConstraint>& constraints,
                                                  sqlite3_value** argv,
                                                  const std::vector<int>& projected_columns);

}  // namespace vgi_sqlite
