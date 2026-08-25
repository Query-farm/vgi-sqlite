# Arrow ↔ SQLite type mapping

VGI's wire protocol is Arrow-typed (every column has a real Arrow
`DataType`); SQLite has five storage classes (`NULL`, `INTEGER`, `REAL`,
`TEXT`, `BLOB`) plus type *affinity* on declared columns, not real column
types. There is no bridge library to lean on for this direction the way
`vgi` (the DuckDB extension) leans on DuckDB's own Arrow C-ABI import -
this mapping is implemented from scratch in `src/types/type_mapping.{h,cpp}`.

Two separate concerns, both driven by the same table below:

- **Declared type** (`SqliteDeclaredType`): the column type named in the
  `CREATE TABLE`/`CREATE VIRTUAL TABLE` DDL `xConnect`/`xCreate` emit via
  `sqlite3_declare_vtab`. This only sets SQLite's *affinity* for that
  column - it doesn't constrain what storage class an actual value can
  carry at read time.
- **Result conversion** (`SetSqliteResultFromArrow`): the actual
  `sqlite3_result_*` call made for each returned row's value, driven by
  the *array's own* Arrow type at read time, independent of what the
  declared column type says.

## The table

| Arrow type | Declared SQLite type | Result value | Notes |
|---|---|---|---|
| `bool` | `INTEGER` | `sqlite3_result_int` (0/1) | |
| `int8`/`int16`/`int32` | `INTEGER` | `sqlite3_result_int` | |
| `int64` | `INTEGER` | `sqlite3_result_int64` | |
| `uint8`/`uint16` | `INTEGER` | `sqlite3_result_int` | |
| `uint32` | `INTEGER` | `sqlite3_result_int64` | widened to avoid `int32` overflow |
| `uint64` | `INTEGER` | `sqlite3_result_int64` | **overflow risk**: SQLite `INTEGER` is signed 64-bit; a `uint64` value above `2^63-1` reinterprets as negative. Not detected or rejected - documented here, not silently "handled". |
| `half_float`/`float`/`double` | `REAL` | `sqlite3_result_double` | |
| `utf8`/`large_utf8` | `TEXT` | `sqlite3_result_text64` | |
| `binary`/`large_binary`/`fixed_size_binary` | `BLOB` | `sqlite3_result_blob64` | |
| `date32` | `TEXT` | ISO-8601 `YYYY-MM-DD` | matches SQLite's own `date()` output shape, so its date/time functions work directly against these columns |
| `date64` | `TEXT` | ISO-8601 `YYYY-MM-DD` | ms-since-epoch, always midnight-aligned per the Arrow spec, so the day conversion is exact |
| `timestamp(unit, tz)` | `TEXT` | ISO-8601 `YYYY-MM-DD HH:MM:SS[.ffffff][Z]` | fractional seconds only printed when non-zero; trailing `Z` only when the type carries a timezone (Arrow stores a tz-aware timestamp's value already UTC-normalized - the tz string is metadata about what produced it, not an offset to apply) |
| `decimal128`/`decimal256` | `TEXT` | exact decimal string (`Decimal128`/`Decimal256::ToString(scale)`) | exact, not `REAL` - avoids silent precision loss, at the cost of the column no longer sorting/comparing numerically in SQL without an explicit `CAST` |
| `struct`/`list`/`large_list`/`fixed_size_list`/`map` | `TEXT` | JSON-encoded (via nlohmann::json) | recurses through nested struct/list/map; a map's non-string keys are stringified via `ToString()`; SQLite's built-in JSON1 functions work directly against these columns |
| anything else (dictionary-encoded, extension, run-end-encoded, ...) | `TEXT` | `ToString()` on the scalar | not yet needed by any fixture table this driver has been tested against; always at least readable, not necessarily semantically ideal |

`NULL` (an array's own null bitmap, independent of type) always maps to
`sqlite3_result_null` / the `NULL` storage class, regardless of the
underlying Arrow type.

## The reverse direction: SQLite value → Arrow scalar

Needed when *calling into* VGI - a scalar function argument, or a pushed
WHERE-constraint's comparison value (`vtab/filter_pushdown.cpp`). Two
converters, for two different situations:

- **`BuildArrowScalarFromSqliteValue(value, target_type)`**: converts
  toward a *known* target Arrow type (used by filter pushdown, where the
  target column's declared type is known from the table's schema).
  Rejects a storage-class mismatch it can't safely coerce (e.g. `TEXT`
  toward an `INTEGER`-typed column) by returning `nullptr` rather than
  guessing - the caller treats that as "don't push this one" rather than
  sending the worker a wrong-typed value.
- **`BuildArrowScalarFromSqliteValueNatural(value)`**: infers the Arrow
  type from the SQLite value's *own* storage class instead - `int64` for
  `SQLITE_INTEGER`, `double` for `SQLITE_FLOAT`, `utf8` for `SQLITE_TEXT`,
  `binary` for `SQLITE_BLOB`. Used for scalar function arguments, where
  there is no reliable declared target type to convert against at all
  (see `ScalarFunctionCaller`'s file comment on why `FunctionInfo.arguments`
  can't be trusted for this). Returns `nullptr` for `SQLITE_NULL`, since no
  type can be inferred from an absent value alone - a scalar function
  called with a `NULL` argument is rejected with a clear error rather than
  guessed at (see `extension.cpp`'s `ScalarFunctionBridge`).

Both converters are scoped to the same primitive types the outbound table
covers (bool/int8..64/uint8..32/float/double/utf8/binary) - temporal,
decimal, and nested (struct/list/map) *target* types aren't handled on
this inbound direction, since SQLite itself has no native storage class to
carry them as an argument value in the first place (a caller would have
already stringified/JSON-encoded them to pass them as `TEXT`).

## What's deliberately out of scope here

- **Numeric overflow beyond `uint64` → signed `INTEGER`**: not otherwise
  checked; a Decimal literal or timestamp value outside SQLite's usual
  range just round-trips through its string representation.
- **Collation-aware comparison of the `TEXT`-encoded temporal/decimal
  types**: they sort lexicographically as text, which happens to match
  numeric/chronological order for the fixed-width formats emitted here
  (zero-padded ISO-8601 dates/timestamps), but is not something SQLite
  itself knows about the column's real semantics.
