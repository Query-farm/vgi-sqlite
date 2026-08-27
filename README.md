# vgi-sqlite

A **SQLite driver for VGI** (Vector Gateway Interface) — attach a VGI worker
to a SQLite connection and query its tables and functions like they were
native. VGI is [Query Farm](https://query.farm)'s protocol for exposing
external data sources as query-engine catalogs over Apache Arrow IPC; this
repo is the *client* side for SQLite, playing the same role
[`vgi`](https://github.com/Query-farm/vgi) (the DuckDB extension) plays for
DuckDB — new, from-scratch client code, not a fork of it. SQLite has no Arrow
bridge, no native `ATTACH` extension point, and a much simpler
constraint-pushdown model than DuckDB, so nothing here is a straight port.

It builds a single loadable extension (`vgi.{so,dylib}`) exposing:

- **`vgi_attach(location, catalog)`** — a SQL function that discovers every
  table and function in a worker's catalog and auto-creates a
  `CREATE VIRTUAL TABLE` for each one.
- Read/write virtual tables (`vgi_worker`) with projection, filter, and
  LIMIT pushdown, transactions, and cost-based query planning.
- Correlated table-valued functions (`vgi_table_in_out`, `vgi_table_function`)
  — SQLite's `FROM t, fn(t.x)` syntax needs no `LATERAL` keyword for this,
  the same way `json_each(t.x)` doesn't.
- Scalar and aggregate functions, registered natively as
  `<catalog>_<name>`.
- Subprocess, `unix://`, AF_UNIX launcher-discovery, and HTTP (bearer-auth)
  transports.

## Build

```bash
git clone https://github.com/Query-farm/vgi-cpp.git ../vgi-cpp
git clone https://github.com/Query-farm/vgi-rpc-cpp.git ../vgi-rpc-c++
export VCPKG_ROOT=/path/to/vcpkg

scripts/fetch_sqlite.sh          # once: pins/builds SQLite from source
cmake --preset default
cmake --build build
```

`vgi-cpp` (and, transitively, `vgi-rpc-cpp`) is built from a sibling checkout
by default, since the two move together and an installed copy goes stale
silently — override with `-DVGI_CPP_SOURCE_DIR=<path>`.

Not `find_package(SQLite3)`: the driver needs a recent SQLite built with
`SQLITE_ENABLE_LOAD_EXTENSION`/`SQLITE_ENABLE_COLUMN_METADATA`, which the
system/package-manager SQLite usually isn't — `scripts/fetch_sqlite.sh`
pins/downloads/verifies/builds it from the amalgamation.

Note for Linux and macOS: vcpkg builds `libsodium` through its autotools
script and `thrift` through flex/bison, so `autoconf automake libtool
autoconf-archive flex bison` need to be installed first.

## Usage

```sql
.load ./build/vgi

SELECT vgi_attach('uv run --project ~/vgi-python vgi-fixture-worker', 'example');

SELECT * FROM example_some_table WHERE some_column > 10 LIMIT 5;
SELECT example_some_scalar_function(some_column) FROM example_some_table;
```

`location` can be a subprocess argv, `unix:///path/to.sock`, an AF_UNIX
`launch:<argv...>` discovery location, or `http(s)://host:port` — see
[`CLAUDE.md`](CLAUDE.md) for the full transport/pushdown/type-mapping design
and every gotcha found building it.

## Testing

```bash
cd test/integration
uv sync
uv run pytest -v
```

`test/integration/` (pytest, driving the built extension via Python's stdlib
`sqlite3` module against a real `vgi-fixture-worker`) is the test source of
record — see [`test/integration/README.md`](test/integration/README.md) for
the coverage table. `test/sqllogictest/` is a separate, complementary tool
that runs `vgi`'s DuckDB-dialect sqllogictest corpus against this driver to
track continuously growing coverage of what's mechanically translatable —
see [`test/sqllogictest/README.md`](test/sqllogictest/README.md).

## Status

All 5 originally-planned milestones are done (read/write tables,
projection/filter/LIMIT pushdown, scalar and aggregate functions, per-row
correlated table functions, transactions, subprocess/unix/launcher/HTTP
transports, a security review), plus real-world validation against
third-party VGI workers and 5 further milestones of bug fixes and new
capabilities on top. See [`CLAUDE.md`](CLAUDE.md) for the full history.

## License

Query Farm Source-Available License, Version 1.0 — see [LICENSE](LICENSE).
The same license as [`vgi`](https://github.com/Query-farm/vgi),
[`vgi-cpp`](https://github.com/Query-farm/vgi-cpp), and
[`vgi-python`](https://github.com/Query-farm/vgi-python).
