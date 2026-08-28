<p align="center">
  <img src="https://raw.githubusercontent.com/Query-farm/vgi-sqlite/main/docs/vgi-logo.png" alt="Vector Gateway Interface logo" width="320">
</p>

<h1 align="center">VGI for SQLite</h1>

<p align="center">
  Attach a VGI worker to a SQLite connection and query its tables and functions like they were native.<br>
  Built by <a href="https://query.farm">🚜 Query.Farm</a>
</p>

<p align="center">
  <a href="https://github.com/Query-farm/vgi-sqlite/actions/workflows/ci.yml"><img src="https://github.com/Query-farm/vgi-sqlite/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/Query-farm/vgi-sqlite/releases"><img src="https://img.shields.io/github/v/release/Query-farm/vgi-sqlite" alt="Latest release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Query%20Farm%20Source--Available-blue" alt="License"></a>
</p>

---

A **SQLite driver for VGI** (Vector Gateway Interface) — VGI is [Query
Farm](https://query.farm)'s protocol for exposing external data sources as
query-engine catalogs over Apache Arrow IPC. This repo is the *client* side
for SQLite, playing the same role [`vgi`](https://github.com/Query-farm/vgi)
(the DuckDB extension) plays for DuckDB — new, from-scratch client code, not
a fork of it. SQLite has no Arrow bridge, no native `ATTACH` extension point,
and a much simpler constraint-pushdown model than DuckDB, so nothing here is
a straight port.

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
- Subprocess, `unix://`, launcher-discovery (`launch:` — AF_UNIX on POSIX,
  named pipes on Windows), and HTTP (bearer-auth) transports.

- Sibling reference port (Python): [`vgi-python`](https://github.com/Query-farm/vgi-python)
- DuckDB extension: [`vgi`](https://github.com/Query-farm/vgi)

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
.load ./build/vgi          -- vgi.so/.dylib on macOS/Linux, build\Debug\vgi.dll on Windows

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

## Feature coverage

| Area | Status |
|---|---|
| Read scans | ✅ projection, filter (`=` `!=` `<` `<=` `>` `>=` `IS [NOT] NULL`), and `LIMIT` pushdown; cost-based query planning from worker-reported cardinality |
| Writes | ⚠️ `INSERT`/`UPDATE`/`DELETE` implemented and tested against a synthetic fixture worker, but not yet validated against any real-world VGI worker (unlike reads, which have been) — and writable tables are uncommon in the VGI ecosystem generally, since most workers expose read-only external data sources |
| Transactions | ✅ one flat VGI transaction shared across every table from a catalog touched in one SQL transaction; no nested `SAVEPOINT` — VGI's protocol has no nested-transaction concept to map it onto |
| Scalar functions | ✅ registered natively as `<catalog>_<name>` |
| Aggregate functions | ✅ including `GROUP BY`; windowed (`OVER`) aggregates are not supported — a structurally different RPC family with no incremental step model |
| Correlated table functions | ✅ `vgi_table_in_out` (row-transform functions) and `vgi_table_function` (plain generator functions), called via `FROM t, fn(t.x)` — no `LATERAL` keyword needed |
| VGI splits | ✅ sequential single-reader redemption — SQLite has no parallel-scan-worker concept, so splits are claimed one at a time rather than concurrently |
| Transports | ✅ subprocess, `unix://`, `launch:` discovery (AF_UNIX on POSIX, named pipes on Windows), `http(s)://` with bearer auth |
| Classic `table_in_out` (relation-valued argument) | ❌ SQLite's table-valued-function calling convention has no equivalent of a whole-relation argument |
| Multi-branch (union-of-sources) tables | ❌ no natural multi-source union at the SQLite virtual-table level |
| Function overloading (same name, different arity) | ✅ each overload gets its own generated SQL name (`<catalog>_<function>_<N>`, `N` = argument count) instead of one silently winning |
| Windows | ✅ full parity with POSIX, including `launch:` discovery (named pipes) — verified in CI on `windows-2022` |

See [`CLAUDE.md`](CLAUDE.md) for the full design, every non-obvious protocol
contract, and the real bugs found building each of these against live
workers.

## License

Query Farm Source-Available License, Version 1.0 — see [LICENSE](LICENSE).
The same license as [`vgi`](https://github.com/Query-farm/vgi),
[`vgi-cpp`](https://github.com/Query-farm/vgi-cpp), and
[`vgi-python`](https://github.com/Query-farm/vgi-python).
