# vgi-sqlite integration tests

Drives the built extension via Python's stdlib `sqlite3` module against a
real VGI worker - `vgi-python`'s `vgi-fixture-worker` by default, the same
fixture `vgi`'s own DuckDB-extension sqllogictest suite uses (for parity).

`vgi`'s 332-file suite is not directly portable here (see the plan file's
Context section) - its DuckDB-dialect SQL (`ATTACH (TYPE vgi, ...)`,
`::CAST`, unsigned/struct/decimal types, `range()`, catalog-qualified
function calls, `vgi_function_arguments()`) has no SQLite equivalent. This
suite ports *coverage categories*, not files, tracked informally here
rather than via a strict file-parity check.

## Running

```bash
cmake --build ../../build          # build the extension first
uv sync                            # once
uv run pytest -v
```

Override the worker with `VGI_TEST_WORKER` (matching `vgi/Makefile`'s
convention) or the extension path with `VGI_SQLITE_EXTENSION`.

## Coverage

| Category | Status | File |
|---|---|---|
| `vgi_attach()` discovery, auto-created virtual tables | ported | `test_attach.py` |
| Full-scan correctness (`SELECT`, `LIMIT`, `WHERE`, `count(*)`) | ported | `test_scan.py` |
| Connection pooling (repeated/concurrent-table scans on one worker) | ported | `test_scan.py` |
| Projection pushdown (correct columns, incl. functions that ignore it) | ported | `test_pushdown.py` |
| Type mapping (per Arrow type -> SQLite storage class) | not yet ported | — |
| Filter (WHERE-constraint) pushdown | not yet implemented (`xBestIndex` doesn't push constraints yet) | — |
| Protocol version mismatch | not yet ported | — |
| Scalar functions | not applicable yet (Milestone 3) | — |
| Writable tables, transactions, aggregates | not applicable yet (Milestone 4) | — |
| Bearer/OAuth auth | not applicable yet (Milestone 3/4) | — |

Known worker-side gap this suite works around, not fixes: several of
`vgi-fixture-worker`'s own fixture tables (`geo_points`, the
`multi_branch_*` tables) don't implement `catalog_table_scan_function_get`
for non-declarative tables - `vgi_attach()` skips them (logs a warning)
rather than failing the whole attach, and `test_attach_creates_tables`
asserts a floor (`> 30`) rather than an exact table count for this reason.
