# © Copyright 2026 Query Farm LLC - https://query.farm
"""Plain, standalone table (generator) functions - e.g. split_sequence -
callable directly via SQL table-valued-function syntax
(`FROM catalog_fn(args)`, literal or per-outer-row correlated), no per-row
streaming input at all (unlike table_in_out - see
src/vtab/vgi_table_function_vtab.h's file comment for the exact
distinction and why this needed its own vtab module).

Against the `example` catalog's own real fixtures (split_sequence,
geo_encode) - no new fixture worker needed; these were already registered
in vgi-fixture-worker the whole time (see the plan file's Milestone 10
notes on how that was missed during Milestone 9's own research pass)."""

from __future__ import annotations

import sqlite3


def _attach(conn: sqlite3.Connection, worker_location: str) -> None:
    conn.execute("SELECT vgi_attach(?, 'example')", (worker_location,))


def test_attach_registers_split_sequence(conn: sqlite3.Connection, worker_location: str) -> None:
    _attach(conn, worker_location)
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()}
    assert "example_split_sequence" in names


def test_literal_call(conn: sqlite3.Connection, worker_location: str) -> None:
    """The non-correlated call shape - `SELECT * FROM fn(literal, literal)`
    - exercises the throwaway xConnect-time schema probe (placeholder
    argument values) plus a real, fresh per-xFilter bind using the
    literal call-site values."""
    _attach(conn, worker_location)
    rows = conn.execute("SELECT n FROM example_split_sequence(10, 1) ORDER BY n").fetchall()
    assert rows == [(i,) for i in range(10)]


def test_literal_call_drains_multiple_splits(conn: sqlite3.Connection, worker_location: str) -> None:
    """split_sequence's own splits support (ScanFunction.supports_splits)
    is honored transparently by TableScanner - this asks for more than
    one split and checks every row still arrives exactly once, in the
    same total, regardless of how many splits the worker planned."""
    _attach(conn, worker_location)
    rows = conn.execute("SELECT n FROM example_split_sequence(100, 4) ORDER BY n").fetchall()
    assert rows == [(i,) for i in range(100)]


def test_correlated_call_one_scan_per_outer_row(conn: sqlite3.Connection, worker_location: str) -> None:
    """The actual LATERAL-style use case: `FROM t, fn(t.n, ...)` - SQLite
    reuses the SAME cursor across every outer row (one xFilter call per
    row), so this is also the regression test for a real bug found
    building this feature: AdvanceBatch only ever SETS eof true (a
    genuine end-of-stream), never clears it - without an explicit reset
    at the start of every xFilter call, a PRIOR outer row's scan
    completing (eof left true) stayed stuck true forever, silently
    returning zero rows for every outer row after the first."""
    _attach(conn, worker_location)
    conn.execute("CREATE TABLE nums(n INTEGER)")
    conn.executemany("INSERT INTO nums VALUES (?)", [(5,), (10,), (20,)])
    rows = conn.execute(
        "SELECT nums.n, COUNT(*) FROM nums, example_split_sequence(nums.n, 2) GROUP BY nums.n ORDER BY nums.n"
    ).fetchall()
    assert rows == [(5, 5), (10, 10), (20, 20)]


def test_missing_argument_rejected_at_prepare_time(conn: sqlite3.Connection, worker_location: str) -> None:
    """Same xBestIndex contract as vgi_table_in_out's own module (every
    HIDDEN argument column requires an EQ constraint, or the plan is
    rejected outright) - a missing argument fails cleanly at prepare
    time, not at runtime and not via a silent wrong-arity call."""
    _attach(conn, worker_location)
    try:
        conn.execute("SELECT * FROM example_split_sequence(10)").fetchall()
    except sqlite3.OperationalError as e:
        assert "no query solution" in str(e)
    else:
        raise AssertionError("expected a missing-argument query to fail to prepare")


def test_geo_encode_blended_function_still_works(conn: sqlite3.Connection, worker_location: str) -> None:
    """Not a plain table function (geo_encode is table_in_out/blended,
    vgi_table_in_out's own module) - included here as a quick cross-check
    that registering both modules' functions side by side in one
    vgi_attach() call doesn't interfere with either."""
    _attach(conn, worker_location)
    assert conn.execute("SELECT geohash FROM example_geo_encode(52.0, 13.0, 4)").fetchall() == [("52.0:13.0",)]
