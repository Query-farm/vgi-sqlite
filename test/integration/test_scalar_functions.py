# © Copyright 2026 Query Farm LLC - https://query.farm
"""Scalar functions: vgi_attach() registers each worker scalar function as
a native SQLite function under "<catalog>_<function>" (see extension.cpp's
VgiAttachFunc file comment on why - SQLite has no catalog-qualified
function namespace the way DuckDB does)."""

from __future__ import annotations

import sqlite3


def test_scalar_function_basic_call(conn: sqlite3.Connection, worker_location: str) -> None:
    conn.execute("SELECT vgi_attach(?, 'example')", (worker_location,))
    (result,) = conn.execute("SELECT example_add_values(3, 4)").fetchone()
    assert result == 7


def test_scalar_function_repeated_calls(conn: sqlite3.Connection, worker_location: str) -> None:
    """Calling the same registered function many times in one session -
    each call acquires and releases its own connection (see
    ScalarFunctionCaller's file comment) - must keep working, not just
    the first call."""
    conn.execute("SELECT vgi_attach(?, 'example')", (worker_location,))
    for a, b in [(1, 2), (10, 20), (-5, 5), (0, 0)]:
        (result,) = conn.execute("SELECT example_add_values(?, ?)", (a, b)).fetchone()
        assert result == a + b


def test_scalar_function_during_table_scan(conn: sqlite3.Connection, worker_location: str) -> None:
    """Regression test: a scalar function evaluated once per row while a
    table scan's producer stream is still open (interleaved, on the same
    (location, catalog) pair) used to fail with "raw RPC client already
    has an active call" - first because ScalarFunctionCaller held its own
    exchange stream open persistently across calls on the shared pooled
    connection, then (after fixing that) because open-and-close-per-call
    still shared that same connection with the table scan's own
    long-lived producer stream. The real fix was ConnectionPool handing
    out per-use checkouts instead of one shared connection per
    (location, catalog) - see connection_pool.h's file comment. This test
    is the exact scenario that surfaced the bug: `SELECT
    example_add_values(value, 100) FROM "data.numbers"`."""
    conn.execute("SELECT vgi_attach(?, 'example')", (worker_location,))
    rows = conn.execute(
        'SELECT value, example_add_values(value, 100) FROM "data.numbers" ORDER BY value LIMIT 5'
    ).fetchall()
    assert rows == [(i, i + 100) for i in range(5)]


def test_scalar_function_null_argument_errors(conn: sqlite3.Connection, worker_location: str) -> None:
    """A NULL argument's Arrow type can't be inferred from an absent value
    (see BuildArrowScalarFromSqliteValueNatural / the bridge's explicit
    check in extension.cpp) - a documented gap, surfaced as a clear SQLite
    error rather than a crash or a silently wrong call."""
    conn.execute("SELECT vgi_attach(?, 'example')", (worker_location,))
    try:
        conn.execute("SELECT example_add_values(NULL, 4)").fetchone()
        assert False, "expected a NULL argument to raise"
    except sqlite3.OperationalError:
        pass
