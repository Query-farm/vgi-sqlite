# © Copyright 2026 Query Farm LLC - https://query.farm
"""Real data scans through the vgi_worker virtual table."""

from __future__ import annotations

import sqlite3


def _attach(conn: sqlite3.Connection, worker_location: str) -> None:
    conn.execute("SELECT vgi_attach(?, 'example')", (worker_location,))


def test_count_matches_known_fixture_size(conn: sqlite3.Connection, worker_location: str) -> None:
    _attach(conn, worker_location)
    (count,) = conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    assert count == 100


def test_select_limit_returns_correct_rows(conn: sqlite3.Connection, worker_location: str) -> None:
    _attach(conn, worker_location)
    rows = conn.execute('SELECT value FROM "data.numbers" ORDER BY value LIMIT 5').fetchall()
    assert [r[0] for r in rows] == [0, 1, 2, 3, 4]


def test_where_clause_filters_correctly(conn: sqlite3.Connection, worker_location: str) -> None:
    """No pushdown yet (Milestone 3) - SQLite fetches the full scan and
    filters itself, but the result must still be correct."""
    _attach(conn, worker_location)
    (count,) = conn.execute('SELECT count(*) FROM "data.numbers" WHERE value > 50').fetchone()
    assert count == 49


def test_typeof_matches_declared_type(conn: sqlite3.Connection, worker_location: str) -> None:
    _attach(conn, worker_location)
    (t,) = conn.execute('SELECT typeof(value) FROM "data.numbers" LIMIT 1').fetchone()
    assert t == "integer"


def test_repeated_scans_on_same_table_both_succeed(conn: sqlite3.Connection, worker_location: str) -> None:
    """Regression test for the "RPC client is closed" bug (see the plan
    file's Milestone 2 status): a pooled connection's stream must be
    explicitly closed after each scan, or the second scan on the same
    table breaks the whole shared connection."""
    _attach(conn, worker_location)
    first = conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    second = conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    assert first == second == (100,)


def test_two_different_tables_share_one_connection(conn: sqlite3.Connection, worker_location: str) -> None:
    """Regression test for the connection-pooling fix: two different
    tables from the same catalog must both be queryable without either
    breaking the other's shared connection."""
    _attach(conn, worker_location)
    (numbers,) = conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    (ten_k,) = conn.execute('SELECT count(*) FROM "data.ten_thousand_table"').fetchone()
    assert numbers == 100
    assert ten_k == 10_000
