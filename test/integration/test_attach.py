# © Copyright 2026 Query Farm LLC - https://query.farm
"""vgi_attach() discovery: creates virtual tables from a real catalog."""

from __future__ import annotations

import sqlite3

import pytest


def test_attach_creates_tables(conn: sqlite3.Connection, worker_location: str) -> None:
    (table_count,) = conn.execute(
        "SELECT vgi_attach(?, 'example')", (worker_location,)
    ).fetchone()

    # The example fixture catalog has ~60 tables; a handful are known not
    # to implement catalog_table_scan_function_get (see the plan file's
    # Milestone 2 status) and are skipped, not counted as a hard failure -
    # assert a generous floor rather than an exact count so this doesn't
    # break every time the fixture worker adds a table.
    assert table_count > 30, f"expected >30 tables created, got {table_count}"

    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'data.%'"
        )
    }
    assert "data.numbers" in tables
    assert "data.ten_thousand_table" in tables


def test_attach_is_idempotent(conn: sqlite3.Connection, worker_location: str) -> None:
    """Re-running vgi_attach() against the same catalog refreshes (DROP +
    CREATE) rather than failing on "table already exists"."""
    conn.execute("SELECT vgi_attach(?, 'example')", (worker_location,))
    (second_count,) = conn.execute(
        "SELECT vgi_attach(?, 'example')", (worker_location,)
    ).fetchone()
    assert second_count > 30


def test_attach_wrong_catalog_name_errors(conn: sqlite3.Connection, worker_location: str) -> None:
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("SELECT vgi_attach(?, 'this_catalog_does_not_exist')", (worker_location,))
