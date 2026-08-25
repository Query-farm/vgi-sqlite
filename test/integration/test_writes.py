# © Copyright 2026 Query Farm LLC - https://query.farm
"""Writable tables (xUpdate): INSERT only so far - see vgi_vtab.cpp's
xUpdate file comment on why UPDATE/DELETE aren't wired up yet (no rowid-
identification design in this vtab yet). Tested against vgi-fixture-worker's
`simple_writable` catalog (see table_writer.h/conftest.py's
writable_worker_location for why that fixture, not `writable`)."""

from __future__ import annotations

import sqlite3

import pytest


def _attach(conn: sqlite3.Connection, writable_worker_location: str) -> None:
    conn.execute("SELECT vgi_attach(?, 'simple_writable')", (writable_worker_location,))


def test_insert_is_visible_to_a_later_select(conn: sqlite3.Connection, writable_worker_location: str) -> None:
    """Regression test for the bug that motivated ConnectionPool's
    attach_opaque_data sharing (see connection_pool.h's file comment):
    an INSERT and a later SELECT used to land on two independently
    spawned physical connections, each with its own freshly-minted
    attach_opaque_data - invisible to each other on a worker (like this
    fixture) whose backing storage is keyed by those very bytes. Failure
    mode here was silent: no error, just an empty table afterward."""
    _attach(conn, writable_worker_location)
    conn.execute('INSERT INTO "main.items_insert_only" (id, name, rowid) VALUES (?, ?, NULL)', (1, "alpha"))
    rows = conn.execute('SELECT id, name FROM "main.items_insert_only"').fetchall()
    assert rows == [(1, "alpha")]


def test_multiple_inserts_all_visible(conn: sqlite3.Connection, writable_worker_location: str) -> None:
    _attach(conn, writable_worker_location)
    for i, name in enumerate(["a", "b", "c"]):
        conn.execute(
            'INSERT INTO "main.items_insert_only" (id, name, rowid) VALUES (?, ?, NULL)', (i, name)
        )
    rows = conn.execute('SELECT id, name FROM "main.items_insert_only" ORDER BY id').fetchall()
    assert rows == [(0, "a"), (1, "b"), (2, "c")]


def test_insert_into_full_items_table(conn: sqlite3.Connection, writable_worker_location: str) -> None:
    """`items` (unlike items_insert_only) also declares supports_update/
    supports_delete=True - not exercised by this test, just confirms
    INSERT works on a table where those flags happen to be set too."""
    _attach(conn, writable_worker_location)
    conn.execute(
        'INSERT INTO "main.items" (id, name, qty, rowid) VALUES (?, ?, ?, NULL)', (7, "widget", 3)
    )
    rows = conn.execute('SELECT id, name, qty FROM "main.items"').fetchall()
    assert rows == [(7, "widget", 3)]


def test_update_not_yet_supported(conn: sqlite3.Connection, writable_worker_location: str) -> None:
    """Documented gap (see vgi_vtab.cpp's xUpdate file comment) - must fail
    clearly, not silently no-op or corrupt data."""
    _attach(conn, writable_worker_location)
    conn.execute('INSERT INTO "main.items" (id, name, qty, rowid) VALUES (?, ?, ?, NULL)', (1, "x", 1))
    with pytest.raises(sqlite3.OperationalError):
        conn.execute('UPDATE "main.items" SET qty = 2 WHERE id = 1')


def test_delete_not_yet_supported(conn: sqlite3.Connection, writable_worker_location: str) -> None:
    _attach(conn, writable_worker_location)
    conn.execute('INSERT INTO "main.items" (id, name, qty, rowid) VALUES (?, ?, ?, NULL)', (1, "x", 1))
    with pytest.raises(sqlite3.OperationalError):
        conn.execute('DELETE FROM "main.items" WHERE id = 1')
