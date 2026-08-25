# © Copyright 2026 Query Farm LLC - https://query.farm
"""Writable tables (xUpdate): INSERT, UPDATE, DELETE. UPDATE/DELETE
identify their target row via VGI's declared is_row_id column, mapped onto
SQLite's own rowid contract - see vgi_vtab.cpp's xRowid/xUpdate file
comments. Tested against vgi-fixture-worker's `simple_writable` catalog
(see table_writer.h/conftest.py's writable_worker_location for why that
fixture, not `writable`)."""

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


def test_update_changes_the_row(conn: sqlite3.Connection, writable_worker_location: str) -> None:
    _attach(conn, writable_worker_location)
    conn.execute('INSERT INTO "main.items" (id, name, qty, rowid) VALUES (?, ?, ?, NULL)', (1, "x", 1))
    conn.execute('UPDATE "main.items" SET qty = 99 WHERE id = 1')
    rows = conn.execute('SELECT id, name, qty FROM "main.items"').fetchall()
    assert rows == [(1, "x", 99)]


def test_update_only_matching_row(conn: sqlite3.Connection, writable_worker_location: str) -> None:
    """Regression test for VGI's row identification: UPDATE must target
    the row VGI's is_row_id column names (mapped onto SQLite's rowid via
    xRowid), not whichever row happens to be first/last - a wrong mapping
    here would still "work" on a single-row table but silently update the
    wrong row once there's more than one."""
    _attach(conn, writable_worker_location)
    for i in range(5):
        conn.execute('INSERT INTO "main.items" (id, name, qty, rowid) VALUES (?, ?, ?, NULL)', (i, "x", 0))
    conn.execute('UPDATE "main.items" SET qty = 42 WHERE id = 3')
    rows = conn.execute('SELECT id, qty FROM "main.items" ORDER BY id').fetchall()
    assert rows == [(0, 0), (1, 0), (2, 0), (3, 42), (4, 0)]


def test_delete_removes_the_row(conn: sqlite3.Connection, writable_worker_location: str) -> None:
    _attach(conn, writable_worker_location)
    conn.execute('INSERT INTO "main.items" (id, name, qty, rowid) VALUES (?, ?, ?, NULL)', (1, "x", 1))
    conn.execute('DELETE FROM "main.items" WHERE id = 1')
    (count,) = conn.execute('SELECT count(*) FROM "main.items"').fetchone()
    assert count == 0


def test_delete_only_matching_row(conn: sqlite3.Connection, writable_worker_location: str) -> None:
    _attach(conn, writable_worker_location)
    for i in range(5):
        conn.execute('INSERT INTO "main.items" (id, name, qty, rowid) VALUES (?, ?, ?, NULL)', (i, "x", 0))
    conn.execute('DELETE FROM "main.items" WHERE id = 3')
    rows = conn.execute('SELECT id FROM "main.items" ORDER BY id').fetchall()
    assert rows == [(0,), (1,), (2,), (4,)]


def test_update_unsupported_table_errors(conn: sqlite3.Connection, writable_worker_location: str) -> None:
    """items_insert_only declares supports_update=False - must fail
    clearly, not silently no-op."""
    _attach(conn, writable_worker_location)
    conn.execute('INSERT INTO "main.items_insert_only" (id, name, rowid) VALUES (?, ?, NULL)', (1, "x"))
    with pytest.raises(sqlite3.OperationalError):
        conn.execute('UPDATE "main.items_insert_only" SET name = ? WHERE id = 1', ("y",))


def test_delete_unsupported_table_errors(conn: sqlite3.Connection, writable_worker_location: str) -> None:
    _attach(conn, writable_worker_location)
    conn.execute('INSERT INTO "main.items_insert_only" (id, name, rowid) VALUES (?, ?, NULL)', (1, "x"))
    with pytest.raises(sqlite3.OperationalError):
        conn.execute('DELETE FROM "main.items_insert_only" WHERE id = 1')
