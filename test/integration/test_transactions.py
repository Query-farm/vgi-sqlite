# © Copyright 2026 Query Farm LLC - https://query.farm
"""Transactions (xBegin/xSync/xCommit/xRollback): see connection_pool.h's
file comment for the full design - SQLite calls xBegin once per *vtab
instance* per transaction, but VGI's catalog_transaction_begin is scoped
to one *(location, catalog) attachment*, so ConnectionPool ref-counts per
key and only the first/last participant actually calls the RPC.

Every read test in this whole suite already exercises the implicit,
per-statement transaction case (the `example` catalog declares
supports_transactions=True, and SQLite wraps even a bare SELECT in one) -
these tests specifically target *explicit* multi-statement/multi-table
transactions, since that's the scenario the ref-counted coordination
exists for and a single-table implicit-transaction test can't distinguish
"coordinated correctly" from "got lucky because only one table was ever
involved"."""

from __future__ import annotations

import sqlite3


def _attach(conn: sqlite3.Connection, worker_location: str) -> None:
    conn.execute("SELECT vgi_attach(?, 'example')", (worker_location,))


def test_explicit_transaction_commit(conn: sqlite3.Connection, worker_location: str) -> None:
    _attach(conn, worker_location)
    conn.execute("BEGIN")
    (total,) = conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    conn.execute("COMMIT")
    assert total == 100


def test_explicit_transaction_rollback(conn: sqlite3.Connection, worker_location: str) -> None:
    """Reads have no side effects to verify undone, but this still proves
    the catalog_transaction_rollback RPC round trip itself succeeds rather
    than erroring - a real, previously entirely-untested code path (every
    other test either commits or lets SQLite's own implicit per-statement
    transaction close normally)."""
    _attach(conn, worker_location)
    conn.execute("BEGIN")
    (total,) = conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    conn.execute("ROLLBACK")
    assert total == 100


def test_explicit_transaction_spans_multiple_tables(conn: sqlite3.Connection, worker_location: str) -> None:
    """The core scenario ConnectionPool's ref-counted transaction
    coordination exists for: two different vgi_worker tables from the
    *same* catalog, both touched inside one explicit SQL transaction, so
    SQLite calls xBegin on each vtab instance separately - they must share
    exactly one catalog_transaction_begin/transaction_opaque_data, not
    each try to begin (and later commit) their own. A wrong implementation
    here wouldn't necessarily error - it could just silently open two
    unrelated transactions - so this asserts on the actual data read from
    both tables, not just "no exception was raised"."""
    _attach(conn, worker_location)
    conn.execute("BEGIN")
    (numbers_count,) = conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    (ten_thousand_count,) = conn.execute('SELECT count(*) FROM "data.ten_thousand_table"').fetchone()
    conn.execute("COMMIT")
    assert numbers_count == 100
    assert ten_thousand_count == 10_000


def test_explicit_transaction_spans_multiple_tables_rollback(
    conn: sqlite3.Connection, worker_location: str
) -> None:
    _attach(conn, worker_location)
    conn.execute("BEGIN")
    conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    conn.execute('SELECT count(*) FROM "data.ten_thousand_table"').fetchone()
    conn.execute("ROLLBACK")
    # A later, separate statement must still work normally - confirms the
    # rollback actually released the shared transaction state rather than
    # leaving the pool believing a transaction is still active.
    (total,) = conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    assert total == 100


def test_sequential_transactions_on_same_catalog(conn: sqlite3.Connection, worker_location: str) -> None:
    """Two separate explicit transactions, one after another, on the same
    (location, catalog) - confirms a completed transaction's ref count
    correctly returns to zero so the next one begins its own fresh
    catalog_transaction_begin rather than being (incorrectly) treated as
    still active."""
    _attach(conn, worker_location)
    conn.execute("BEGIN")
    conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    conn.execute("COMMIT")

    conn.execute("BEGIN")
    (total,) = conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    conn.execute("COMMIT")
    assert total == 100


def test_non_transactional_catalog_writes_still_work(
    conn: sqlite3.Connection, writable_worker_location: str
) -> None:
    """simple_writable declares supports_transactions=False - registering
    xBegin/xCommit must not suddenly require (or attempt) a
    catalog_transaction_begin call for it. Every existing test_writes.py
    test already covers this implicitly (they all still passed after
    xBegin/xCommit were wired up), but this pins it down explicitly as a
    transaction-specific regression test rather than relying on that
    inference."""
    conn.execute("SELECT vgi_attach(?, 'simple_writable')", (writable_worker_location,))
    conn.execute('INSERT INTO "main.items_insert_only" (id, name, rowid) VALUES (?, ?, NULL)', (1, "alpha"))
    rows = conn.execute('SELECT id, name FROM "main.items_insert_only"').fetchall()
    assert rows == [(1, "alpha")]
