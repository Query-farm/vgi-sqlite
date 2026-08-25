# © Copyright 2026 Query Farm LLC - https://query.farm
"""LIMIT pushdown (xBestIndex's SQLITE_INDEX_CONSTRAINT_LIMIT ->
InitRequest.row_limit).

Scoped deliberately narrow (see vgi_vtab.cpp's xBestIndex comment): only
pushed when there's no other WHERE constraint on this table's scan and no
ORDER BY requested of it - both of vgi-fixture-worker's fixtures don't
expose a row_limit echo the way filter_echo_table echoes pushdown_filters,
so these tests pin down *correctness* under the scoping rule (plain LIMIT
gets the right rows; LIMIT alongside WHERE or ORDER BY - where it must NOT
be pushed - still gets the right rows too) rather than proving the wire
bytes directly.
"""

from __future__ import annotations

import sqlite3


def _attach(conn: sqlite3.Connection, worker_location: str) -> None:
    conn.execute("SELECT vgi_attach(?, 'example')", (worker_location,))


def test_plain_limit_returns_correct_row_count(conn: sqlite3.Connection, worker_location: str) -> None:
    """No WHERE, no ORDER BY - the case this driver actually pushes
    row_limit down for. Must still return exactly N rows, matching the
    first N of an unlimited scan (whatever order the worker naturally
    produces)."""
    _attach(conn, worker_location)
    full = conn.execute('SELECT n FROM "data.ten_thousand_table"').fetchall()
    limited = conn.execute('SELECT n FROM "data.ten_thousand_table" LIMIT 7').fetchall()
    assert limited == full[:7]


def test_limit_with_where_still_correct(conn: sqlite3.Connection, worker_location: str) -> None:
    """WHERE + LIMIT together: per xBestIndex's scoping rule, row_limit is
    NOT pushed here (constraints is non-empty), so this must fall back to
    SQLite fetching everything matching the WHERE and applying LIMIT
    itself - still correct, just not the optimized path. A regression here
    would mean the scoping rule's guard isn't actually preventing the
    unsafe combination."""
    _attach(conn, worker_location)
    full = conn.execute('SELECT value FROM "data.numbers" WHERE value > 2 ORDER BY value').fetchall()
    limited = conn.execute(
        'SELECT value FROM "data.numbers" WHERE value > 2 ORDER BY value LIMIT 2'
    ).fetchall()
    assert limited == full[:2]


def test_limit_with_order_by_still_correct(conn: sqlite3.Connection, worker_location: str) -> None:
    """ORDER BY + LIMIT (Top-N): per xBestIndex's scoping rule, row_limit is
    NOT pushed here (info->nOrderBy != 0) - SQLite must see every row to
    sort correctly before trimming to N. A regression here (pushing
    row_limit despite the requested order) could silently return the
    wrong N rows - "first N in scan order" instead of "first N in sorted
    order"."""
    _attach(conn, worker_location)
    full_sorted = conn.execute('SELECT n FROM "data.ten_thousand_table" ORDER BY n DESC').fetchall()
    top_5 = conn.execute('SELECT n FROM "data.ten_thousand_table" ORDER BY n DESC LIMIT 5').fetchall()
    assert top_5 == full_sorted[:5]
