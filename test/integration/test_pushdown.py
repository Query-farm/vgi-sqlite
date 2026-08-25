# © Copyright 2026 Query Farm LLC - https://query.farm
"""Projection pushdown (xBestIndex's colUsed -> InitRequest.projection_ids)."""

from __future__ import annotations

import sqlite3


def _attach(conn: sqlite3.Connection, worker_location: str) -> None:
    conn.execute("SELECT vgi_attach(?, 'example')", (worker_location,))


def test_single_column_select_returns_that_columns_values(
    conn: sqlite3.Connection, worker_location: str
) -> None:
    """Regression test: a function that doesn't honor projection_ids
    (observed against vgi-fixture-worker's cache_multicol - it returns
    every column regardless) must still read the *right* column, not
    whatever ended up at position 0 of the unnarrowed batch. See
    xColumn's width-check comment in vgi_vtab.cpp."""
    _attach(conn, worker_location)
    full_rows = conn.execute('SELECT a, b, c FROM "data.cache_multicol" ORDER BY a LIMIT 3').fetchall()
    assert len(full_rows) == 3

    b_only = conn.execute('SELECT b FROM "data.cache_multicol" ORDER BY a LIMIT 3').fetchall()
    assert [r[0] for r in b_only] == [row[1] for row in full_rows]


def test_two_column_select_returns_correct_columns_in_order(
    conn: sqlite3.Connection, worker_location: str
) -> None:
    _attach(conn, worker_location)
    full_rows = conn.execute('SELECT a, b, c FROM "data.cache_multicol" ORDER BY a LIMIT 3').fetchall()

    a_c = conn.execute('SELECT a, c FROM "data.cache_multicol" ORDER BY a LIMIT 3').fetchall()
    assert [(r[0], r[1]) for r in a_c] == [(row[0], row[2]) for row in full_rows]
