# © Copyright 2026 Query Farm LLC - https://query.farm
"""Blended table_in_out (RowTransformFunction) support: vgi_attach()
registers each such catalog function as its own "vgi_table_in_out"
virtual table, callable both as a literal table-valued-function call
(`SELECT * FROM catalog_fn(1, 2)`) and, more to the point, correlated per
outer row exactly like SQLite's own `FROM t, json_each(t.x)` - no LATERAL
keyword needed (see src/vtab/vgi_table_in_out_vtab.h's file comment for
the full design and why this was originally believed out of scope).

Against the `row_transform` fixture worker (fixtures/row_transform_worker.py,
this repo's own - no vgi-fixture-worker fixture exercises this shape, see
that file's comment), not `example`."""

from __future__ import annotations

import sqlite3


def _attach(conn: sqlite3.Connection, worker_location: str) -> None:
    conn.execute("SELECT vgi_attach(?, 'row_transform')", (worker_location,))


def test_attach_registers_both_functions_as_tables(
    conn: sqlite3.Connection, row_transform_worker_location: str
) -> None:
    _attach(conn, row_transform_worker_location)
    names = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    assert {"row_transform_add_row", "row_transform_repeat_row"} <= names


def test_literal_call(conn: sqlite3.Connection, row_transform_worker_location: str) -> None:
    """The non-correlated call shape - `SELECT * FROM fn(literal, literal)`
    - same table-valued-function machinery, just with constant arguments
    instead of a correlated column."""
    _attach(conn, row_transform_worker_location)
    assert conn.execute("SELECT * FROM row_transform_add_row(3, 4)").fetchall() == [(7,)]


def test_missing_argument_rejected_at_prepare_time(
    conn: sqlite3.Connection, row_transform_worker_location: str
) -> None:
    """A table_in_out call with a missing required argument can't be
    bound at all - xBestIndex rejects every candidate plan
    (SQLITE_CONSTRAINT), which SQLite surfaces at *prepare* time ("no
    query solution"), not as a runtime error or (worse) a silent
    wrong-arity call to the worker."""
    _attach(conn, row_transform_worker_location)
    try:
        conn.execute("SELECT * FROM row_transform_add_row(3)").fetchall()
    except sqlite3.OperationalError as e:
        assert "no query solution" in str(e)
    else:
        raise AssertionError("expected a missing-argument query to fail to prepare")


def test_correlated_call_one_row_per_input_row(
    conn: sqlite3.Connection, row_transform_worker_location: str
) -> None:
    """The actual LATERAL-style use case: `FROM t, fn(t.x, t.y)` - SQLite
    re-invokes xFilter once per outer row (same mechanism `FROM t,
    json_each(t.x)` already uses, confirmed during this feature's design
    research), and this driver's TableInOutCaller reuses one bound
    connection/stream across every one of those calls rather than
    re-binding per row."""
    _attach(conn, row_transform_worker_location)
    conn.execute("CREATE TABLE pairs(x INTEGER, y INTEGER)")
    conn.executemany("INSERT INTO pairs VALUES (?, ?)", [(1, 2), (10, 20), (100, 200)])
    rows = conn.execute(
        "SELECT pairs.x, pairs.y, t.sum FROM pairs, row_transform_add_row(pairs.x, pairs.y) AS t "
        "ORDER BY pairs.x"
    ).fetchall()
    assert rows == [(1, 2, 3), (10, 20, 30), (100, 200, 300)]


def test_one_input_row_can_produce_many_output_rows(
    conn: sqlite3.Connection, row_transform_worker_location: str
) -> None:
    """Regression test for the specific design point the user raised
    (SumAllColumnsFunction being a genuine table_in_out contract
    violation, and the follow-up question of whether one input row could
    legitimately produce more than one output row): confirms 1-input-row
    -> N-output-rows already works with no protocol-level change, via the
    existing multi-row-batch machinery - see table_in_out_caller.h's file
    comment on why Exchange()'s single response batch has no row-count
    cap."""
    _attach(conn, row_transform_worker_location)
    assert conn.execute("SELECT * FROM row_transform_repeat_row(7, 3)").fetchall() == [(7,), (7,), (7,)]


def test_one_input_row_can_produce_zero_output_rows(
    conn: sqlite3.Connection, row_transform_worker_location: str
) -> None:
    """The other edge of the same case: a present-but-empty response
    batch is a legitimate "this row produced nothing" result, not an
    error and not end-of-stream (see table_in_out_caller.h's Exchange()
    doc comment on the wire protocol's exact contract here)."""
    _attach(conn, row_transform_worker_location)
    assert conn.execute("SELECT * FROM row_transform_repeat_row(7, 0)").fetchall() == []


def test_correlated_call_with_varying_output_row_counts(
    conn: sqlite3.Connection, row_transform_worker_location: str
) -> None:
    """Combines the two regression points above with real correlation:
    each outer row's `n` independently controls how many rows THAT row
    expands to (2, 0, 1) - would catch a bug that accidentally shared
    state (e.g. a stale batch, or a miscounted row-in-batch cursor)
    across successive xFilter calls on the same cursor."""
    _attach(conn, row_transform_worker_location)
    conn.execute("CREATE TABLE reps(v INTEGER, n INTEGER)")
    conn.executemany("INSERT INTO reps VALUES (?, ?)", [(1, 2), (2, 0), (3, 1)])
    rows = conn.execute(
        "SELECT reps.v, t.value FROM reps, row_transform_repeat_row(reps.v, reps.n) AS t "
        "ORDER BY reps.v, t.value"
    ).fetchall()
    assert rows == [(1, 1), (1, 1), (3, 3)]
