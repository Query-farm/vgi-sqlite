# © Copyright 2026 Query Farm LLC - https://query.farm
"""Aggregate functions: vgi_attach() registers each worker aggregate as a
native SQLite aggregate (sqlite3_create_function_v2's xStep/xFinal) under
"<catalog>_<function>", same flat-namespace convention scalar functions
use. One VGI execution_id is bound per SQLite aggregate context - see
aggregate_caller.h's file comment on why that's the correct mapping for
both whole-table and GROUP BY aggregation, verified below by GROUP BY
tests that would silently mix group results if it weren't."""

from __future__ import annotations

import sqlite3


def _attach(conn: sqlite3.Connection, worker_location: str) -> None:
    conn.execute("SELECT vgi_attach(?, 'example')", (worker_location,))


def test_whole_table_sum(conn: sqlite3.Connection, worker_location: str) -> None:
    _attach(conn, worker_location)
    (total,) = conn.execute('SELECT example_vgi_sum(value) FROM "data.numbers"').fetchone()
    # data.numbers holds 0..99.
    assert total == sum(range(100))


def test_whole_table_nullary_count(conn: sqlite3.Connection, worker_location: str) -> None:
    _attach(conn, worker_location)
    (count,) = conn.execute('SELECT example_vgi_count() FROM "data.numbers"').fetchone()
    assert count == 100


def test_group_by_isolates_each_group(conn: sqlite3.Connection, worker_location: str) -> None:
    """Regression test for the core design decision (aggregate_caller.h):
    one AggregateCaller/execution_id per SQLite aggregate context. If two
    groups' state were ever shared or crossed, these sums/counts would be
    wrong in a way a single-group test can't catch."""
    _attach(conn, worker_location)
    rows = conn.execute(
        'SELECT value % 3 AS grp, example_vgi_sum(value), example_vgi_count() '
        'FROM "data.numbers" GROUP BY grp ORDER BY grp'
    ).fetchall()
    expected = []
    for grp in range(3):
        values = [v for v in range(100) if v % 3 == grp]
        expected.append((grp, sum(values), len(values)))
    assert rows == expected


def test_two_aggregates_in_one_query_dont_interfere(conn: sqlite3.Connection, worker_location: str) -> None:
    """vgi_sum and vgi_count each get their own registered SQLite function
    and their own AggregateCaller per row group - confirms running both
    together (already exercised implicitly by the GROUP BY test above)
    also gives correct, independent results ungrouped."""
    _attach(conn, worker_location)
    (total, count) = conn.execute(
        'SELECT example_vgi_sum(value), example_vgi_count() FROM "data.numbers"'
    ).fetchone()
    assert total == sum(range(100))
    assert count == 100


def test_empty_group_result(conn: sqlite3.Connection, worker_location: str) -> None:
    """An aggregate over zero rows never binds (see AggregateFinalBridge) -
    must report NULL, not error or crash."""
    _attach(conn, worker_location)
    (total,) = conn.execute('SELECT example_vgi_sum(value) FROM "data.numbers" WHERE value > 1000000').fetchone()
    assert total is None
