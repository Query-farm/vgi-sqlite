# © Copyright 2026 Query Farm LLC - https://query.farm
"""HTTP transport with bearer-token auth: see VgiConnection's file comment
(src/rpc/vgi_connection.h) for the full design - vgi_rpc::RpcClient
(subprocess, every other test in this suite) and vgi_rpc::HttpClient are
structurally distinct types unified behind VgiConnection's
CallUnary/OpenProducer/OpenExchange surface, selected by a location
string's http://​/https:// prefix, with a bearer token folded into
that same location string as URL userinfo.

Runs the exact same assertions test_scan.py/test_scalar_functions.py/
test_aggregates.py/test_transactions.py already use against the subprocess
transport - the point isn't new behavior, it's confirming every RPC shape
(unary, producer stream, exchange stream) actually works over the second
transport too, not just that it doesn't crash. Uses the http_worker_base_url
session fixture (conftest.py) - skipped automatically if vgi-python's
optional `http` extra isn't installed.
"""

from __future__ import annotations

import sqlite3

import pytest


def test_attach_rejects_missing_bearer_token(conn: sqlite3.Connection, http_worker_base_url: str) -> None:
    """Proves auth is actually enforced by the server, not just plumbed
    through and silently ignored - a wrong implementation could easily
    "work" (attach succeeds) while never sending the Authorization header
    at all, since vgi-fixture-http's own fallback mode never rejects
    anything when VGI_BEARER_TOKENS isn't set (see http_worker_base_url's
    docstring) - the http_worker_base_url fixture explicitly sets it so
    this distinction is real."""
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("SELECT vgi_attach(?, 'example')", (http_worker_base_url,))


def test_attach_rejects_wrong_bearer_token(conn: sqlite3.Connection, http_worker_base_url: str) -> None:
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("SELECT vgi_attach(?, 'example', 'not-the-right-token')", (http_worker_base_url,))


def test_attach_discovers_tables_over_http(
    conn: sqlite3.Connection, http_worker_base_url: str, http_bearer_token: str
) -> None:
    (count,) = conn.execute(
        "SELECT vgi_attach(?, 'example', ?)", (http_worker_base_url, http_bearer_token)
    ).fetchone()
    # Same floor as test_attach.py's subprocess-transport version - a
    # handful of vgi-fixture-worker's own tables are skipped regardless of
    # transport (see that file's comment).
    assert count > 30


def test_scan_over_http(conn: sqlite3.Connection, http_worker_base_url: str, http_bearer_token: str) -> None:
    """A producer stream (VgiConnection::OpenProducer -> HttpStreamSession)
    carrying real row data, not just a unary call."""
    conn.execute("SELECT vgi_attach(?, 'example', ?)", (http_worker_base_url, http_bearer_token))
    (total,) = conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    assert total == 100
    (over_50,) = conn.execute('SELECT count(*) FROM "data.numbers" WHERE value > 50').fetchone()
    assert over_50 == 49


def test_scalar_function_over_http(
    conn: sqlite3.Connection, http_worker_base_url: str, http_bearer_token: str
) -> None:
    """An exchange stream (VgiConnection::OpenExchange -> HttpStreamSession),
    the other stream kind besides a plain producer scan."""
    conn.execute("SELECT vgi_attach(?, 'example', ?)", (http_worker_base_url, http_bearer_token))
    rows = conn.execute(
        'SELECT value, example_add_values(value, 100) FROM "data.numbers" ORDER BY value LIMIT 5'
    ).fetchall()
    assert rows == [(i, i + 100) for i in range(5)]


def test_aggregate_group_by_over_http(
    conn: sqlite3.Connection, http_worker_base_url: str, http_bearer_token: str
) -> None:
    """Aggregate bind/update/finalize (all unary calls) over HTTP, with
    GROUP BY isolation - the real regression risk per test_aggregates.py's
    subprocess-transport version of this same test."""
    conn.execute("SELECT vgi_attach(?, 'example', ?)", (http_worker_base_url, http_bearer_token))
    rows = conn.execute(
        'SELECT value % 3 AS grp, example_vgi_sum(value), example_vgi_count() '
        'FROM "data.numbers" GROUP BY grp ORDER BY grp'
    ).fetchall()
    assert len(rows) == 3
    assert sum(count for _, _, count in rows) == 100
    assert sum(total for _, total, _ in rows) == sum(range(100))


def test_explicit_transaction_spans_multiple_tables_over_http(
    conn: sqlite3.Connection, http_worker_base_url: str, http_bearer_token: str
) -> None:
    """The transaction RPCs (catalog_transaction_begin/commit) over HTTP -
    same scenario as test_transactions.py's
    test_explicit_transaction_spans_multiple_tables, the core case
    ConnectionPool's ref-counted transaction coordination exists for."""
    conn.execute("SELECT vgi_attach(?, 'example', ?)", (http_worker_base_url, http_bearer_token))
    conn.execute("BEGIN")
    (numbers_count,) = conn.execute('SELECT count(*) FROM "data.numbers"').fetchone()
    (ten_thousand_count,) = conn.execute('SELECT count(*) FROM "data.ten_thousand_table"').fetchone()
    conn.execute("COMMIT")
    assert numbers_count == 100
    assert ten_thousand_count == 10_000
