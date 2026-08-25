# © Copyright 2026 Query Farm LLC - https://query.farm
"""Shared fixtures for vgi-sqlite's integration test suite.

Drives the built extension via the stdlib sqlite3 module (enable_load_extension
+ .load), against the same fixture worker vgi's own DuckDB-extension suite
uses, for parity. Mirrors that suite's VGI_TEST_WORKER environment-variable
convention (see vgi/Makefile).
"""

from __future__ import annotations

import os
import platform
import socket
import sqlite3
import subprocess
import threading
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _extension_path() -> Path:
    override = os.environ.get("VGI_SQLITE_EXTENSION")
    if override:
        return Path(override)
    suffix = ".dylib" if platform.system() == "Darwin" else ".so"
    # CMake's MODULE library type on macOS actually produces .so by
    # default (BUNDLE/MACOSX_RPATH quirks) - try both, .load() only cares
    # that the file exists and is Mach-O/ELF-loadable, not its suffix.
    for name in (f"vgi{suffix}", "vgi.so", "vgi.dylib"):
        candidate = REPO_ROOT / "build" / name
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"vgi extension not found under {REPO_ROOT / 'build'} - build it first "
        "(cmake --build build), or set VGI_SQLITE_EXTENSION"
    )


def _default_worker() -> str:
    vgi_python = os.environ.get("VGI_PYTHON", str(Path.home() / "Development" / "vgi-python"))
    return f"uv run --project {vgi_python} vgi-fixture-worker"


@pytest.fixture(scope="session")
def extension_path() -> Path:
    return _extension_path()


@pytest.fixture(scope="session")
def worker_location() -> str:
    """The `example` catalog's worker command - VGI_TEST_WORKER, or the
    vgi-fixture-worker default (matching vgi/Makefile's convention)."""
    return os.environ.get("VGI_TEST_WORKER", _default_worker())


@pytest.fixture(scope="session")
def writable_worker_location() -> str:
    """The `simple_writable` catalog's worker command - a separate fixture
    worker binary (not one of the catalogs vgi-fixture-worker's own
    MetaWorker dispatches to), self-contained and SQLite-file-backed, no
    transactor subprocess dependency - see table_writer.h's file comment
    on why this is the write-path test target rather than the fuller
    `writable` fixture (which requires supports_transactions=True).
    VGI_SIMPLE_WRITABLE_WORKER overrides, matching vgi/Makefile's
    convention for this same fixture."""
    vgi_python = os.environ.get("VGI_PYTHON", str(Path.home() / "Development" / "vgi-python"))
    return os.environ.get(
        "VGI_SIMPLE_WRITABLE_WORKER", f"uv run --project {vgi_python} vgi-fixture-simple-writable-worker"
    )


def _free_port() -> int:
    """Bind to an OS-assigned port, then release it - a real race window
    (something else could grab it before we launch our own server) but the
    same acceptable-risk tradeoff vgi-python's own tests/_http_fixtures.py
    makes for exactly this fixture worker, rather than parsing the
    subprocess's stdout "PORT:N" line."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def http_bearer_token() -> str:
    return "vgi-sqlite-test-token"


@pytest.fixture(scope="session")
def http_worker_base_url(http_bearer_token: str):
    """Session-scoped `vgi-fixture-http` (the HTTP-transport counterpart of
    `worker_location`'s subprocess `vgi-fixture-worker`) - see
    VgiConnection::Connect's file comment (rpc/vgi_connection.h) for what
    this exercises. VGI_BEARER_TOKENS is set so the fixture enforces real
    bearer auth rather than falling back to its own never-401 anonymous
    mode (see vgi-python's http_server.py - auth is only enforced when this
    env var is non-empty). Requires vgi-python's optional `http` extra
    (`uv sync --extra http` under VGI_PYTHON) - skips (not fails) the whole
    session's HTTP-transport tests if that's missing, same
    environment-gap-not-code-bug spirit as _verify_worker_available below.

    stdout/stderr are drained on background daemon threads rather than left
    unread: an unread OS pipe fills up under load and freezes waitress's
    single I/O loop, hanging every subsequent request - found by reading
    vgi-python's own tests/_http_fixtures.py, which hits the identical
    problem for the identical fixture and documents the same fix.
    """
    vgi_python = os.environ.get("VGI_PYTHON", str(Path.home() / "Development" / "vgi-python"))
    port = _free_port()
    env = dict(os.environ)
    env["VGI_BEARER_TOKENS"] = f"{http_bearer_token}=test-principal"

    proc = subprocess.Popen(
        [
            "uv", "run", "--project", vgi_python, "vgi-fixture-http",
            "--host", "127.0.0.1", "--port", str(port), "--prefix", "/vgi",
        ],
        cwd=vgi_python,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    tail: list[str] = []

    def _drain(stream) -> None:
        for line in stream:
            tail.append(line)
            del tail[:-50]  # keep only the last 50 lines, for a failure message

    threads = [threading.Thread(target=_drain, args=(s,), daemon=True) for s in (proc.stdout, proc.stderr)]
    for t in threads:
        t.start()

    base_url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 15
    ready = False
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                ready = True
                break
        except OSError:
            time.sleep(0.2)

    if not ready:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        pytest.skip(
            f"vgi-fixture-http didn't start listening on {base_url} (rc={proc.returncode}) - "
            f"probably missing the 'http' extra (uv sync --extra http under {vgi_python}). "
            f"Last output:\n{''.join(tail)}"
        )

    yield base_url

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture()
def conn(extension_path: Path, tmp_path: Path) -> sqlite3.Connection:
    """A fresh SQLite connection (file-backed, not :memory: - CREATE VIRTUAL
    TABLE's module arguments need to round-trip through sqlite_master the
    same way a real session would exercise) with the extension loaded."""
    db_path = tmp_path / "vgi_test.db"
    connection = sqlite3.connect(str(db_path))
    connection.enable_load_extension(True)
    connection.load_extension(str(extension_path))
    connection.enable_load_extension(False)
    yield connection
    connection.close()


@pytest.fixture(scope="session", autouse=True)
def _verify_worker_available(worker_location: str) -> None:
    """Fail fast with a clear message if the fixture worker can't even
    start, rather than every test timing out individually."""
    argv = worker_location.split()
    try:
        result = subprocess.run(
            [*argv, "--help"], capture_output=True, timeout=30, text=True
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        pytest.exit(f"worker_location {worker_location!r} did not start: {e}")
    if result.returncode != 0:
        pytest.exit(
            f"worker_location {worker_location!r} --help failed (rc={result.returncode}):\n"
            f"{result.stderr}"
        )
