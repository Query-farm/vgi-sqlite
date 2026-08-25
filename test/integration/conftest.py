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
import sqlite3
import subprocess
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
