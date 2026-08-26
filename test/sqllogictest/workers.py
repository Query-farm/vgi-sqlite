# © Copyright 2026 Query Farm LLC - https://query.farm
"""Resolves the require-env variable names the vgi sqllogictest corpus uses
to real values in this environment - mirrors ~/Development/vgi/Makefile's
own worker-command mapping (the authoritative source for what each var
means), not a fresh guess.

A file whose `require-env X` name isn't in the resolved env - or whose
resolver's own prerequisites (e.g. VGI_PYTHON's checkout) aren't present -
is a real, correctly-reported SKIP ("require-env X"), not a failure. Several
of these are deliberately never resolved here at all: vgi's own Makefile
treats them the same way in its most permissive CI lane (see its
VGI_EXPECTED_SKIPS list) - an external runtime this test environment has no
reason to stand up (docker, iceberg, github-network, a *_HTTP_TRANSPORT
variant lane, a launcher/companion/rules worker this driver doesn't
implement the protocol surface for at all).

Persistent workers (start_persistent_workers/PersistentWorkers): each
resolvable fixture worker is, by default, a `uv run ... vgi-fixture-*`
*subprocess-spawn command string* - VgiConnection::spawn (see
rpc/vgi_connection.h) launches a brand-new process for it on every single
ATTACH. Across a 327-file run where the overwhelming majority (308 of 333
require-env occurrences corpus-wide) is VGI_TEST_WORKER alone, that means
hundreds of `uv run` + Python-interpreter-startup round trips - real,
measurable wall-clock cost, not spread across anything. Every fixture
worker binary here also supports `--unix PATH` (bind to a socket instead of
stdin/stdout, confirmed via each one's own --help), and vgi-sqlite's
VgiConnection::Connect now speaks `unix://PATH` (connects to an
already-running worker rather than spawning one - see vgi_connection.h's
file comment on why this is deliberately smaller than the full
launcher-protocol discovery contract). start_persistent_workers() launches
each resolvable worker exactly ONCE, bound to a socket, and every
subsequent ATTACH across the whole run reuses that one long-lived process
over unix:// instead of spawning a fresh one - the same reuse a real
launcher-protocol deployment would give this driver, just started
explicitly by this test runner instead of on-demand-discovered by it.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# {ENV_VAR_NAME: fixture worker console-script name} - every worker this
# runner knows how to both spawn-per-connection (the fallback) and, when
# start_persistent_workers() is used, run once and reuse over unix://.
_FIXTURE_SCRIPTS = {
    "VGI_TEST_WORKER": "vgi-fixture-worker",
    "VGI_SIMPLE_WRITABLE_WORKER": "vgi-fixture-simple-writable-worker",
    "VGI_ATTACH_OPTIONS_WORKER": "vgi-fixture-attach-options-worker",
    "VGI_ATTACH_OPTIONS_REQUIRED_WORKER": "vgi-fixture-attach-options-worker",
    "VGI_VERSIONED_WORKER": "vgi-fixture-versioned-worker",
    "VGI_VERSIONED_TABLES_WORKER": "vgi-fixture-versioned-tables-worker",
    "VGI_BAD_PROTOCOL_WORKER": "vgi-fixture-bad-protocol-worker",
    "VGI_BAD_ENUM_WORKER": "vgi-fixture-bad-enum-worker",
}


def _vgi_python() -> str:
    return os.environ.get("VGI_PYTHON", str(Path.home() / "Development" / "vgi-python"))


def _worker_cmd(script: str) -> str:
    return f"uv run --project {_vgi_python()} {script}"


def resolve_env(extra_scratch_dir: Path | None = None) -> dict[str, str]:
    """Build the {var_name: value} map used to satisfy `require-env` and
    `${VAR}` interpolation, with every fixture worker as a spawn-per-
    connection command string (see this module's docstring for the
    persistent-worker alternative). Called once per run (not per file) -
    the temp-file-backed values (VGI_SCHEMA_RECONCILE_DB) are real
    filesystem paths shared across every file in the run, matching how a
    real `make test` invocation shares one across its whole session too."""
    scratch = extra_scratch_dir or Path(tempfile.mkdtemp(prefix="vgi_sqllogictest_"))
    scratch.mkdir(parents=True, exist_ok=True)

    env: dict[str, str] = {var: _worker_cmd(script) for var, script in _FIXTURE_SCRIPTS.items()}
    env["VGI_TEST_BRANCH_DIR"] = tempfile.gettempdir()
    env["VGI_SCHEMA_RECONCILE_DB"] = str(scratch / "vgi_schema_reconcile.sqlite")

    # Allow the environment to override/extend any of the above, matching
    # every other convention in this repo's own test/integration/conftest.py
    # (VGI_TEST_WORKER, VGI_SIMPLE_WRITABLE_WORKER overrides).
    for key in list(env):
        if key in os.environ:
            env[key] = os.environ[key]
    return env


class PersistentWorkers:
    """Owns zero or more long-lived `vgi-fixture-* --unix <socket>`
    subprocesses, one per fixture this run actually needs. Use as a context
    manager (or call .stop_all() explicitly) - every process is terminated
    on exit, socket files included."""

    def __init__(self) -> None:
        self._procs: list[subprocess.Popen] = []
        self._sock_dir = Path(tempfile.mkdtemp(prefix="vgi_sqllogictest_sockets_"))

    def __enter__(self) -> "PersistentWorkers":
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop_all()

    def start_all(self, base_env: dict[str, str], pool_size: int = 4) -> dict[str, list[str]]:
        """Starts `pool_size` persistent unix-socket-bound processes PER
        fixture worker in _FIXTURE_SCRIPTS, and returns
        {ENV_VAR: [list of "unix://<socket path>", one per pool member]} to
        pick from (round-robin/hash) and merge over base_env.

        Why a pool, not one process per fixture: `--unix` serves each
        connection on its own daemon thread (`--threaded`, its own default
        for `--unix`/`--tcp`), but these are CPython worker processes - a
        thread's actual request handling (Arrow/IPC (de)serialization,
        MetaWorker dispatch, ordinary Python glue) mostly holds the GIL, so
        N client connections against ONE worker process don't get N-way
        real parallelism, just interleaving. Measured directly on this
        driver's own table/ category (59 files): --jobs 1 (one client
        subprocess at a time) took 33.2s wall / 29.4s CPU against a
        single-instance pool; --jobs 16 against that SAME single instance
        only dropped wall time to 24.2s while CPU time roughly DOUBLED
        (58.7s, thread-scheduling/GIL-contention overhead) - i.e. 16x
        client-side concurrency bought roughly 1.4x wall-clock improvement,
        confirming the single worker process, not client-side parallelism,
        was the bottleneck. A small pool of real OS processes (each with
        its own GIL) fixes that the way threads within one process can't.
        A worker that fails to start within the readiness window (missing
        `uv`/vgi-python checkout, etc.) is simply left out of its var's
        list - if a var's list ends up empty, that var keeps its
        spawn-per-connection command string from base_env instead (slower
        for that one fixture, not a failure)."""
        pending: list[tuple[str, Path, subprocess.Popen]] = []
        for var, script in _FIXTURE_SCRIPTS.items():
            cmd = base_env.get(var, _worker_cmd(script)).split()
            # base_env's value is a full "uv run --project <dir> <script>"
            # command string - reuse it verbatim (respecting any override
            # already applied) and just append the --unix/--idle-timeout
            # flags, rather than re-deriving the command from scratch.
            for i in range(pool_size):
                sock_path = self._sock_dir / f"{script}.{i}.sock"
                proc = subprocess.Popen(
                    [*cmd, "--unix", str(sock_path), "--idle-timeout", "0"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL,
                )
                pending.append((var, sock_path, proc))

        # All pool_size * len(_FIXTURE_SCRIPTS) processes are launched
        # above before waiting on any of them, so their (independent)
        # startup costs overlap instead of serializing pool_size-fold.
        overrides: dict[str, list[str]] = {}
        for var, sock_path, proc in pending:
            if self._wait_for_socket(sock_path, proc, timeout=20.0):
                overrides.setdefault(var, []).append(f"unix://{sock_path}")
                self._procs.append(proc)
            else:
                proc.terminate()
        return overrides

    @staticmethod
    def _wait_for_socket(sock_path: Path, proc: subprocess.Popen, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                return False  # exited before ever binding - real failure, not just "not yet"
            if sock_path.exists():
                try:
                    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                        s.connect(str(sock_path))
                    return True
                except OSError:
                    pass
            time.sleep(0.1)
        return False

    def stop_all(self) -> None:
        for proc in self._procs:
            proc.terminate()
        for proc in self._procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        self._procs.clear()


if __name__ == "__main__":
    # Smoke-test entry point: `python3 workers.py` starts every persistent
    # worker, prints their unix:// locations, and leaves them running for
    # 30s so they can be probed manually - not used by run_sqllogictest.py
    # itself, which manages the lifecycle inline.
    with PersistentWorkers() as pw:
        overrides = pw.start_all(resolve_env())
        for k, v in overrides.items():
            print(f"{k}={v}")
        print("(running for 30s - Ctrl+C to stop early)", file=sys.stderr)
        time.sleep(30)
