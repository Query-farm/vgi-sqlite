#!/usr/bin/env python3
# © Copyright 2026 Query Farm LLC - https://query.farm
"""Runs ~/Development/vgi's DuckDB-dialect sqllogictest corpus against the
built vgi-sqlite extension, translating what's mechanically translatable
(translate.py) and reporting pass/fail/skip per individual statement/query
record - not just per file - so the count this is meant to track
("continuously increase the number of successfully executed tests") is a
real, sensitive number rather than a blunt per-file verdict.

Why this exists, and why it isn't "port all 333 files": CLAUDE.md and the
plan file both already establish that ~/Development/vgi's suite is not
directly portable (DuckDB SQL dialect: `ATTACH (TYPE vgi, ...)`, `::CAST`,
STRUCT/list/unsigned/decimal types, `range()`/`generate_series()`,
catalog-qualified table-function calls, DuckDB-extension-only introspection
like `vgi_result_cache()`/`duckdb_views()`/`vgi_global_functions()`) - but
it's still the single largest, most battle-tested collection of VGI
protocol/behavior assertions that exists anywhere, spanning years of bugs
found against real workers. Hand-porting each file's assertions into
test/integration/'s pytest suite (this repo's actual test source of record
- see CLAUDE.md's "Definition of done") is real, valuable, ongoing work;
this tool exists to make that work *incremental and measurable* instead of
all-or-nothing - run it, see what already passes for free, see exactly why
everything else doesn't (a stable, groupable reason string per skip/fail),
and treat closing the largest reason category as the next slice of work.

Every file was surveyed by category before writing translate.py's rule set
and this module's STRUCTURAL_SKIP_CATEGORIES - see the plan file's Milestone
6 status for the category-by-category findings (result-cache tests need a
query-cache layer this driver doesn't have; table_in_out/secret/launcher/
macro/view/global_functions/overload/settings all call SQL surface
this driver's vtab-only, fixed-arguments-at-CREATE-TIME model has no
equivalent for; COPY TO/FROM has no SQLite statement to map onto).
Structural skips are reported with a `category: <dir>` reason distinct from
an ordinary `require-env`/translation-rule skip, so the two are never
conflated in the summary.

Usage:
    python3 test/sqllogictest/run_sqllogictest.py
    python3 test/sqllogictest/run_sqllogictest.py --update-baseline
    python3 test/sqllogictest/run_sqllogictest.py -k filter_pushdown -v
    python3 test/sqllogictest/run_sqllogictest.py --min-executed 400 \\
        --allow-skip 'category: table_in_out' --allow-skip 'require-env VGI_DOCKER_IMAGE'

See README.md in this directory for the full design and how to extend
translate.py's rules to grow the passing count.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import platform
import sqlite3
import subprocess
import sys
import tempfile
import zlib
from dataclasses import asdict, dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import comparison  # noqa: E402
import parser as sqllt_parser  # noqa: E402
import workers  # noqa: E402
from parser import Directive, Query, Statement, TestFile  # noqa: E402
from translate import TranslationContext, Untranslatable, translate_sql  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BASELINE = Path(__file__).resolve().parent / "baseline.json"

# Whole categories (top-level directories under the corpus's test/sql/
# integration/) with no vgi-sqlite equivalent AT ALL, regardless of which
# fixture worker is available - see this module's docstring and the plan
# file's Milestone 6 status for what each one actually tests and why.
STRUCTURAL_SKIP_CATEGORIES = {
    "table_in_out": "table-in-out functions have no vgi-sqlite equivalent (explicit gap - see plan file)",
    # "splits" was here (whole-category skip) until vgi-sqlite implemented
    # sequential split redemption - see catalog_table_plan.h. Individual
    # splits/*.test files may still skip/fail for unrelated reasons
    # (parallelism-only scenarios this driver's single-reader model can't
    # exercise the same way, or ordinary untranslated DuckDB syntax), but
    # the category itself is no longer blanket-skipped.
    "secret": "DuckDB's secrets manager has no vgi-sqlite equivalent",
    "launcher": "the AF_UNIX launcher discovery protocol isn't implemented in vgi-sqlite",
    "cache": "DuckDB-extension-side VGI result cache has no vgi-sqlite equivalent",
    "copy_to": "SQL COPY has no vgi-sqlite/SQLite equivalent",
    "copy_from": "SQL COPY has no vgi-sqlite/SQLite equivalent",
    "macro": "SQL CREATE MACRO has no vgi-sqlite/SQLite equivalent",
    "overload": "catalog-qualified table-function-call syntax has no vgi-sqlite equivalent",
    "global_functions": "vgi_global_functions() introspection has no vgi-sqlite equivalent",
    "view": "duckdb_views()-based assertions have no vgi-sqlite equivalent",
    "settings": "these tests call table-in-out functions with SQL arguments - see table_in_out",
}

# require values this environment can always satisfy - DuckDB extension
# loading, not a vgi-sqlite concept, so any file requiring one of these is
# trivially "satisfied" (nothing to load). Anything else (json/parquet/
# spatial) names a real DuckDB extension whose functionality vgi-sqlite
# doesn't have a substitute for - such a file is skipped.
SATISFIED_REQUIRES = {"vgi", "httpfs", "notwindows"}


@dataclass
class RecordResult:
    status: str  # "pass" | "fail" | "skip"
    line: int
    reason: str = ""


@dataclass
class FileResult:
    relpath: str
    category: str
    name: str = ""
    file_skip_reason: str | None = None
    records: list[RecordResult] = field(default_factory=list)
    error: str | None = None  # a file-level crash (e.g. couldn't open a connection) unrelated to any one record


def run_statement(conn: sqlite3.Connection, stmt: Statement, ctx: TranslationContext) -> RecordResult:
    try:
        sql = translate_sql(stmt.sql, ctx)
    except Untranslatable as e:
        return RecordResult("skip", stmt.line, str(e))
    try:
        conn.execute(sql)
        conn.commit()
    except sqlite3.Error as e:
        if stmt.kind == "statement_error":
            if stmt.expected_error and stmt.expected_error not in str(e):
                return RecordResult("fail", stmt.line, f"error text mismatch: expected {stmt.expected_error!r}, got {e!r}")
            return RecordResult("pass", stmt.line)
        return RecordResult("fail", stmt.line, f"unexpected error: {e}")
    if stmt.kind == "statement_error":
        return RecordResult("fail", stmt.line, "expected an error, statement succeeded")
    return RecordResult("pass", stmt.line)


def run_query(conn: sqlite3.Connection, q: Query, ctx: TranslationContext) -> RecordResult:
    try:
        sql = translate_sql(q.sql, ctx)
    except Untranslatable as e:
        return RecordResult("skip", q.line, str(e))
    try:
        cur = conn.execute(sql)
        rows = cur.fetchall()
    except sqlite3.Error as e:
        return RecordResult("fail", q.line, f"query error: {e}")
    actual = [comparison.format_actual_row(row, q.type_string) for row in rows]
    ok, diff = comparison.rows_match(q, actual)
    if ok:
        return RecordResult("pass", q.line)
    return RecordResult("fail", q.line, diff)


def run_file(path: Path, source_root: Path, ext_path: Path, env: dict[str, str]) -> FileResult:
    rel = path.relative_to(source_root)
    category = rel.parts[0] if rel.parts else ""
    relpath = str(rel)

    if category in STRUCTURAL_SKIP_CATEGORIES:
        return FileResult(relpath, category, file_skip_reason=f"category: {category}")

    try:
        tf = sqllt_parser.parse_file(path)
    except Exception as e:  # noqa: BLE001 - a parse bug shouldn't crash the whole run
        return FileResult(relpath, category, error=f"parse error: {e}")

    ctx = TranslationContext(env=env)
    fd, db_path = tempfile.mkstemp(suffix=".sqlite", prefix="vgi_sqllogictest_")
    os.close(fd)
    os.remove(db_path)  # sqlite3.connect creates it fresh
    conn = None
    results: list[RecordResult] = []
    file_skip_reason: str | None = None
    try:
        conn = sqlite3.connect(db_path)
        conn.enable_load_extension(True)
        conn.load_extension(str(ext_path))
        conn.enable_load_extension(False)

        for rec in tf.records:
            if file_skip_reason is not None:
                results.append(RecordResult("skip", rec.line, file_skip_reason))
                continue
            if isinstance(rec, Directive):
                if rec.kind == "require_env" and rec.value not in env:
                    file_skip_reason = f"require-env {rec.value}"
                elif rec.kind == "require" and rec.value not in SATISFIED_REQUIRES:
                    file_skip_reason = f"require {rec.value}"
                elif rec.kind == "mode_skip":
                    file_skip_reason = "mode skip"
                # "set" (DuckDB's `set ignore_error_messages ...`, not a SQL
                # SET statement - see parser.py) and "halt" are no-ops here.
                continue
            if isinstance(rec, Statement):
                results.append(run_statement(conn, rec, ctx))
            elif isinstance(rec, Query):
                results.append(run_query(conn, rec, ctx))
    except Exception as e:  # noqa: BLE001
        return FileResult(relpath, category, name=tf.name, records=results, error=f"runner error: {e}")
    finally:
        if conn is not None:
            conn.close()
        try:
            os.remove(db_path)
        except OSError:
            pass

    return FileResult(relpath, category, name=tf.name, file_skip_reason=file_skip_reason, records=results)


def _file_result_to_dict(fr: FileResult) -> dict:
    return asdict(fr)


def _file_result_from_dict(d: dict) -> FileResult:
    return FileResult(
        relpath=d["relpath"],
        category=d["category"],
        name=d.get("name", ""),
        file_skip_reason=d.get("file_skip_reason"),
        records=[RecordResult(**r) for r in d.get("records", [])],
        error=d.get("error"),
    )


# A single crashing test file must not take the whole run down with it (see
# README.md: a real full-corpus run reliably SIGABRT-crashed the process
# partway through, exit code 134/"dumped core", even single-threaded - not
# root-caused, but real). Every file therefore runs in its OWN subprocess
# (this same script, invoked with --single-file) rather than in-process:
# a crash there is just a nonzero/abnormal exit code this process observes
# and records as a distinct FileResult.error, never something that takes
# any other file's result down with it. This also happens to be what makes
# --jobs > 1 safe again (each file is a real, independent OS process either
# way) - see README.md.
def _pick_worker_pool_member(pools: dict[str, list[str]], key: str) -> dict[str, str]:
    """One location per var, deterministically spread across each var's
    pool by `key` (a file's relpath) - see PersistentWorkers.start_all's
    docstring for why a pool exists at all. zlib.crc32, not the builtin
    hash(), so the distribution doesn't depend on PYTHONHASHSEED - not a
    correctness requirement here (this only ever runs within one process's
    lifetime), just a clearer/more debuggable choice than relying on that."""
    return {var: locs[zlib.crc32(key.encode()) % len(locs)] for var, locs in pools.items() if locs}


def run_file_isolated(
    path: Path,
    source_root: Path,
    ext_path: Path,
    timeout: float = 120.0,
    worker_pools: dict[str, list[str]] | None = None,
) -> FileResult:
    rel = path.relative_to(source_root)
    # One pool member per var, picked deterministically by this file's own
    # relpath (see _pick_worker_pool_member) - rides into the child as real
    # OS environment variables, which resolve_env() picks up via its own
    # "environment overrides every default" pass.
    env_overrides = _pick_worker_pool_member(worker_pools or {}, str(rel))
    child_env = {**os.environ, **env_overrides}
    try:
        proc = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--single-file", str(path),
             "--source-dir", str(source_root), "--extension", str(ext_path)],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=child_env,
        )
    except subprocess.TimeoutExpired:
        return FileResult(str(rel), rel.parts[0] if rel.parts else "", error=f"timed out after {timeout}s")

    if proc.returncode != 0:
        tail = "\n".join(proc.stderr.strip().splitlines()[-15:])
        return FileResult(
            str(rel),
            rel.parts[0] if rel.parts else "",
            error=f"runner subprocess exited {proc.returncode} (crashed) - last stderr:\n{tail}",
        )
    try:
        return _file_result_from_dict(json.loads(proc.stdout))
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        return FileResult(
            str(rel), rel.parts[0] if rel.parts else "", error=f"malformed subprocess output: {e}"
        )


def discover_files(source_dir: Path, category: str | None, pattern: str | None) -> list[Path]:
    files = sorted(source_dir.rglob("*.test"))
    if category:
        files = [f for f in files if f.relative_to(source_dir).parts[0] == category]
    if pattern:
        files = [f for f in files if pattern in str(f.relative_to(source_dir))]
    return files


def summarize(file_results: list[FileResult]) -> dict:
    total = {"pass": 0, "fail": 0, "skip": 0}
    by_category: dict[str, dict[str, int]] = {}
    skip_reasons: dict[str, int] = {}
    fail_details: list[tuple[str, int, str]] = []
    files_with_error = []

    for fr in file_results:
        cat_counts = by_category.setdefault(fr.category, {"pass": 0, "fail": 0, "skip": 0, "files": 0})
        cat_counts["files"] += 1
        if fr.error:
            files_with_error.append((fr.relpath, fr.error))
        for r in fr.records:
            total[r.status] += 1
            cat_counts[r.status] += 1
            if r.status == "skip":
                skip_reasons[r.reason] = skip_reasons.get(r.reason, 0) + 1
            if r.status == "fail":
                fail_details.append((fr.relpath, r.line, r.reason))

    return {
        "total": total,
        "by_category": by_category,
        "skip_reasons": skip_reasons,
        "fail_details": fail_details,
        "files_with_error": files_with_error,
    }


def passing_ids(file_results: list[FileResult]) -> set[str]:
    ids = set()
    for fr in file_results:
        for r in fr.records:
            if r.status == "pass":
                ids.add(f"{fr.relpath}:{r.line}")
    return ids


def print_summary(summary: dict, verbose: bool) -> None:
    total = summary["total"]
    executed = total["pass"] + total["fail"]
    print()
    print(f"{'Category':<20} {'pass':>6} {'fail':>6} {'skip':>6} {'files':>6}")
    for cat, counts in sorted(summary["by_category"].items()):
        print(f"{cat:<20} {counts['pass']:>6} {counts['fail']:>6} {counts['skip']:>6} {counts['files']:>6}")
    print("-" * 52)
    print(
        f"{'TOTAL':<20} {total['pass']:>6} {total['fail']:>6} {total['skip']:>6}"
        f" {sum(c['files'] for c in summary['by_category'].values()):>6}"
    )
    print(f"\nexecuted (pass+fail): {executed}   pass rate of executed: "
          f"{(100 * total['pass'] / executed if executed else 0):.1f}%")

    print("\nTop skip reasons:")
    for reason, count in sorted(summary["skip_reasons"].items(), key=lambda kv: -kv[1])[:15]:
        print(f"  {count:>5}  {reason}")

    if summary["files_with_error"]:
        print(f"\n{len(summary['files_with_error'])} file(s) errored at the runner level (not a normal skip/fail):")
        for relpath, err in summary["files_with_error"][:10]:
            print(f"  {relpath}: {err}")

    if verbose and summary["fail_details"]:
        print(f"\n{len(summary['fail_details'])} failing record(s):")
        for relpath, line, reason in summary["fail_details"][:50]:
            print(f"  {relpath}:{line}: {reason}")
        if len(summary["fail_details"]) > 50:
            print(f"  ... and {len(summary['fail_details']) - 50} more")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--source-dir",
        type=Path,
        default=Path(os.environ.get("VGI_SQLLOGICTEST_SOURCE", str(Path.home() / "Development" / "vgi" / "test" / "sql" / "integration"))),
        help="Path to ~/Development/vgi's test/sql/integration directory",
    )
    ap.add_argument(
        "--extension",
        type=Path,
        default=Path(os.environ.get("VGI_SQLITE_EXTENSION", str(REPO_ROOT / "build" / "vgi.so"))),
        help="Path to the built vgi-sqlite extension (.so/.dylib)",
    )
    ap.add_argument("--category", help="Restrict to one top-level category directory")
    ap.add_argument("-k", "--filter", dest="pattern", help="Only run files whose relative path contains this substring")
    ap.add_argument(
        "--jobs",
        type=int,
        default=os.cpu_count() or 4,
        help="Parallel test files (default: cpu count). Safe to parallelize: "
        "every file runs in its own subprocess (see run_file_isolated) "
        "specifically because a real run crashed the whole process outright "
        "(SIGABRT/core dump) partway through a full-corpus run - even "
        "single-threaded, so it wasn't a threading bug - before this "
        "isolation existed. Not root-caused (out of scope for this "
        "framework's first pass - see README.md and the plan file); "
        "isolation means it no longer matters which specific file triggers "
        "it, since that file's own subprocess is the only thing that pays "
        "for it.",
    )
    ap.add_argument("--timeout", type=float, default=120.0, help="Per-file timeout in seconds")
    ap.add_argument(
        "--single-file",
        type=Path,
        help=argparse.SUPPRESS,  # internal: re-invoked as a subprocess by run_file_isolated, not a user-facing mode
    )
    ap.add_argument("-v", "--verbose", action="store_true", help="Print every failing record")
    ap.add_argument("--json-out", type=Path, help="Write full results as JSON to this path")
    ap.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE, help="Baseline file for regression tracking")
    ap.add_argument("--update-baseline", action="store_true", help="Overwrite the baseline with this run's results")
    ap.add_argument("--min-executed", type=int, default=0, help="Fail if pass+fail count drops below this")
    ap.add_argument(
        "--allow-skip",
        action="append",
        default=[],
        help="An expected skip reason (repeatable). If given at least once, any OTHER skip reason fails the run "
        "- mirrors ~/Development/vgi/Makefile's VGI_EXPECTED_SKIPS gate.",
    )
    ap.add_argument("--allow-regression", action="store_true", help="Don't fail on a previously-passing record now failing/skipping")
    ap.add_argument(
        "--no-persistent-workers",
        action="store_true",
        help="Spawn a fresh worker subprocess per ATTACH instead of one long-lived worker per fixture reused over "
        "unix:// (see workers.py's PersistentWorkers) - much slower; mainly useful when debugging the persistent-"
        "worker mechanism itself.",
    )
    ap.add_argument(
        "--worker-pool-size",
        type=int,
        default=4,
        help="Persistent worker processes PER fixture (default: 4), not just one - each vgi-python fixture worker "
        "is a single CPython process; --unix/--threaded gives it a thread per connection, but CPU-bound request "
        "handling still serializes on the GIL across those threads, so one worker process doesn't scale with "
        "--jobs the way a real multi-process pool does (measured directly - see PersistentWorkers.start_all's "
        "docstring). Files are assigned to a pool member deterministically by relpath.",
    )
    args = ap.parse_args()

    if not args.source_dir.is_dir():
        print(f"error: source dir not found: {args.source_dir}", file=sys.stderr)
        print("(pass --source-dir, or set VGI_SQLLOGICTEST_SOURCE)", file=sys.stderr)
        return 2
    if not args.extension.exists():
        print(f"error: extension not found: {args.extension}", file=sys.stderr)
        print("(build it first, pass --extension, or set VGI_SQLITE_EXTENSION)", file=sys.stderr)
        return 2

    if args.single_file:
        # Internal mode: run exactly one file in THIS process and print its
        # result as JSON on stdout - run_file_isolated (the normal, external
        # entry point) invokes this in a subprocess per file. Not meant to
        # be used directly (suppressed from --help).
        env = workers.resolve_env()
        fr = run_file(args.single_file, args.source_dir, args.extension, env)
        print(json.dumps(_file_result_to_dict(fr)))
        return 0

    files = discover_files(args.source_dir, args.category, args.pattern)
    if not files:
        print("error: no test files matched", file=sys.stderr)
        return 2

    print(f"Running {len(files)} sqllogictest file(s) from {args.source_dir} on {platform.system()} "
          f"with {args.jobs} worker(s)...")

    worker_pools: dict[str, list[str]] = {}
    persistent = None
    if not args.no_persistent_workers:
        persistent = workers.PersistentWorkers()
        worker_pools = persistent.start_all(workers.resolve_env(), pool_size=args.worker_pool_size)
        if worker_pools:
            summary_line = ", ".join(f"{var} x{len(locs)}" for var, locs in sorted(worker_pools.items()))
            print(f"Persistent worker pools up over unix://: {summary_line}", file=sys.stderr)

    try:
        file_results: list[FileResult] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
            futures = {
                pool.submit(
                    run_file_isolated, f, args.source_dir, args.extension, args.timeout, worker_pools
                ): f
                for f in files
            }
            done = 0
            for fut in concurrent.futures.as_completed(futures):
                file_results.append(fut.result())
                done += 1
                if done % 25 == 0 or done == len(files):
                    print(f"  ... {done}/{len(files)} files done", file=sys.stderr)
    finally:
        if persistent is not None:
            persistent.stop_all()

    file_results.sort(key=lambda fr: fr.relpath)
    summary = summarize(file_results)
    print_summary(summary, args.verbose)

    current_ids = passing_ids(file_results)
    exit_code = 0

    baseline = None
    if args.baseline.exists():
        baseline = json.loads(args.baseline.read_text())

    if baseline and not args.allow_regression:
        regressed = sorted(set(baseline.get("passing_ids", [])) - current_ids)
        if regressed:
            exit_code = 1
            print(f"\nREGRESSION: {len(regressed)} previously-passing record(s) no longer pass:")
            for rid in regressed[:30]:
                print(f"  {rid}")
            if len(regressed) > 30:
                print(f"  ... and {len(regressed) - 30} more")

    executed = summary["total"]["pass"] + summary["total"]["fail"]
    if args.min_executed and executed < args.min_executed:
        exit_code = 1
        print(f"\nFAIL: only {executed} record(s) executed, expected at least {args.min_executed} "
              "(a worker likely went missing and everything silently skipped instead)")

    if args.allow_skip:
        unexpected = sorted(r for r in summary["skip_reasons"] if r not in args.allow_skip)
        if unexpected:
            exit_code = 1
            print(f"\nFAIL: {len(unexpected)} unexpected skip reason(s) not in --allow-skip:")
            for r in unexpected:
                print(f"  {r}  ({summary['skip_reasons'][r]} record(s))")

    if args.json_out:
        args.json_out.write_text(json.dumps(
            {
                "summary": summary,
                "files": [
                    {
                        "relpath": fr.relpath,
                        "category": fr.category,
                        "file_skip_reason": fr.file_skip_reason,
                        "error": fr.error,
                        "records": [{"status": r.status, "line": r.line, "reason": r.reason} for r in fr.records],
                    }
                    for fr in file_results
                ],
            },
            indent=2,
        ))
        print(f"\nWrote full results to {args.json_out}")

    if args.update_baseline:
        args.baseline.write_text(json.dumps(
            {
                "generated_by": "test/sqllogictest/run_sqllogictest.py",
                "total": summary["total"],
                "passing_ids": sorted(current_ids),
                "skip_reasons": sorted(summary["skip_reasons"]),
            },
            indent=2,
        ) + "\n")
        print(f"\nUpdated baseline at {args.baseline} ({len(current_ids)} passing records)")
        return 0

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
