# vgi-sqlite sqllogictest runner

Runs `~/Development/vgi`'s (the DuckDB C++ extension) 327-file sqllogictest
corpus under `test/sql/integration/` against the built `vgi-sqlite`
extension, translating what's mechanically translatable from DuckDB SQL to
SQLite/vgi-sqlite SQL, and reporting pass/fail/skip **per individual
statement/query record** (not just per file).

## Why this exists

`test/integration/` (this repo's pytest suite) is - per `CLAUDE.md`'s
"Definition of done" - the actual test source of record for vgi-sqlite.
That hasn't changed. `~/Development/vgi`'s sqllogictest corpus is a
*DuckDB-dialect* suite (`ATTACH (TYPE vgi, ...)`, `::CAST`, STRUCT/LIST
types, `range()`/`generate_series()`, catalog-qualified table-function
calls) that was never directly portable - confirmed by reading
representative files from every category (see this file's "What's
structurally out of scope" section, and the plan file's Milestone 6 status
for the full category-by-category survey).

But it's also the single largest, most battle-tested collection of VGI
protocol/behavior assertions that exists anywhere - years of real bugs
found against real workers, across every VGI client SDK. Hand-porting each
file's assertions into `test/integration/` one at a time is real, valuable,
ongoing work. This tool exists to make that work **incremental and
measurable** instead of all-or-nothing: run it, see what already passes for
free, see exactly *why* everything else doesn't (a stable, groupable reason
string per skip/fail), and treat closing the largest reason category as the
next slice of work - a CI-trackable number that should only ever go up.

This tool does **not** replace `test/integration/`, and passing here isn't
"done" for a category - it's a signal for where to look next. A record that
passes here is a strong candidate to also get a *real*, hand-written,
purpose-built pytest test in `test/integration/` (which can assert more
precisely, e.g. real regression coverage the way `test_pushdown.py` asserts
on the *fact of* pushdown, not just query correctness) - this tool finding
it "passes" is a lead, not a substitute for that.

## Current status

As of Milestone 7's combined verification (splits support + the two bugs
this framework found - see the plan file's Milestone 6/7 status for the
full writeup): **615 passing records** out of 1911 executed (pass+fail;
32.2%), 1435 skipped (mostly real, correct skips - see `skip_reasons` in a
`--json-out` run), across all 327 files, zero of which crash the process.
Runs in **~16 seconds** on a 16-core box with pooled persistent workers
(see below; ~48s with one persistent worker per fixture instead of a pool,
several minutes with `--no-persistent-workers`) - `baseline.json` (checked
in) pins the exact passing set for regression tracking.

## How it works

- **`parser.py`** - parses the small subset of the sqllogictest format the
  corpus actually uses (`statement ok`/`error`, `query <types> [sortmode]
  [label]`, `require`/`require-env`, `mode skip`, `set`). Confirmed by
  grepping the whole corpus before writing this: no file uses
  `loop`/`foreach`/`skipif`/`onlyif`/`connection`/`restart` at all, so none
  of those are implemented.
- **`translate.py`** - the DuckDB->vgi-sqlite SQL rewriter. Raises
  `Untranslatable("short reason")` for anything it can't handle - that
  becomes a SKIP, never a crash. See its module docstring for the exact
  rule list (ATTACH/DETACH, catalog-qualified refs, `::CAST`, `SET`/`CALL`
  no-ops, the `(VALUES ...) alias(cols)` rewrite, STRUCT/LIST detection).
  **This is the file to edit to grow the passing count** - see "How to grow
  coverage" below.
- **`comparison.py`** - expected-vs-actual result comparison, approximating
  (not byte-for-byte replicating) DuckDB's sqllogictest runner conventions:
  boolean `true`/`false` normalized against vgi-sqlite's real INTEGER 0/1
  representation, `R`-type floats compared after rounding both sides to 6
  significant digits (not true numeric tolerance - good enough for this
  corpus's actual values, not chasing float-equality edge cases no test
  here needs), `nosort`/`rowsort`/`valuesort` all implemented.
- **`workers.py`** - resolves `require-env`/`${VAR}` names to real values,
  mirroring `~/Development/vgi/Makefile`'s own worker-command mapping (the
  authoritative source, not a fresh guess) for the fixture workers this
  environment can actually stand up (`VGI_TEST_WORKER`,
  `VGI_SIMPLE_WRITABLE_WORKER`, `VGI_ATTACH_OPTIONS_WORKER`,
  `VGI_VERSIONED_WORKER`, `VGI_VERSIONED_TABLES_WORKER`,
  `VGI_BAD_PROTOCOL_WORKER`, `VGI_BAD_ENUM_WORKER`, `VGI_TEST_BRANCH_DIR`,
  `VGI_SCHEMA_RECONCILE_DB`). Anything else - docker/github-network/
  iceberg/companion/launcher/rules workers, HTTP-transport-variant lanes -
  is deliberately never resolved; a file needing one of those is a real,
  correctly-reported SKIP, mirroring how `vgi`'s own Makefile treats the
  exact same set in its `VGI_EXPECTED_SKIPS` list for its most permissive
  CI lane.
- **`run_sqllogictest.py`** - the orchestrator: discovers `*.test` files,
  applies `STRUCTURAL_SKIP_CATEGORIES` (whole top-level directories with no
  vgi-sqlite equivalent at all, regardless of worker availability - see
  below), gives each remaining file its own fresh sqlite3 connection
  (temp-file-backed, matching `test/integration/conftest.py`'s own `conn`
  fixture - not `:memory:`, since `CREATE VIRTUAL TABLE`'s module arguments
  need to round-trip through `sqlite_master`), runs every record in order,
  and reports.

## What's structurally out of scope (`STRUCTURAL_SKIP_CATEGORIES`)

Whole top-level corpus directories skipped before even parsing, because
they test SQL surface with no vgi-sqlite equivalent AT ALL, independent of
which fixture worker is available:

| directory | why |
|---|---|
| `table_in_out/` | table-in-out functions - explicit, already-documented gap (see the plan file) |
| `secret/` | DuckDB's secrets manager - N/A |
| `launcher/` | AF_UNIX launcher discovery protocol - not implemented |
| `cache/` | DuckDB-extension-side VGI result cache - vgi-sqlite has no query cache layer at all |
| `copy_to/`, `copy_from/` | SQL `COPY` has no vgi-sqlite/SQLite equivalent |
| `macro/` | SQL `CREATE MACRO` has no vgi-sqlite/SQLite equivalent |
| `overload/` | catalog-qualified *table-function-call* syntax (`FROM alias.fn(args)`) - this driver's vtabs take fixed module arguments at `CREATE VIRTUAL TABLE` time, never per-query SQL arguments |
| `global_functions/` | `vgi_global_functions()` introspection - N/A |
| `view/` | asserts via `duckdb_views()` introspection - N/A |
| `settings/` | table-in-out function calls with SQL arguments - see `table_in_out/` |

Every one of these was actually read (representative files, not assumed)
before being added to this list - see the plan file's Milestone 6 status
for what each category's files actually assert.

**`splits/` is no longer on this list** - vgi-sqlite implements sequential
split redemption (`src/catalog/catalog_table_plan.h`; see the plan file's
Milestone 7 status). Most individual `splits/*.test` files still fail,
though - every split-capable fixture in this corpus is exposed only as a
directly-callable table FUNCTION with SQL-visible arguments
(`split_sequence(n := 100, splits := 1)`), which is the SAME
table-function-call gap `overload/`/`settings/` hit above, not a splits-
specific one. Splits itself was verified against those same fixtures via
a standalone probe tool instead (`tools/vgi-split-probe`) - see the plan
file for the exact scenarios checked.

## Running it

```bash
cmake --build ../../build   # build the extension first, same as test/integration/

# Full corpus:
python3 test/sqllogictest/run_sqllogictest.py

# One category, verbose (prints every failing record):
python3 test/sqllogictest/run_sqllogictest.py --category scalar -v

# One file by substring:
python3 test/sqllogictest/run_sqllogictest.py -k add_values.test -v
```

Defaults: `--source-dir ~/Development/vgi/test/sql/integration` (override
with `--source-dir` or `VGI_SQLLOGICTEST_SOURCE`), `--extension
../../build/vgi.{so,dylib}` (override with `--extension` or
`VGI_SQLITE_EXTENSION`, matching `test/integration/conftest.py`'s own
convention).

**Every file runs in its own subprocess** (`run_file_isolated`, re-invoking
this same script with an internal `--single-file` flag) rather than
in-process - not for cosmetic isolation, but because a real full-corpus run
reliably crashed the whole process with `SIGABRT`/a core dump partway
through, **even single-threaded** (`--jobs 1` crashed too, ruling out a
threading bug specifically). **Not root-caused** - out of scope for this
framework's first pass, see the plan file's Milestone 6 status for what was
captured about it (a worker-side `IndexError` during pushdown-filter
application, immediately followed by a bare C++ `what():` line - i.e. an
uncaught C++ exception reaching `std::terminate()`, somewhere in the
extension's own exception handling, not in this Python runner - two exact
reproducer files are named in the plan file). Given that, `--jobs` defaults
to the CPU count again (each file already pays for its own process either
way, so parallelizing is free correctness-wise) - a crash now costs one
`FileResult.error` for that file, never the whole run.

**Persistent worker POOLS, not one spawn per ATTACH**
(`workers.PersistentWorkers`) - the biggest speed win, and the reason a
full-corpus run takes ~16s rather than several minutes.
`VgiConnection::spawn` (the default subprocess transport) launches a
brand-new worker process on every single `ATTACH`; across 327 files where
`VGI_TEST_WORKER` alone is needed by ~94% of `require-env` occurrences
corpus-wide, that's hundreds of `uv run` + Python-interpreter-startup round
trips. Every vgi-python fixture worker binary also supports `--unix PATH`
(bind to a socket instead of stdin/stdout), and vgi-sqlite's
`VgiConnection::Connect` now speaks `unix://PATH` (connects to an
already-running worker instead of spawning one - see
`rpc/vgi_connection.h`'s file comment). `run_sqllogictest.py` starts
`--worker-pool-size` (default 4) instances of each resolvable fixture
worker, bound to their own sockets, and every file's subprocess picks one
pool member deterministically by its own relpath - reusing long-lived
processes over `unix://` instead of spawning its own, the same reuse a
full launcher-protocol deployment would give this driver, just started
explicitly by this test runner rather than on-demand-discovered by it (the
full AF_UNIX auto-spawn-on-demand launcher protocol itself,
`docs/launcher-protocol.md`, remains a separate, not-yet-implemented gap -
see the plan file's "Later phases" section).

**Why a *pool*, not just one persistent worker per fixture**: measured
directly (see `PersistentWorkers.start_all`'s docstring for the exact
numbers) - a single persistent worker is one CPython process, and
`--unix`'s `--threaded` default serves each connection on its own thread
within that *one* process, not one process per connection. Real request
handling (Arrow/IPC (de)serialization, dispatch, ordinary Python glue)
mostly holds the GIL, so N concurrent client connections against one
worker process get interleaving, not N-way parallelism - confirmed by
timing this driver's own `table/` category (59 files): `--jobs 1` against
a single persistent instance took 33.2s wall (29.4s CPU, sequential, as
expected); `--jobs 16` against that *same single instance* only dropped
wall time to 24.2s while CPU time roughly **doubled** (58.7s, thread-
scheduling/GIL-contention overhead) - 16x client-side concurrency bought
~1.4x wall-clock improvement. A pool of 4 real OS processes (`--worker-
pool-size 4`, each with its own GIL) dropped the same category to 13.1s -
real parallelism a single process's threads can't provide. Pass
`--no-persistent-workers` to fall back to spawn-per-connection entirely
(e.g. while debugging the persistent-worker mechanism itself), or
`--worker-pool-size 1` to keep persistence but isolate the pooling effect.

## CI usage: `--min-executed` / `--allow-skip` / baselines

Mirrors `~/Development/vgi/Makefile`'s own `VGI_EXPECTED_SKIPS`/`--allow-skip`
design (a validated pattern already proven on that project, not invented
fresh here) plus a passing-record baseline for regression tracking:

```bash
python3 test/sqllogictest/run_sqllogictest.py \
    --min-executed 400 \
    --allow-skip 'category: table_in_out' \
    --allow-skip 'category: cache' \
    ... \
    --allow-skip 'require-env VGI_DOCKER_IMAGE'
```

- **`--min-executed N`** - fails the run if fewer than N records actually
  executed (pass+fail, excluding skips). A lane that stops running tests
  reports green by default (fewer results, and every one of them passes) -
  this is the floor that catches a worker silently going missing.
- **`--allow-skip REASON`** (repeatable) - the skip reasons this run
  expects. An **unlisted** reason fails the run, so a newly-gated test (or
  a translation regression that suddenly can't translate something it used
  to) can't quietly leave the counted set.
- **`baseline.json`** (checked in) records every currently-passing record's
  id. Every run compares against it and **fails on regression** (a
  previously-passing record now failing or skipping) unless
  `--allow-regression` is passed. Run with `--update-baseline` to
  intentionally accept the current run's results as the new baseline (do
  this in the same commit as any translate.py change that changes the
  passing set, exactly like updating a snapshot test).

  **Record identity is content-based, not `relpath:line`** - an id is
  `{relpath}#{content_key}#{occurrence}`, where `content_key` is a hash of
  the record's own original SQL text (`_content_key` in
  `run_sqllogictest.py`) and `occurrence` disambiguates the rare case of the
  literal same statement appearing more than once in one file. `relpath:line`
  was the original scheme and it broke silently: `~/Development/vgi` (the
  corpus source) is a sibling repo we don't control, and it edits its own
  `.test` files over time - a line-number-only id then attributes a
  record's whole pass/fail history to whatever *different* query now
  happens to sit at that line after such an edit, reporting a false
  "regression" that costs real investigation time to rule out (this
  actually happened - see CLAUDE.md's Milestone 10 status for the
  vgi-go/column-comments red herring it produced). The content-based id
  makes a corpus edit that only moves a query a no-op for regression
  tracking, while a corpus edit that genuinely changes a query's text is
  correctly treated as a brand-new record with no stale baseline entry to
  compare against. `line` is still recorded on every result and used for
  human-readable output (`_describe_id` resolves a regressed id back to its
  current line, or reports plainly that the record's no longer found in the
  file at all) - it's just not part of the matching key anymore. A baseline
  written before this change (no `"id_scheme": "content-v1"` field) is
  detected and the regression comparison is skipped once with a note,
  rather than reporting a mass false regression from every id's format
  simply not matching - run `--update-baseline` once to adopt the new
  scheme.

GitHub Actions wiring is **not** set up yet - `~/Development/vgi` is a
private, secrets-gated sibling repo the same way `vgi-c++`/`vgi-rpc-c++`
are (see the plan file's Milestone 5 status on why CI for *this* repo's own
`test/integration/` suite isn't built either, for the identical reason) -
but this script is written to be CI-ready: point `--source-dir` at a
checked-out `vgi` (via whatever secret/deploy-key mechanism a repo admin
sets up), run with the flags above, done.

## How to grow coverage

1. Run the full corpus (`--json-out results.json` for the full per-record
   detail) and look at the `skip_reasons`/`fail_details` breakdown.
2. Pick the largest reason category that's actually fixable (a genuine
   translation gap, not a structural one from the table above).
3. Add or extend a rule in `translate.py` - each rule is a small, isolated
   function; add a case to `translate_sql`'s dispatch, or extend an
   existing rewrite.
4. Re-run, confirm the pass count went up and nothing regressed.
5. `--update-baseline` in the same commit.
6. For anything that reveals an actual vgi-sqlite **bug** (not a
   translation gap) - like the scalar-function argument-type-locking issue
   this tool's first real run already found (see the plan file's Milestone
   6 status) - file/fix it separately; this tool's job is finding it, not
   fixing driver bugs itself.

## Known approximations (not bugs - accepted scope for a first pass)

- Float (`R`-type) comparison rounds both sides to 6 significant digits
  rather than doing true numeric-tolerance comparison.
- `BLOB` values are formatted as `\xAABBCC` (uppercase hex, no per-byte
  backslash escapes) for comparison purposes - close to but not identical
  to DuckDB's own blob literal rendering.
- The `(VALUES ...) alias(cols)` rewrite only handles VALUES specifically,
  not the general `(<any subquery>) AS alias(cols)` form (would need
  inspecting the subquery's own column list to rename positionally - a
  bigger, much rarer need in this corpus).
- 2-part catalog-qualified table references (DuckDB's implicit-`main`-
  schema form, `alias.table`) aren't rewritten, only the 3-part
  `alias.schema.table` form actually seen in the corpus.
