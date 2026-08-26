# © Copyright 2026 Query Farm LLC - https://query.farm
"""Best-effort DuckDB-SQL -> vgi-sqlite-SQL translation for the vgi
sqllogictest corpus.

This is deliberately NOT a general DuckDB->SQLite SQL translator - it only
implements the small, mechanical rewrites this specific corpus actually
needs (confirmed by reading representative files from every category before
writing this - see run_sqllogictest.py's module docstring and the plan
file's Milestone 6 status for what was surveyed). Anything it can't handle
raises Untranslatable with a short, stable reason string - the runner turns
that into a per-record/per-file SKIP, not a crash, so extending coverage
later is just adding a rule here and re-running, never fixing a crash.

Rules implemented, in the order they matter:
1. `${VAR}` env-var interpolation (the corpus's own convention for
   worker locations/paths).
2. `ATTACH '<name>' AS <alias> (TYPE vgi, LOCATION '<loc>' [, bearer_token
   '<tok>'])` -> `SELECT vgi_attach('<loc>', '<alias>'[, '<tok>'])`. Any
   OTHER ATTACH option (this corpus's attach_options_echo.test family) has
   no vgi-sqlite equivalent at all (vgi_attach() takes only location/
   catalog/bearer_token) - raises Untranslatable rather than silently
   dropping the option, which would make a passing test meaningless.
3. `DETACH <alias>;` -> a harmless no-op (`SELECT 1;`). vgi-sqlite has no
   detach/cleanup mechanism (see connection_pool.h) and this runner gives
   every test file its own fresh sqlite3 connection anyway, so there's
   nothing for DETACH to actually do.
4. `<alias>.<function>(` -> `<alias>_<function>(` for every catalog alias
   attached so far in this file (vgi-sqlite's flat `<catalog>_<name>`
   scalar/aggregate function namespace - see extension.cpp's vgi_attach()).
5. `<alias>.<schema>.<table>` (not followed by `(`, i.e. not rule 4's
   function-call form) -> `"<schema>.<table>"` (vgi-sqlite's own
   double-quoted vtab identifier convention - see extension.cpp's
   vgi_attach()). A bare 2-part `<alias>.<table>` (DuckDB's implicit-`main`-
   schema form) is NOT rewritten - not observed as a real need in the files
   this was built against, and guessing which schema "main" or no-schema
   means for a given table would be more likely wrong than useful.
6. `<expr>::<TYPE>` -> `CAST(<expr> AS <TYPE>)` (SQLite has no `::` cast
   operator at all). Implemented as a small hand-written scanner, not a
   single regex - the preceding operand can be a parenthesized expression,
   a quoted string, or a dotted identifier/number run, and `::` binds only
   to that immediate operand (the same postfix precedence Postgres/DuckDB
   give it), not to a looser surrounding expression.
7. `SET <name> ...;` -> a no-op. SQLite has no SET statement AT ALL (every
   occurrence in this corpus would otherwise be a hard syntax error) and
   every SET in this corpus is test-harness configuration (threads, VGI's
   DuckDB-extension-side result-cache tuning, etc.), never an assertion in
   its own right - the assertion is always a later statement/query.
8. `CALL {enable_logging,disable_logging,truncate_duckdb_logs}(...)` -> a
   no-op - DuckDB log-instrumentation calls with no vgi-sqlite equivalent
   and, like SET, never an assertion in themselves.
9. `(VALUES (...), ...) [AS] alias(col1, col2, ...)` -> `(SELECT column1 AS
   col1, column2 AS col2, ... FROM (VALUES (...), ...)) AS alias`. Confirmed
   by testing directly against the built extension: SQLite's VALUES clause
   works standalone (default column names `column1`, `column2`, ...) but
   SQLite has NO table-valued column-renaming alias syntax at all (`SELECT
   * FROM (SELECT 1 AS x) AS t(a)` is a syntax error, independent of VALUES)
   - confirmed a genuine, permanent SQLite dialect gap, not a build
   configuration issue. This corpus's overwhelmingly common form of
   deriving an inline table (`(VALUES (1,2),(3,4)) t(a,b)`, ~90 occurrences)
   is common and mechanical enough to be worth a dedicated rewrite; the
   general `(<any subquery>) AS t(cols)` form is not attempted (would need
   inspecting the subquery's own column list to rename positionally, a
   bigger and much rarer need in this corpus).
10. A DuckDB STRUCT/LIST literal (`{...}`, `[...]::sometype`, `::STRUCT(...)`,
    `::LIST`) -> Untranslatable. SQLite has no analog of either type at all
    (see docs/type-mapping.md's struct/list -> JSON-text mapping - that's
    this driver's *scan-result* representation, not something a test can
    construct as a literal in a SQLite SQL expression), so there is no
    rewrite to attempt - this is a structural detector, not a translation.

Everything else passes through unchanged - ordinary SELECT/WHERE/ORDER BY/
GROUP BY/VALUES is standard SQL both dialects share.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


class Untranslatable(Exception):
    """Raised with a short, stable reason - becomes a SKIP reason string."""


@dataclass
class TranslationContext:
    """Threads state across the records of one test file - which catalog
    aliases have been ATTACHed so far, in the order later SQL is allowed to
    reference them."""

    env: dict[str, str]
    aliases: set[str] = field(default_factory=set)


_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def interpolate_env(sql: str, env: dict[str, str]) -> str:
    def repl(m: re.Match[str]) -> str:
        name = m.group(1)
        if name not in env:
            raise Untranslatable(f"require-env {name}")
        return env[name]

    return _VAR_RE.sub(repl, sql)


_ATTACH_RE = re.compile(
    r"^\s*ATTACH\s+'[^']*'\s+AS\s+(?P<alias>[A-Za-z_]\w*)\s*\(\s*TYPE\s+vgi\s*,"
    r"\s*LOCATION\s+'(?P<location>[^']*)'(?P<rest>.*?)\)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_BEARER_TOKEN_RE = re.compile(r",?\s*bearer_token\s+'(?P<token>[^']*)'\s*", re.IGNORECASE)
_DETACH_RE = re.compile(r"^\s*DETACH\s+[A-Za-z_]\w*\s*;?\s*$", re.IGNORECASE)


def _sql_quote(s: str) -> str:
    """A real SQL string-literal quoter - `repr()` is NOT safe here: Python
    switches to double-quotes for a string containing an embedded `'`
    (`repr("it's")` -> `"it's"`), which is syntactically valid Python but
    NOT a SQL string literal - SQLite parses a double-quoted token as an
    identifier, not a string, so a location/alias/token containing an
    apostrophe would silently become a different (and almost certainly
    invalid) statement instead of erroring loudly. Found by inspection
    while writing _translate_attach, not by a real failure."""
    return "'" + s.replace("'", "''") + "'"


def _translate_attach(sql: str, ctx: TranslationContext) -> str:
    m = _ATTACH_RE.match(sql)
    if not m:
        raise Untranslatable("unrecognized ATTACH syntax")
    alias = m.group("alias")
    location = m.group("location")
    rest = m.group("rest")

    bearer_token = None
    bm = _BEARER_TOKEN_RE.search(rest)
    if bm:
        bearer_token = bm.group("token")
        rest = rest[: bm.start()] + rest[bm.end() :]
    # Anything left over (beyond commas/whitespace) is an ATTACH option
    # vgi_attach() has no way to express at all.
    if re.sub(r"[,\s]", "", rest):
        raise Untranslatable("unsupported ATTACH option")

    ctx.aliases.add(alias)
    if bearer_token is not None:
        return f"SELECT vgi_attach({_sql_quote(location)}, {_sql_quote(alias)}, {_sql_quote(bearer_token)})"
    return f"SELECT vgi_attach({_sql_quote(location)}, {_sql_quote(alias)})"


def _rewrite_catalog_refs(sql: str, aliases: set[str]) -> str:
    for alias in sorted(aliases, key=len, reverse=True):
        esc = re.escape(alias)
        sql = re.sub(rf"\b{esc}\.([A-Za-z_]\w*)\s*\(", rf"{alias}_\1(", sql)
        sql = re.sub(rf'\b{esc}\.([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b(?!\s*\()', r'"\1.\2"', sql)
    return sql


_VALUES_START_RE = re.compile(r"\(\s*VALUES\b", re.IGNORECASE)
_VALUES_ALIAS_TAIL_RE = re.compile(
    r"\s*(?:AS\s+)?(?P<alias>[A-Za-z_]\w*)\((?P<cols>[A-Za-z_]\w*(?:\s*,\s*[A-Za-z_]\w*)*)\)"
)


def _matching_paren(sql: str, open_idx: int) -> int:
    """Index of the `)` matching the `(` at open_idx, by depth-tracking -
    needed instead of a regex because a VALUES row can itself contain
    parenthesized expressions (`VALUES (CAST(1 AS INTEGER))`, seen in this
    corpus)."""
    depth = 0
    k = open_idx
    while k < len(sql):
        if sql[k] == "(":
            depth += 1
        elif sql[k] == ")":
            depth -= 1
            if depth == 0:
                return k
        k += 1
    raise Untranslatable("unbalanced parens in VALUES")


def _rewrite_values_alias(sql: str) -> str:
    out = []
    i = 0
    while True:
        m = _VALUES_START_RE.search(sql, i)
        if not m:
            out.append(sql[i:])
            break
        open_idx = m.start()
        close_idx = _matching_paren(sql, open_idx)
        tail_m = _VALUES_ALIAS_TAIL_RE.match(sql, close_idx + 1)
        if not tail_m:
            # `(VALUES ...)` with no column-renaming alias - already valid
            # SQLite as-is, nothing to rewrite here.
            out.append(sql[i : close_idx + 1])
            i = close_idx + 1
            continue
        alias = tail_m.group("alias")
        cols = [c.strip() for c in tail_m.group("cols").split(",")]
        values_sql = sql[open_idx : close_idx + 1]
        select_list = ", ".join(f"column{n} AS {c}" for n, c in enumerate(cols, start=1))
        out.append(sql[i:open_idx])
        out.append(f"(SELECT {select_list} FROM {values_sql}) AS {alias}")
        i = tail_m.end()
    return "".join(out)


_STRUCT_LITERAL_RE = re.compile(r"\{\s*'?[A-Za-z_]\w*'?\s*:")
_STRUCT_OR_LIST_CAST_RE = re.compile(r"::\s*(STRUCT|LIST)\b", re.IGNORECASE)
_LIST_LITERAL_CAST_RE = re.compile(r"\]\s*::")


def _check_no_struct_or_list(sql: str) -> None:
    if _STRUCT_LITERAL_RE.search(sql) or _STRUCT_OR_LIST_CAST_RE.search(sql) or _LIST_LITERAL_CAST_RE.search(sql):
        raise Untranslatable("struct/list literal (no vgi-sqlite equivalent)")


def _translate_casts(sql: str) -> str:
    out: list[str] = []
    i = 0
    n = len(sql)
    while True:
        idx = sql.find("::", i)
        if idx == -1:
            out.append(sql[i:])
            break
        # Determine the operand's start by scanning backward from idx.
        if idx > 0 and sql[idx - 1] == ")":
            depth = 0
            k = idx - 1
            while k >= 0:
                if sql[k] == ")":
                    depth += 1
                elif sql[k] == "(":
                    depth -= 1
                    if depth == 0:
                        break
                k -= 1
            if k < 0:
                raise Untranslatable("unbalanced parens before ::CAST")
            start = k
        elif idx > 0 and sql[idx - 1] == "'":
            k = idx - 2
            while k >= 0 and sql[k] != "'":
                k -= 1
            if k < 0:
                raise Untranslatable("unterminated string before ::CAST")
            start = k
        else:
            k = idx - 1
            while k >= 0 and (sql[k].isalnum() or sql[k] in "_."):
                k -= 1
            start = k + 1
        operand = sql[start:idx]

        # Parse the type name after '::': an identifier, optionally
        # followed by (args) (DECIMAL(18,4)) and/or a trailing [] (list
        # types - kept verbatim; SQLite has no such type, so a cast like
        # this will simply fail at execution time rather than translation
        # time, which is fine - it surfaces as an honest per-test failure).
        m = idx + 2
        t0 = m
        while m < n and (sql[m].isalnum() or sql[m] == "_"):
            m += 1
        type_name = sql[t0:m]
        if not type_name:
            raise Untranslatable(":: not followed by a type name")
        if m < n and sql[m] == "(":
            depth = 0
            t_start = m
            while m < n:
                if sql[m] == "(":
                    depth += 1
                elif sql[m] == ")":
                    depth -= 1
                    if depth == 0:
                        m += 1
                        break
                m += 1
            type_name += sql[t_start:m]
        while m + 1 < n and sql[m] == "[" and sql[m + 1] == "]":
            type_name += "[]"
            m += 2

        out.append(sql[i:start])
        out.append(f"CAST({operand} AS {type_name})")
        i = m
    return "".join(out)


_SET_RE = re.compile(r"^\s*SET\s+\S", re.IGNORECASE)
_CALL_NOOP_RE = re.compile(
    r"^\s*CALL\s+(enable_logging|disable_logging|truncate_duckdb_logs)\s*\(.*\)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def translate_sql(sql: str, ctx: TranslationContext) -> str:
    """Translate one statement/query body. Raises Untranslatable (with a
    short reason) if it uses something this translator doesn't handle."""
    sql = interpolate_env(sql, ctx.env)

    if re.match(r"^\s*ATTACH\b", sql, re.IGNORECASE):
        return _translate_attach(sql, ctx)
    if _DETACH_RE.match(sql):
        return "SELECT 1"
    if _SET_RE.match(sql):
        return "SELECT 1"
    if _CALL_NOOP_RE.match(sql):
        return "SELECT 1"

    # A DuckDB catalog-qualified *table function call* (`FROM alias.foo(...)`)
    # has no vgi-sqlite equivalent - this driver's vtabs take fixed module
    # arguments at CREATE VIRTUAL TABLE time, never per-query SQL arguments -
    # so surface that clearly rather than mistranslating it as a scalar
    # function call (rule 4 above would otherwise mangle it: `alias.foo(`
    # looks identical to a scalar-function call syntactically).
    for alias in ctx.aliases:
        if re.search(rf"\bFROM\s+{re.escape(alias)}\.[A-Za-z_]\w*\s*\(", sql, re.IGNORECASE):
            raise Untranslatable("table-function call (no vgi-sqlite equivalent)")

    _check_no_struct_or_list(sql)

    sql = _rewrite_catalog_refs(sql, ctx.aliases)
    sql = _rewrite_values_alias(sql)
    sql = _translate_casts(sql)
    return sql
