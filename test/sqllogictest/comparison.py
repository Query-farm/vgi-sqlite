# © Copyright 2026 Query Farm LLC - https://query.farm
"""Expected-vs-actual comparison for `query` records, matching the sqllogictest
convention closely enough to be useful without chasing DuckDB's exact runner
byte-for-byte (see this package's README for what's approximated and why).
"""

from __future__ import annotations

from parser import Query


def _format_float(f: float) -> str:
    """Canonical string for an R-type value on both sides of a comparison -
    not true numeric tolerance, but rounding to 6 significant digits (and
    always showing at least one fractional digit, so `4` and `4.0` compare
    equal) absorbs the formatting differences actually seen in this corpus's
    R-type expectations without chasing float-equality edge cases no test
    here actually needs."""
    if f == int(f) and abs(f) < 1e15:
        return f"{int(f)}.0"
    return f"{f:.6g}"


def canonicalize_expected(token: str, type_char: str) -> str:
    if token == "NULL":
        return "NULL"
    if type_char == "R":
        try:
            return _format_float(float(token))
        except ValueError:
            return token
    if type_char == "I" and token in ("true", "false"):
        # vgi-sqlite maps Arrow bool -> SQLite INTEGER 0/1 (see
        # types/type_mapping.cpp); DuckDB's own boolean literal rendering is
        # "true"/"false" - normalize the expected side to match, rather than
        # the actual side, so a diff on failure still shows the real value.
        return "1" if token == "true" else "0"
    return token


def canonicalize_actual(value: object, type_char: str) -> str:
    if value is None:
        return "NULL"
    if type_char == "R":
        try:
            return _format_float(float(value))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return str(value)
    if isinstance(value, bytes):
        return "\\x" + value.hex().upper()
    return str(value)


def format_actual_row(row: tuple, type_string: str) -> list[str]:
    return [
        canonicalize_actual(v, type_string[i] if i < len(type_string) else "T") for i, v in enumerate(row)
    ]


def rows_match(query: Query, actual_rows: list[list[str]]) -> tuple[bool, str]:
    """actual_rows must already be canonicalized (format_actual_row)."""
    type_string = query.type_string
    expected = [
        [canonicalize_expected(tok, type_string[i] if i < len(type_string) else "T") for i, tok in enumerate(row)]
        for row in query.expected_rows
    ]
    actual = actual_rows

    if query.sort_mode == "valuesort":
        exp_flat = sorted(v for row in expected for v in row)
        act_flat = sorted(v for row in actual for v in row)
        if exp_flat == act_flat:
            return True, ""
        return False, f"valuesort mismatch: expected {exp_flat!r}, got {act_flat!r}"

    if query.sort_mode == "rowsort":
        exp_sorted = sorted("\t".join(r) for r in expected)
        act_sorted = sorted("\t".join(r) for r in actual)
        if exp_sorted == act_sorted:
            return True, ""
        return False, f"rowsort mismatch: expected {exp_sorted!r}, got {act_sorted!r}"

    # nosort (default): exact row-major order and count.
    if expected == actual:
        return True, ""
    if len(expected) != len(actual):
        return False, f"row count mismatch: expected {len(expected)}, got {len(actual)} ({actual!r})"
    for i, (e, a) in enumerate(zip(expected, actual)):
        if e != a:
            return False, f"row {i} mismatch: expected {e!r}, got {a!r}"
    return False, f"expected {expected!r}, got {actual!r}"
