# © Copyright 2026 Query Farm LLC - https://query.farm
"""Parser for the (small) subset of the sqllogictest format actually used by
~/Development/vgi's test/sql/integration/*.test corpus.

Not a general sqllogictest implementation - deliberately scoped to what that
corpus actually uses (confirmed by grepping the whole corpus before writing
this): `statement ok`/`statement error`, `query <types> [sortmode] [label]`,
`require`/`require-env`, `mode skip`, and `set <name> [args]`. It does NOT
implement `loop`/`foreach`/`endloop`/`skipif`/`onlyif`/`connection`/`restart`/
`hash-threshold` - none of those directives appear anywhere in the corpus
(verified by grep), so supporting them would be speculative complexity with
no test to exercise it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Statement:
    kind: str  # "statement_ok" | "statement_error"
    sql: str
    expected_error: str | None
    line: int


@dataclass
class Query:
    type_string: str
    sort_mode: str  # "nosort" | "rowsort" | "valuesort"
    label: str | None
    sql: str
    expected_rows: list[list[str]]
    line: int
    kind: str = "query"


@dataclass
class Directive:
    kind: str  # "require" | "require_env" | "mode_skip" | "set" | "halt"
    value: str
    line: int


Record = Statement | Query | Directive


@dataclass
class TestFile:
    path: Path
    name: str = ""
    description: str = ""
    group: str = ""
    records: list[Record] = field(default_factory=list)


class ParseError(Exception):
    pass


def _split_row(line: str, num_columns: int) -> list[str]:
    """One expected-result row -> its column tokens. The corpus tab-separates
    multi-column rows (confirmed by inspecting raw bytes) - split on tabs
    first so an empty-string column round-trips correctly; only fall back to
    whitespace-splitting (which collapses runs, losing that fidelity) if the
    tab-split didn't yield the expected column count."""
    if num_columns <= 1:
        return [line]
    parts = line.split("\t")
    if len(parts) == num_columns:
        return parts
    return line.split()


def parse_file(path: Path) -> TestFile:
    text = path.read_text()
    lines = text.split("\n")
    tf = TestFile(path=path)
    i = 0
    n = len(lines)

    def peek() -> str | None:
        return lines[i] if i < n else None

    while i < n:
        raw = lines[i]
        line = raw.rstrip("\r")
        stripped = line.strip()

        if stripped == "":
            i += 1
            continue

        if stripped.startswith("#"):
            if stripped.startswith("# name:"):
                tf.name = stripped[len("# name:") :].strip()
            elif stripped.startswith("# description:"):
                tf.description = stripped[len("# description:") :].strip()
            elif stripped.startswith("# group:"):
                tf.group = stripped[len("# group:") :].strip()
            i += 1
            continue

        parts = stripped.split(None, 1)
        keyword = parts[0]
        rest = parts[1] if len(parts) > 1 else ""

        if keyword == "require-env":
            tf.records.append(Directive("require_env", rest.strip(), i + 1))
            i += 1
            continue

        if keyword == "require":
            tf.records.append(Directive("require", rest.strip(), i + 1))
            i += 1
            continue

        if keyword == "mode":
            if rest.strip() == "skip":
                tf.records.append(Directive("mode_skip", "", i + 1))
            i += 1
            continue

        if keyword == "set":
            tf.records.append(Directive("set", rest.strip(), i + 1))
            i += 1
            continue

        if keyword == "halt":
            tf.records.append(Directive("halt", "", i + 1))
            i += 1
            continue

        if keyword == "statement":
            start_line = i + 1
            status = rest.strip()  # "ok" or "error"
            i += 1
            sql_lines: list[str] = []
            while i < n and lines[i].strip() != "" and lines[i].strip() != "----":
                if not lines[i].lstrip().startswith("#"):
                    sql_lines.append(lines[i])
                i += 1
            expected_error = None
            if i < n and lines[i].strip() == "----":
                i += 1
                err_lines: list[str] = []
                while i < n and lines[i].strip() != "":
                    err_lines.append(lines[i])
                    i += 1
                expected_error = "\n".join(err_lines).strip() or None
            # Consume the trailing blank line separating records.
            if i < n and lines[i].strip() == "":
                i += 1
            kind = "statement_ok" if status == "ok" else "statement_error"
            tf.records.append(Statement(kind, "\n".join(sql_lines).strip(), expected_error, start_line))
            continue

        if keyword == "query":
            start_line = i + 1
            head_parts = rest.split()
            type_string = head_parts[0] if head_parts else ""
            sort_mode = head_parts[1] if len(head_parts) > 1 else "nosort"
            label = head_parts[2] if len(head_parts) > 2 else None
            i += 1
            sql_lines = []
            while i < n and lines[i].strip() != "----":
                if lines[i].strip() == "":
                    # A blank line before ---- would mean this "query" has no
                    # result section at all - not seen in the corpus, but
                    # don't loop forever if it happens.
                    break
                if not lines[i].lstrip().startswith("#"):
                    sql_lines.append(lines[i])
                i += 1
            expected_rows: list[list[str]] = []
            if i < n and lines[i].strip() == "----":
                i += 1
                num_cols = max(len(type_string), 1)
                while i < n and lines[i].strip() != "":
                    expected_rows.append(_split_row(lines[i].rstrip("\r"), num_cols))
                    i += 1
            if i < n and lines[i].strip() == "":
                i += 1
            tf.records.append(
                Query(
                    type_string=type_string,
                    sort_mode=sort_mode,
                    label=label,
                    sql="\n".join(sql_lines).strip(),
                    expected_rows=expected_rows,
                    line=start_line,
                )
            )
            continue

        # Unrecognized directive - shouldn't happen given the corpus survey
        # this parser was scoped against, but skip forward one line rather
        # than looping forever if the corpus grows a new directive.
        i += 1

    return tf
