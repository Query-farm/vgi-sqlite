# © Copyright 2026 Query Farm LLC - https://query.farm
"""Standalone worker hosting `RowTransformFunction` ("blended"/table_in_out)
fixtures for vgi-sqlite's Milestone 9 work (per-row-correlated
table-valued-function support).

Not part of vgi-python itself (that repo is the shared, canonical protocol
implementation across every client SDK - not vgi-sqlite's to add
driver-specific test fixtures to). This script only *imports* vgi-python's
SDK as a library, the same way `~/Development/vgi`'s own reference
`examples/calc_worker.py` does for a from-scratch worker - see this
repo's CLAUDE.md/plan file on why vgi-python is upstream, not owned here.

No vgi-fixture-worker fixture exercises `RowTransformFunction` yet
(confirmed by grepping vgi-python's `_test_fixtures/` tree - every
existing table_in_out fixture subclasses `TableInOutFunction`/
`TableInOutGenerator`/`TableBufferingFunction`, the classic relation-
valued-argument shapes SQLite's table-valued-function calling convention
can't represent at all - see src/catalog/table_in_out_caller.h's file
comment), so this driver needed its own probe fixture to test against a
real worker rather than only a synthetic/mocked one - same "probe tool
first, against something real" discipline as tools/split_probe.cpp.

Run directly: `uv run --project ~/Development/vgi-python python
test/integration/fixtures/row_transform_worker.py [--unix PATH]
[--idle-timeout SECS]` (or spawn-mode with no args, matching
vgi-fixture-worker's own convention) - `Worker.main()` handles all of
that CLI surface generically.

Two functions, both registered under the `row_transform` catalog's
default `main` schema:

  - `add_row(x, y)` -> one row, `{sum: x+y}` - the ordinary 1-input-row
    -> 1-output-row case (a per-row-correlated table function computing
    something from its correlated arguments, the "table-valued function"
    version of an ordinary scalar function call).
  - `repeat_row(value, n)` -> `n` rows, each `{value: value}` - the
    1-input-row -> N-output-rows case this driver's design specifically
    needed to confirm was already supported with no protocol-level
    change (see the plan file's Milestone 9 notes on the user's
    SumAllColumnsFunction-prompted question) - also covers `n=0` (zero
    output rows for one input row, a real and legal case per
    `RowTransformFunction`'s own "1->1, 1->N, 1->0 all work" contract).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
import pyarrow.compute as pc

from vgi import Worker
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.table_function import BindParams, ProcessParams
from vgi.table_in_out_function import RowTransformFunction
from vgi_rpc.rpc import OutputCollector

CATALOG_NAME = "row_transform"


@dataclass(frozen=True)
class _AddRowArgs:
    x: Annotated[int, Arg(0, doc="left addend, per row")]
    y: Annotated[int, Arg(1, doc="right addend, per row")]


class AddRowFunction(RowTransformFunction[_AddRowArgs]):
    """`add_row(x, y)` -> one row of `{sum: x + y}` per correlated call.

    The minimal 1-input-row -> 1-output-row blended table_in_out case:
    exercises bind (custom output schema, not the base class's
    input-schema-passthrough default), and one Exchange() call per row.
    """

    class Meta:
        name = "add_row"
        description = "per-row x+y, probing blended table_in_out (RowTransformFunction) support"

    @classmethod
    def on_bind(cls, params: BindParams[_AddRowArgs]) -> BindResponse:
        return BindResponse(output_schema=pa.schema([pa.field("sum", pa.int64())]))

    @classmethod
    def process(
        cls,
        params: ProcessParams[_AddRowArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        total = pc.add(batch.column("x"), batch.column("y"))
        out.emit(pa.RecordBatch.from_arrays([total], schema=params.output_schema))


@dataclass(frozen=True)
class _RepeatRowArgs:
    # Named distinctly from the output column ("repeated") on purpose -
    # an output column and a HIDDEN input-argument column can't share a
    # name (they're both plain columns in the same CREATE TABLE
    # vgi_table_in_out_vtab.cpp declares; SQLite rejects a duplicate
    # column name outright). Found the hard way: an earlier version of
    # this fixture named both "value" and CREATE VIRTUAL TABLE silently
    # failed sqlite3_declare_vtab, which vgi_attach() correctly logged as
    # a per-function skip (not a crash) but was otherwise easy to miss -
    # this fixture exists partly to keep that constraint documented via a
    # real, obviously-would-collide-if-changed-back example.
    value_in: Annotated[int, Arg(0, doc="value to repeat")]
    n: Annotated[int, Arg(1, doc="how many output rows this one input row produces", ge=0)]


class RepeatRowFunction(RowTransformFunction[_RepeatRowArgs]):
    """`repeat_row(value_in, n)` -> `n` rows of `{value: value_in}`.

    The 1-input-row -> N-output-rows (including N=0) blended table_in_out
    case - see the module docstring on why this driver specifically
    needed to confirm this against a real worker, not just reason about
    it from protocol docs.
    """

    class Meta:
        name = "repeat_row"
        description = "repeats `value_in` `n` times - probes 1-row-in -> N-rows-out (incl. N=0)"

    @classmethod
    def on_bind(cls, params: BindParams[_RepeatRowArgs]) -> BindResponse:
        return BindResponse(output_schema=pa.schema([pa.field("value", pa.int64())]))

    @classmethod
    def process(
        cls,
        params: ProcessParams[_RepeatRowArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        value = batch.column("value_in")[0].as_py()
        n = batch.column("n")[0].as_py()
        out.emit(pa.RecordBatch.from_pydict({"value": [value] * n}, schema=params.output_schema))


class RowTransformWorker(Worker):
    # No explicit catalog_interface: Worker auto-builds a default
    # read-only one from `functions` (see Worker's own class docstring) -
    # there's nothing custom to override here, unlike e.g. narrow_bind's
    # worker, which needs table-specific catalog behavior this probe
    # doesn't (this catalog registers no tables at all, only functions).
    catalog_name = CATALOG_NAME
    functions = [AddRowFunction, RepeatRowFunction]


if __name__ == "__main__":
    RowTransformWorker.main()
