"""Round-trip serialization tests for the contract data families.

Serialization is mandatory (every step's I/O is on disk for auditability), so
each contract must survive a write-then-read with full equality.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pyframework_pipeline.contracts.instruction import InstructionDataset, InstructionSample
from pyframework_pipeline.contracts.records import PerfRecord, RawSample
from pyframework_pipeline.contracts.tables import (
    AggregatedTables,
    CategoryRow,
    SharedObjectRow,
    SymbolRow,
)
from pyframework_pipeline.contracts.timing import TimingDataset, TimingEntry


class TestRecordsContract(unittest.TestCase):
    def test_raw_sample_roundtrip(self):
        rec = RawSample(
            platform_id="x86", arch="x86_64", python_version="3.14.0",
            build_id="b1", benchmark="tpch", event="cycles",
            children_share=42.5, period_share=12.5, period=1000000,
            pid=1234, command="python3", pid_command="1234:python3",
            shared_object="/lib/libpython3.14.so", symbol="PyEval_EvalFrameDefault",
            ip=0x400000, source_report="perf", sample_count=1,
        )
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "recs.csv"
            RawSample.to_file(p, [rec])
            got = RawSample.from_file(p)
        self.assertEqual(got, [rec])

    def test_perf_record_carries_categories(self):
        rec = PerfRecord(
            platform_id="x86", shared_object="/lib/libpython3.14.so",
            symbol="PyEval_EvalFrameDefault", period=100, period_share=10.0,
            category_top="Interpreter", category_sub="ceval", category_reason="symbol_regex:PyEval",
        )
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "classified.csv"
            PerfRecord.to_file(p, [rec])
            got = PerfRecord.from_file(p)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0].category_top, "Interpreter")
        self.assertEqual(got[0].category_sub, "ceval")

    def test_unclassified_perf_record_roundtrip(self):
        rec = PerfRecord(symbol="foo", period=5, period_share=1.0)
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "u.csv"
            PerfRecord.to_file(p, [rec])
            got = PerfRecord.from_file(p)
        self.assertEqual(got[0].category_top, "")


class TestTablesContract(unittest.TestCase):
    def test_aggregated_tables_roundtrip(self):
        tables = AggregatedTables(
            by_category=(
                CategoryRow(platform_id="x86", benchmark="tpch", category_top="Interpreter",
                            children_share=80.0, self_share=50.0, period_sum=1000,
                            sample_count=10, top_shared_object="libpython", top_symbols_preview="PyEval"),
            ),
            by_shared_object=(
                SharedObjectRow(platform_id="x86", benchmark="tpch", shared_object="libpython",
                                children_share=80.0, self_share=50.0, period_sum=1000,
                                sample_count=10, top_symbols_preview="PyEval"),
            ),
            by_symbol=(
                SymbolRow(platform_id="x86", benchmark="tpch", category_top="Interpreter",
                          category_sub="ceval", shared_object="libpython", symbol="PyEval",
                          children_share=80.0, self_share=50.0, period_sum=1000,
                          sample_count=10, rank_in_category=1, rank_in_shared_object=1),
            ),
        )
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            tables.write_dir(base)
            got = AggregatedTables.read_dir(base)
        self.assertEqual(got, tables)

    def test_read_missing_dir_is_empty(self):
        with tempfile.TemporaryDirectory() as d:
            got = AggregatedTables.read_dir(Path(d))
        self.assertEqual(got.by_category, ())
        self.assertEqual(got.by_symbol, ())


class TestInstructionContract(unittest.TestCase):
    def test_instruction_dataset_roundtrip(self):
        ds = InstructionDataset(
            samples=(
                InstructionSample(category_top="Interpreter", shared_object="libpython",
                                  symbol="PyEval", segment_id="1", line_index=2, ip=0x400,
                                  instruction_offset="0x40", instruction_share=5.0,
                                  instruction_text="mov rax,1"),
            ),
        )
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            ds.write_dir(base)
            got = InstructionDataset.read_dir(base)
        self.assertEqual(got, ds)

    def test_read_missing_dir_is_empty(self):
        with tempfile.TemporaryDirectory() as d:
            got = InstructionDataset.read_dir(Path(d))
        self.assertEqual(got.samples, ())


class TestTimingContract(unittest.TestCase):
    def test_timing_dataset_roundtrip(self):
        ds = TimingDataset(
            entries=(TimingEntry(label="q1", wall_clock_ns=1_000_000_000),),
            platform_id="x86", benchmark="tpch",
        )
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "timing-normalized.json"
            ds.write_json(p)
            got = TimingDataset.read_json(p)
        self.assertEqual(got, ds)

    def test_read_missing_file_is_empty(self):
        with tempfile.TemporaryDirectory() as d:
            got = TimingDataset.read_json(Path(d) / "nope.json")
        self.assertEqual(got.entries, ())


if __name__ == "__main__":
    unittest.main()
