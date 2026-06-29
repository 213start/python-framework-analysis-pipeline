"""Tests for the C1-C4 analyze subflows (path-in/path-out)."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pyframework_pipeline.analyze.aggregate import aggregate
from pyframework_pipeline.analyze.annotate import annotate
from pyframework_pipeline.analyze.classify import CategoryClassifier, classify
from pyframework_pipeline.analyze.parse import parse
from pyframework_pipeline.contracts.records import PerfRecord


class TestClassify(unittest.TestCase):
    def test_classifier_loads_builtin_rules(self):
        clf = CategoryClassifier()
        self.assertGreater(len(clf.rules), 0)

    def test_classify_path_roundtrip(self):
        rec = PerfRecord(
            platform_id="x86", benchmark="tpch",
            shared_object="/lib/libpython3.14.so.1.0",
            symbol="PyEval_EvalFrameDefault", period=100, period_share=10.0,
        )
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            inp = base / "in.csv"
            PerfRecord.to_file(inp, [rec])
            out = base / "out.csv"
            classify(input_path=inp, output_path=out)
            got = PerfRecord.from_file(out)
        self.assertEqual(len(got), 1)
        # PyEval_EvalFrameDefault is a CPython interpreter symbol -> classified.
        self.assertNotEqual(got[0].category_top, "Unknown")
        self.assertTrue(got[0].category_top)


class TestParseSignature(unittest.TestCase):
    def test_parse_missing_perf_data_raises(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(FileNotFoundError):
                parse(Path(d) / "missing.perf.data", output=Path(d) / "out.csv")


class TestAggregateSignature(unittest.TestCase):
    def test_aggregate_empty_records_writes_tables(self):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            PerfRecord.to_file(base / "in.csv", [])
            out = aggregate(input_path=base / "in.csv", output_dir=base / "tables")
            self.assertTrue((base / "tables").is_dir())
            # The three summary CSVs are produced (empty but present).
            self.assertTrue((base / "tables" / "category_summary.csv").exists())


class TestAnnotateSignature(unittest.TestCase):
    def test_annotate_skips_without_perf_data(self):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            PerfRecord.to_file(base / "recs.csv", [])
            # No perf_data -> graceful skip, returns None, no crash.
            result = annotate(records_path=base / "recs.csv", output_dir=base / "ann")
            self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
