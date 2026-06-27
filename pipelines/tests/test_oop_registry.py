from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


class StepRegistryTest(unittest.TestCase):
    def setUp(self) -> None:
        from pyframework_pipeline.registry import StepRegistry

        self.registry = StepRegistry()

    def test_register_and_lookup(self) -> None:
        @self.registry.register
        class SampleStep:
            name = "sample"
            requires = ()
            produces = ("sample-output",)

            def run(self, ctx) -> None:
                return None

        self.assertIs(self.registry.get("sample"), SampleStep)

    def test_topological_order_by_requires_and_produces(self) -> None:
        @self.registry.register
        class SecondStep:
            name = "second"
            requires = ("first-output",)
            produces = ("second-output",)

            def run(self, ctx) -> None:
                return None

        @self.registry.register
        class FirstStep:
            name = "first"
            requires = ()
            produces = ("first-output",)

            def run(self, ctx) -> None:
                return None

        ordered = [step.name for step in self.registry.resolve_plan(["second", "first"])]

        self.assertEqual(ordered, ["first", "second"])

    def test_missing_dependency_raises(self) -> None:
        @self.registry.register
        class MissingInputStep:
            name = "missing-input"
            requires = ("not-produced",)
            produces = ("output",)

            def run(self, ctx) -> None:
                return None

        with self.assertRaisesRegex(ValueError, "not-produced"):
            self.registry.resolve_plan(["missing-input"])

    def test_cycle_raises(self) -> None:
        @self.registry.register
        class FirstStep:
            name = "first"
            requires = ("second-output",)
            produces = ("first-output",)

            def run(self, ctx) -> None:
                return None

        @self.registry.register
        class SecondStep:
            name = "second"
            requires = ("first-output",)
            produces = ("second-output",)

            def run(self, ctx) -> None:
                return None

        with self.assertRaisesRegex(ValueError, "cycle"):
            self.registry.resolve_plan(["first", "second"])


class AdapterRegistryTest(unittest.TestCase):
    def test_framework_adapter_protocol_lists_six_strategies(self) -> None:
        from pyframework_pipeline.contracts.adapter import FrameworkAdapter

        expected = {
            "deploy_workload",
            "run_benchmark",
            "perf_attach_strategy",
            "normalize_timing",
            "collect_flamegraph",
            "disassembly_source",
        }
        actual = {
            name
            for name in dir(FrameworkAdapter)
            if not name.startswith("_") and name != "framework_id"
        }

        self.assertEqual(actual, expected)

    def test_known_frameworks_resolve_to_adapters(self) -> None:
        from pyframework_pipeline.adapters.registry import get_adapter

        for framework_id in ("pyflink", "datajuicer", "udfbenchmarking"):
            with self.subTest(framework_id=framework_id):
                adapter = get_adapter(framework_id)
                self.assertEqual(adapter.framework_id, framework_id)


class OrchestratorRegistryDispatchTest(unittest.TestCase):
    def test_execute_step_dispatches_registered_step(self) -> None:
        from pyframework_pipeline.orchestrator import _execute_step
        from pyframework_pipeline.registry import register_step

        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "marker.txt"

            @register_step
            class DynamicTestStep:
                name = "x-test-dynamic-dispatch"
                requires = ()
                produces = ("x-test-output",)

                def run(self, ctx) -> None:
                    marker.write_text(
                        f"{ctx.project_path.name}:{ctx.platform}:{ctx.config['yes']}",
                        encoding="utf-8",
                    )

            project_path = Path(tmp) / "project.yaml"
            project_path.write_text("id: registry-dispatch\n", encoding="utf-8")

            _execute_step(
                "x-test-dynamic-dispatch",
                project_path,
                Path(tmp) / "run",
                "arm",
                yes=True,
            )

            self.assertEqual(marker.read_text(encoding="utf-8"), "project.yaml:arm:True")


class PerfRecordsCheckpointTest(unittest.TestCase):
    def test_complete_perf_records_csv_is_valid_checkpoint(self) -> None:
        from pyframework_pipeline.orchestrator import _perf_records_csv_is_complete

        header = [
            "platform_id", "arch", "python_version", "build_id", "benchmark",
            "event", "children", "self", "period", "pid", "command",
            "pid_command", "shared_object", "symbol", "ip", "category_top",
            "category_sub", "category_reason", "source_report",
            "sample_count", "instruction_text", "instruction_offset",
            "instruction_share",
        ]
        row = [
            "x86", "x86_64", "3.11.15", "", "data-juicer-text",
            "cycles", "0.01", "0", "0", "", "dj-process",
            "dj-process", "libc.so.6", "PyObject_Call", "", "CPython",
            "", "symbol_regex:PyObject", "perf_report_csv",
            "1", "", "", "",
        ]

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "perf_records.csv"
            path.write_text(",".join(header) + "\n" + ",".join(row) + "\n", encoding="utf-8")

            self.assertTrue(_perf_records_csv_is_complete(path))

    def test_truncated_perf_records_csv_is_not_valid_checkpoint(self) -> None:
        from pyframework_pipeline.orchestrator import _perf_records_csv_is_complete

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "perf_records.csv"
            path.write_text(
                "platform_id,arch,python_version,benchmark,event,children,self,period,"
                "shared_object,symbol,category_top,source_report,sample_count\n"
                "x86,x86_64,3.11.15,data-juicer-text,cycles,0.01,0,0,"
                "libc.so.6,0x00007f497e9035cc,glibc",
                encoding="utf-8",
            )

            self.assertFalse(_perf_records_csv_is_complete(path))


if __name__ == "__main__":
    unittest.main()
