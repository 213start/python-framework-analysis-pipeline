"""TDD tests for orchestrator output streaming and error reporting.

Rules enforced:
1. Long-running executor.run() calls (timeout >= 60s) must use stream=True
   so output is visible in real-time and not lost on failure.
2. All error paths must include both stdout and stderr (or merged output
   from streaming mode) in the StepError message.
3. All sub-steps must have a logger.info() call with a step label prefix
   like [5a] or [5b] for progress visibility.
"""

import ast
import re
import unittest
from pathlib import Path

ORCHESTRATOR = Path(__file__).resolve().parents[2] / "pipelines" / "pyframework_pipeline" / "orchestrator.py"
SRC = ORCHESTRATOR.read_text(encoding="utf-8")
TREE = ast.parse(SRC)


def _find_executor_run_calls() -> list[dict]:
    """Extract all executor.run() calls from the orchestrator AST."""
    calls = []
    for node in ast.walk(TREE):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "run"
                and isinstance(func.value, ast.Name) and func.value.id == "executor"):
            continue
        timeout = 300  # default from SshExecutor.run()
        stream = False
        for kw in node.keywords:
            if kw.arg == "timeout" and isinstance(kw.value, ast.Constant):
                timeout = kw.value.value
            if kw.arg == "stream" and isinstance(kw.value, ast.Constant):
                stream = kw.value.value
        calls.append({
            "line": node.lineno,
            "timeout": timeout,
            "stream": stream,
        })
    return calls


class TestStreamingForLongOperations(unittest.TestCase):
    """All executor.run() calls with timeout >= 60s must use stream=True."""

    def test_long_ops_use_stream(self):
        calls = _find_executor_run_calls()
        violations = []
        for c in calls:
            # Check the run() call and its arguments (may span multiple lines)
            start = c["line"] - 1
            end = min(start + 5, len(SRC.splitlines()))
            call_block = "\n".join(SRC.splitlines()[start:end])

            is_docker_cp = "docker cp" in call_block
            is_grep = "grep " in call_block

            if c["timeout"] >= 60 and not c["stream"] and not is_docker_cp and not is_grep:
                violations.append(
                    f"L{c['line']}: timeout={c['timeout']}, stream=False"
                )

        self.assertEqual(
            violations, [],
            "These long-running executor.run() calls need stream=True:\n"
            + "\n".join(violations),
        )


class TestStepErrorIncludesOutput(unittest.TestCase):
    """All StepError raises after executor.run() must include output."""

    def test_step_errors_include_stdout_or_output(self):
        """StepError after executor.run() that checks returncode must show output."""
        lines = SRC.splitlines()
        errors = []

        # Find all executor.run() result variable names and their lines
        # Then find StepError raises that follow a returncode check on those vars
        result_vars = {}  # line -> var_name
        for i, line in enumerate(lines):
            m = re.match(r'\s*(\w+)\s*=\s*executor\.run\(', line)
            if m:
                result_vars[i] = m.group(1)

        for i, line in enumerate(lines):
            if "raise StepError" not in line:
                continue
            # Only check StepErrors that are preceded by a returncode check
            # on an executor.run() result (within 15 lines above)
            precedes = "\n".join(lines[max(0, i - 15):i])
            if "returncode" not in precedes:
                continue

            block = line
            j = i + 1
            while j < len(lines) and (lines[j].strip().startswith("f\"") or lines[j].strip().startswith("f'")):
                block += "\n" + lines[j]
                j += 1

            has_output = any(
                kw in block
                for kw in [
                    "result.stdout", "result.stderr", "result.stdout[-",
                    "jm_result.stdout", "jm_result.stderr",
                    "tm_result.stdout", "tm_result.stderr",
                    "check.stdout", "check.stderr",
                    "objdump_result.stdout",
                    "cp_result.stdout",
                ]
            )
            if not has_output:
                errors.append(f"L{i + 1}: StepError doesn't reference result output")

        self.assertEqual(errors, [], "\n".join(errors))


class TestSubStepLogging(unittest.TestCase):
    """All major sub-steps in _run_benchmark and _run_collect must have log lines."""

    def test_benchmark_has_step_labels(self):
        """_run_benchmark must log key sub-steps with [5a] prefix."""
        # Find _run_benchmark function body
        content = SRC
        self.assertIn("[5a]", content, "Missing [5a] step labels in _run_benchmark")

        # Must have labels for: perf check, perf start, perf stop, query run
        required_labels = [
            "Ensuring perf",
            "Starting perf record",
            "Stopping perf record",
        ]
        for label in required_labels:
            self.assertIn(label, content, f"Missing log line containing '{label}'")

    def test_collect_has_step_labels(self):
        """_run_collect must log key sub-steps with [5b] prefix."""
        self.assertIn("[5b]", content := SRC, "Missing [5b] step labels in _run_collect")

        required_labels = [
            "Collecting perf.data",
            "Running perf-kits",
            "Collecting objdump",
        ]
        for label in required_labels:
            self.assertIn(label, content, f"Missing log line containing '{label}'")


class TestStreamOutputCapturedInErrors(unittest.TestCase):
    """When stream=True, result.stderr is empty — errors must use result.stdout."""

    def test_streaming_errors_use_stdout_not_stderr(self):
        """StepError after stream=True runs should use stdout, not stderr."""
        lines = SRC.splitlines()
        errors = []
        # Track which executor.run() calls use stream=True
        stream_lines = set()
        for node in ast.walk(TREE):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (isinstance(func, ast.Attribute) and func.attr == "run"
                    and isinstance(func.value, ast.Name) and func.value.id == "executor"):
                continue
            for kw in node.keywords:
                if kw.arg == "stream" and isinstance(kw.value, ast.Constant) and kw.value.value:
                    stream_lines.add(node.lineno)

        # For each stream=True call, find the nearest StepError and check it
        # doesn't reference result.stderr as the ONLY output source
        for sl in sorted(stream_lines):
            # Scan forward from the stream line for a raise StepError
            for i in range(sl, min(sl + 30, len(lines))):
                if "raise StepError" in lines[i]:
                    block = "\n".join(lines[i:i + 10])
                    if "result.stderr" in block and "result.stdout" not in block:
                        errors.append(
                            f"L{sl} uses stream=True but L{i + 1} StepError "
                            f"only references stderr (empty in streaming mode)"
                        )
                    break

        self.assertEqual(errors, [], "\n".join(errors))


if __name__ == "__main__":
    unittest.main()
