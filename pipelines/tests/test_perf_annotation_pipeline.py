from __future__ import annotations

import unittest

from pyframework_pipeline.analyze.annotate_perf_hotspots import merge_instruction_rows, parse_annotate_text


ANNOTATE_TEXT = """
Samples:
-----------
  35.00 : 7f00aa10: mov    %rax,%rbx
  12.00 : 7f00aa14: callq  0x7f00bb20
   8.00 : 7f00aa18: add    $0x1,%rax
""".strip()


class AnnotateParsingTests(unittest.TestCase):
    def test_parse_annotate_text(self) -> None:
        rows = parse_annotate_text(
            ANNOTATE_TEXT,
            platform_id="amd-baseline",
            benchmark="richards",
            category_top="CPython.Interpreter",
            shared_object="/usr/bin/python3.12",
            symbol="_PyEval_EvalFrameDefault",
        )
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["ip"], "7f00aa10")
        self.assertEqual(rows[0]["instruction_offset"], "0x0")
        self.assertEqual(rows[0]["instruction_text"], "mov    %rax,%rbx")

    def test_merge_instruction_rows(self) -> None:
        record_rows = [
            {
                "shared_object": "/usr/bin/python3.12",
                "symbol": "_PyEval_EvalFrameDefault",
                "ip": "7f00aa14",
                "instruction_text": "",
                "instruction_offset": "",
            }
        ]
        instruction_rows = parse_annotate_text(
            ANNOTATE_TEXT,
            platform_id="amd-baseline",
            benchmark="richards",
            category_top="CPython.Interpreter",
            shared_object="/usr/bin/python3.12",
            symbol="_PyEval_EvalFrameDefault",
        )
        merged = merge_instruction_rows(record_rows, instruction_rows)
        self.assertEqual(merged[0]["instruction_offset"], "0x4")
        self.assertEqual(merged[0]["instruction_text"], "callq  0x7f00bb20")

    def test_merge_instruction_rows_without_exact_ip_match_keeps_blank_instruction_fields(self) -> None:
        record_rows = [
            {
                "shared_object": "/usr/bin/python3.12",
                "symbol": "_PyEval_EvalFrameDefault",
                "ip": "",
                "instruction_text": "",
                "instruction_offset": "",
            }
        ]
        instruction_rows = parse_annotate_text(
            ANNOTATE_TEXT,
            platform_id="amd-baseline",
            benchmark="richards",
            category_top="CPython.Interpreter",
            shared_object="/usr/bin/python3.12",
            symbol="_PyEval_EvalFrameDefault",
        )
        merged = merge_instruction_rows(record_rows, instruction_rows)
        self.assertEqual(merged[0]["ip"], "")
        self.assertEqual(merged[0]["instruction_offset"], "")
        self.assertEqual(merged[0]["instruction_text"], "")


if __name__ == "__main__":
    unittest.main()
