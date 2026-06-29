from __future__ import annotations

import unittest

from pyframework_pipeline.analyze.show_symbol_machine_code import render_symbol_report


class ShowSymbolMachineCodeTests(unittest.TestCase):
    def test_render_symbol_report(self) -> None:
        text = render_symbol_report(
            "_PyEval_EvalFrameDefault",
            "/usr/bin/python3.12",
            [
                {
                    "ip": "7f00aa10",
                    "instruction_offset": "0x0",
                    "instruction_share": "35",
                    "instruction_text": "mov %rax,%rbx",
                }
            ],
        )
        self.assertIn("# 机器码查看", text)
        self.assertIn("_PyEval_EvalFrameDefault", text)
        self.assertIn("| IP | Offset | Instruction% | Instruction |", text)


if __name__ == "__main__":
    unittest.main()
