from __future__ import annotations

import unittest

from pyframework_pipeline.analyze.perf_data_to_csv import (
    PerfConversionError,
    fill_missing_ips,
    parse_perf_script_text,
    parse_report_text,
)


REPORT_WITH_PID_COMMAND = """
# To display the perf.data header info, please use --header/--header-only options.
#
# Children      Self  Period  Pid:Command         IP              Symbol               Shared Object
# ........  ........  ......  ..................  ..............  ...................  ........................
    52.10%    12.34%    4096  1234:python3        7f00aa10        [.] PyEval_EvalFrame /usr/bin/python3.12
    24.00%    24.00%    2048  4567:my_worker      7f00bb20        [.] memcpy           /usr/lib/libc.so.6
""".strip()

REPORT_WITH_SEPARATE_PID_AND_COMMAND = """
# Children      Self  Period  Pid   Command   IP        Symbol          Shared Object
    40.00%    20.00%    1024  888   python3   400abc    [.] main_loop   /tmp/demo
""".strip()

REPORT_WITH_COMMAND_ONLY = """
# Children      Self  Period  Command       Shared Object         Symbol
    52.10%    12.34%    4096  python3.12    /usr/bin/python3.12   [.] PyEval_EvalFrame
""".strip()

REPORT_MISSING_REQUIRED_FIELD = """
# Children      Self  Pid:Command         Symbol               Shared Object
    52.10%    12.34%  1234:python3        [.] PyEval_EvalFrame /usr/bin/python3.12
""".strip()

PERF_SCRIPT_TEXT = """
python3 1234 7f00aa10 [.] PyEval_EvalFrame /usr/bin/python3.12 4096
python3 1234 7f00aa20 [.] PyEval_EvalFrame /usr/bin/python3.12 2048
""".strip()

PERF_SCRIPT_TEXT_WITH_PARENS = """
python3 1234 7f00bb10 PyEval_EvalFrame (/usr/bin/python3.12) 4096
python3 1234 7f00bb20 PyEval_EvalFrame (/usr/bin/python3.12) 2048
""".strip()


class ParseReportTextTests(unittest.TestCase):
    def test_parse_standard_report(self) -> None:
        rows = parse_report_text(REPORT_WITH_PID_COMMAND)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["Children"], "52.10%")
        self.assertEqual(rows[0]["Self"], "12.34%")
        self.assertEqual(rows[0]["Period"], "4096")
        self.assertEqual(rows[0]["Pid:Command"], "1234:python3")
        self.assertEqual(rows[0]["IP"], "7f00aa10")
        self.assertEqual(rows[0]["Symbol"], "[.] PyEval_EvalFrame")
        self.assertEqual(rows[0]["Shared Object"], "/usr/bin/python3.12")

    def test_parse_report_with_separate_pid_and_command(self) -> None:
        rows = parse_report_text(REPORT_WITH_SEPARATE_PID_AND_COMMAND)
        self.assertEqual(rows[0]["Pid:Command"], "888:python3")

    def test_parse_report_with_command_only_header(self) -> None:
        rows = parse_report_text(REPORT_WITH_COMMAND_ONLY)
        self.assertEqual(rows[0]["Pid:Command"], "python3.12")
        self.assertEqual(rows[0]["IP"], "")

    def test_raise_when_required_field_missing(self) -> None:
        with self.assertRaises(PerfConversionError):
            parse_report_text(REPORT_MISSING_REQUIRED_FIELD)

    def test_parse_perf_script_text(self) -> None:
        hints = parse_perf_script_text(PERF_SCRIPT_TEXT)
        self.assertEqual(
            hints[("1234:python3", "PyEval_EvalFrame", "/usr/bin/python3.12")][0],
            "7f00aa10",
        )

    def test_fill_missing_ips(self) -> None:
        rows = [
            {
                "Children": "52.10%",
                "Self": "12.34%",
                "Period": "4096",
                "Pid:Command": "1234:python3",
                "IP": "",
                "Symbol": "[.] PyEval_EvalFrame",
                "Shared Object": "/usr/bin/python3.12",
            }
        ]
        filled = fill_missing_ips(rows, parse_perf_script_text(PERF_SCRIPT_TEXT))
        self.assertEqual(filled[0]["IP"], "7f00aa10")

    def test_parse_perf_script_text_with_parenthesized_dso(self) -> None:
        hints = parse_perf_script_text(PERF_SCRIPT_TEXT_WITH_PARENS)
        self.assertEqual(
            hints[("1234:python3", "PyEval_EvalFrame", "/usr/bin/python3.12")][0],
            "7f00bb10",
        )


if __name__ == "__main__":
    unittest.main()
