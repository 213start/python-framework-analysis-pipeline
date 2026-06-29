#!/usr/bin/env python3
"""查看指定热点函数的机器码注解。"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

try:
    from .annotate_perf_hotspots import (
        annotate_symbol,
        resolve_perf_binary,
    )
    from .perf_analysis_common import parse_number, render_markdown_table
except ImportError:
    from annotate_perf_hotspots import annotate_symbol, resolve_perf_binary
    from perf_analysis_common import parse_number, render_markdown_table


LOGGER = logging.getLogger("show_symbol_machine_code")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("perf_data", type=Path, help="perf.data 路径")
    parser.add_argument("symbol", help="函数名")
    parser.add_argument("-d", "--shared-object", default="", help="函数所在 DSO")
    parser.add_argument("-p", "--perf-bin", default="perf", help="perf 可执行文件路径")
    parser.add_argument("-n", "--top-n", type=int, default=20, help="展示前 N 条指令")
    parser.add_argument("-m", "--min-share", type=float, default=0.5, help="最小指令占比阈值，默认 0.5")
    parser.add_argument("-o", "--output", type=Path, help="输出 Markdown 文件；不提供则打印到 stdout")
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="日志级别",
    )
    return parser.parse_args()


def render_symbol_report(symbol: str, shared_object: str, rows: list[dict[str, str]]) -> str:
    parts = [
        "# 机器码查看",
        "",
        f"- Symbol: `{symbol}`",
        f"- Shared Object: `{shared_object or '(auto)'}`",
        "",
        render_markdown_table(
            rows,
            [
                ("ip", "IP"),
                ("instruction_offset", "Offset"),
                ("instruction_share", "Instruction%"),
                ("instruction_text", "Instruction"),
            ],
            empty_message="_No instructions_",
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    )

    perf_bin = resolve_perf_binary(args.perf_bin)
    rows = annotate_symbol(
        perf_bin,
        args.perf_data,
        platform_id="",
        benchmark="",
        category_top="",
        shared_object=args.shared_object,
        symbol=args.symbol,
    )
    rows = [row for row in rows if parse_number(row.get("instruction_share", "0")) >= args.min_share][: args.top_n]
    text = render_symbol_report(args.symbol, args.shared_object, rows)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
