#!/usr/bin/env python3
"""将 raw perf CSV 转换为带分类的平台记录表。"""

from __future__ import annotations

import argparse
import logging
import platform
from pathlib import Path

try:
    from .perf_analysis_common import NORMALIZED_FIELDS, load_rules, normalize_raw_row, read_csv_rows, write_csv_rows
except ImportError:
    from perf_analysis_common import NORMALIZED_FIELDS, load_rules, normalize_raw_row, read_csv_rows, write_csv_rows


LOGGER = logging.getLogger("normalize_perf_records")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="输入 raw perf CSV")
    parser.add_argument("-o", "--output", type=Path, required=True, help="输出平台记录 CSV")
    parser.add_argument("-p", "--platform-id", required=True, help="平台标识，用来区分一次完整平台采集，例如 amd-baseline")
    parser.add_argument(
        "-a",
        "--arch",
        default=platform.machine() or "unknown",
        help="架构，默认自动读取当前运行环境，例如 x86_64 或 aarch64",
    )
    parser.add_argument("-V", "--python-version", default="", help="Python 版本")
    parser.add_argument("-i", "--build-id", default="", help="构建标识")
    parser.add_argument("-b", "--benchmark", required=True, help="基准名称，用来区分 workload 或测试场景，例如 richards")
    parser.add_argument("-e", "--event", default="cycles", help="perf 事件名")
    parser.add_argument("-s", "--source-report", default="perf_report_csv", help="来源说明")
    parser.add_argument(
        "-r",
        "--rules",
        type=Path,
        default=Path(__file__).with_name("cpython_category_rules.json"),
        help="分类规则 JSON 文件",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="日志级别",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    )

    rules = load_rules(args.rules)
    raw_rows = read_csv_rows(args.input)
    records_rows = [
        normalize_raw_row(
            row,
            platform_id=args.platform_id,
            arch=args.arch,
            python_version=args.python_version,
            build_id=args.build_id,
            benchmark=args.benchmark,
            event=args.event,
            source_report=args.source_report,
            rules=rules,
        )
        for row in raw_rows
    ]
    write_csv_rows(args.output, NORMALIZED_FIELDS, records_rows)
    LOGGER.info(
        "平台记录生成完成: input=%s output=%s rows=%s rules=%s",
        args.input,
        args.output,
        len(records_rows),
        args.rules,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
