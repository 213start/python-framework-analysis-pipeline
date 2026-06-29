#!/usr/bin/env python3
"""对比两个平台的平台记录 CSV。"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

try:
    from .perf_analysis_common import (
        aggregate_rows,
        compare_aggregates,
        normalize_shared_object_for_compare,
        read_csv_rows,
        write_csv_rows,
    )
except ImportError:
    from perf_analysis_common import (
        aggregate_rows,
        compare_aggregates,
        normalize_shared_object_for_compare,
        read_csv_rows,
        write_csv_rows,
    )


LOGGER = logging.getLogger("compare_platform_perf")


def _prepare_shared_object_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    prepared: list[dict[str, str]] = []
    for row in rows:
        item = dict(row)
        item["shared_object_compare_key"] = normalize_shared_object_for_compare(row.get("shared_object", ""))
        prepared.append(item)
    return prepared


def _best_value_by_compare_key(rows: list[dict[str, str]], value_key: str) -> dict[str, str]:
    aggregated = aggregate_rows(rows, ["shared_object_compare_key", value_key], sort_by="self_share")
    mapping: dict[str, str] = {}
    for row in aggregated:
        compare_key = row.get("shared_object_compare_key", "")
        value = row.get(value_key, "")
        if compare_key and value and compare_key not in mapping:
            mapping[compare_key] = value
    return mapping


def build_shared_object_compare(
    baseline_rows: list[dict[str, str]],
    target_rows: list[dict[str, str]],
    *,
    baseline_platform: str,
    target_platform: str,
    baseline_e2e_time: float,
    target_e2e_time: float,
) -> list[dict[str, str]]:
    baseline_prepared = _prepare_shared_object_rows(baseline_rows)
    target_prepared = _prepare_shared_object_rows(target_rows)
    compare_rows = compare_aggregates(
        baseline_prepared,
        target_prepared,
        ["shared_object_compare_key"],
        baseline_platform=baseline_platform,
        target_platform=target_platform,
        baseline_e2e_time=baseline_e2e_time,
        target_e2e_time=target_e2e_time,
    )
    baseline_display_map = _best_value_by_compare_key(baseline_prepared, "shared_object")
    target_display_map = _best_value_by_compare_key(target_prepared, "shared_object")
    baseline_category_map = _best_value_by_compare_key(baseline_prepared, "category_top")
    target_category_map = _best_value_by_compare_key(target_prepared, "category_top")

    for row in compare_rows:
        compare_key = row.pop("shared_object_compare_key", "")
        row["shared_object"] = (
            baseline_display_map.get(compare_key)
            or target_display_map.get(compare_key)
            or compare_key
        )
        row["category_top"] = (
            baseline_category_map.get(compare_key)
            or target_category_map.get(compare_key)
            or ""
        )
    return compare_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-b", "--baseline", type=Path, required=True, help="baseline 平台记录 CSV")
    parser.add_argument("-t", "--target", type=Path, required=True, help="target 平台记录 CSV")
    parser.add_argument("-p", "--baseline-platform", required=True, help="baseline 平台名称")
    parser.add_argument("-q", "--target-platform", required=True, help="target 平台名称")
    parser.add_argument("-x", "--baseline-e2e-time", type=float, required=True, help="baseline 端到端时间")
    parser.add_argument("-y", "--target-e2e-time", type=float, required=True, help="target 端到端时间")
    parser.add_argument("-o", "--output-dir", type=Path, required=True, help="输出目录")
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

    baseline_rows = read_csv_rows(args.baseline)
    target_rows = read_csv_rows(args.target)
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    category_compare = compare_aggregates(
        baseline_rows,
        target_rows,
        ["category_top"],
        baseline_platform=args.baseline_platform,
        target_platform=args.target_platform,
        baseline_e2e_time=args.baseline_e2e_time,
        target_e2e_time=args.target_e2e_time,
    )
    write_csv_rows(
        output_dir / "category_compare.csv",
        [
            "benchmark",
            "category_top",
            "baseline_platform",
            "target_platform",
            "baseline_share",
            "target_share",
            "baseline_e2e_time",
            "target_e2e_time",
            "baseline_est_time",
            "target_est_time",
            "delta_time",
            "delta_share",
        ],
        category_compare,
    )

    object_compare = build_shared_object_compare(
        baseline_rows,
        target_rows,
        baseline_platform=args.baseline_platform,
        target_platform=args.target_platform,
        baseline_e2e_time=args.baseline_e2e_time,
        target_e2e_time=args.target_e2e_time,
    )
    write_csv_rows(
        output_dir / "shared_object_compare.csv",
        [
            "benchmark",
            "category_top",
            "shared_object",
            "baseline_platform",
            "target_platform",
            "baseline_share",
            "target_share",
            "baseline_e2e_time",
            "target_e2e_time",
            "baseline_est_time",
            "target_est_time",
            "delta_time",
            "delta_share",
        ],
        object_compare,
    )

    symbol_compare = compare_aggregates(
        baseline_rows,
        target_rows,
        ["category_top", "shared_object", "symbol"],
        baseline_platform=args.baseline_platform,
        target_platform=args.target_platform,
        baseline_e2e_time=args.baseline_e2e_time,
        target_e2e_time=args.target_e2e_time,
        include_target_only=False,
    )
    write_csv_rows(
        output_dir / "symbol_compare.csv",
        [
            "benchmark",
            "category_top",
            "shared_object",
            "symbol",
            "baseline_rank",
            "baseline_platform",
            "target_platform",
            "baseline_share",
            "target_share",
            "baseline_e2e_time",
            "target_e2e_time",
            "baseline_est_time",
            "target_est_time",
            "delta_time",
            "delta_share",
        ],
        symbol_compare,
    )

    LOGGER.info(
        "双平台对比完成: baseline=%s target=%s output_dir=%s",
        args.baseline,
        args.target,
        output_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
