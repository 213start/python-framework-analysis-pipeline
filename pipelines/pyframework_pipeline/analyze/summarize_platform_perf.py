#!/usr/bin/env python3
"""基于平台记录 CSV 生成单平台摘要。"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

try:
    from .perf_analysis_common import aggregate_rows, build_preview, format_float, parse_number, parse_period, rank_rows, read_csv_rows, write_csv_rows
except ImportError:
    from perf_analysis_common import aggregate_rows, build_preview, format_float, parse_number, parse_period, rank_rows, read_csv_rows, write_csv_rows


LOGGER = logging.getLogger("summarize_platform_perf")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="输入平台记录 CSV")
    parser.add_argument("-o", "--output-dir", type=Path, required=True, help="输出目录")
    parser.add_argument("-s", "--script-input", type=Path, help="perf script 导出的逐地址采样 CSV")
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="日志级别",
    )
    return parser.parse_args()


def summarize_ip_hotspots_from_script(
    script_rows: list[dict[str, str]],
    symbol_rows: list[dict[str, str]],
    *,
    platform_id: str,
    benchmark: str,
) -> list[dict[str, str]]:
    symbol_metadata_by_key = {
        (row.get("shared_object", ""), row.get("symbol", "")): (
            row.get("category_top", "Unknown"),
            row.get("shared_object", ""),
        )
        for row in symbol_rows
    }
    symbol_candidates: dict[str, set[tuple[str, str]]] = {}
    for row in symbol_rows:
        symbol_candidates.setdefault(row.get("symbol", ""), set()).add(
            (row.get("category_top", "Unknown"), row.get("shared_object", ""))
        )
    total_period = sum(parse_period(row.get("Period", "0")) for row in script_rows) or 1
    grouped: dict[tuple[str, str, str, str], dict[str, str | int]] = {}
    for row in script_rows:
        shared_object = row.get("Shared Object", "").strip()
        symbol = row.get("Symbol", "").strip()
        ip = row.get("IP", "").strip()
        if not shared_object or not symbol or not ip:
            continue

        metadata = symbol_metadata_by_key.get((shared_object, symbol))
        if metadata is None:
            candidates = symbol_candidates.get(symbol, set())
            if len(candidates) == 1:
                metadata = next(iter(candidates))
        if metadata is None:
            category_top, canonical_shared_object = "Unknown", shared_object
        else:
            category_top, canonical_shared_object = metadata

        key = (category_top, canonical_shared_object, symbol, ip)
        bucket = grouped.setdefault(
            key,
            {
                "platform_id": platform_id,
                "benchmark": benchmark,
                "category_top": category_top,
                "shared_object": canonical_shared_object,
                "symbol": symbol,
                "ip": ip,
                "period_sum": 0,
                "sample_count": 0,
            },
        )
        bucket["period_sum"] = int(bucket["period_sum"]) + parse_period(row.get("Period", "0"))
        bucket["sample_count"] = int(bucket["sample_count"]) + 1

    results: list[dict[str, str]] = []
    for bucket in grouped.values():
        period_sum = int(bucket["period_sum"])
        self_share = 100.0 * period_sum / total_period
        results.append(
            {
                "platform_id": str(bucket["platform_id"]),
                "benchmark": str(bucket["benchmark"]),
                "category_top": str(bucket["category_top"]),
                "shared_object": str(bucket["shared_object"]),
                "symbol": str(bucket["symbol"]),
                "ip": str(bucket["ip"]),
                "children_share": "0",
                "self_share": format_float(self_share),
                "period_sum": str(period_sum),
                "sample_count": str(bucket["sample_count"]),
                "instruction_offset": "",
                "instruction_share": "",
                "hotspot_self": format_float(self_share),
                "instruction_text": "",
            }
        )
    results.sort(
        key=lambda row: (
            -parse_number(row.get("hotspot_self", "0")),
            -parse_period(row.get("period_sum", "0")),
            row.get("ip", ""),
        )
    )
    return rank_rows(results, ["platform_id", "benchmark", "shared_object", "symbol"], "rank_in_symbol")


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    )

    rows = read_csv_rows(args.input)
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    category_summary = aggregate_rows(rows, ["platform_id", "benchmark", "category_top"], sort_by="self_share")
    category_preview = build_preview(rows, ["platform_id", "benchmark", "category_top"], "symbol")
    category_dso_preview = build_preview(rows, ["platform_id", "benchmark", "category_top"], "shared_object", top_n=1)
    for row in category_summary:
        scope = (row["platform_id"], row["benchmark"], row["category_top"])
        row["top_symbols_preview"] = category_preview.get(scope, "")
        row["top_shared_object"] = category_dso_preview.get(scope, "")
    write_csv_rows(
        output_dir / "category_summary.csv",
        [
            "platform_id",
            "benchmark",
            "category_top",
            "children_share",
            "self_share",
            "period_sum",
            "sample_count",
            "top_shared_object",
            "top_symbols_preview",
        ],
        category_summary,
    )

    object_summary = aggregate_rows(rows, ["platform_id", "benchmark", "shared_object"], sort_by="self_share")
    object_preview = build_preview(rows, ["platform_id", "benchmark", "shared_object"], "symbol")
    for row in object_summary:
        scope = (row["platform_id"], row["benchmark"], row["shared_object"])
        row["top_symbols_preview"] = object_preview.get(scope, "")
    write_csv_rows(
        output_dir / "shared_object_summary.csv",
        [
            "platform_id",
            "benchmark",
            "shared_object",
            "children_share",
            "self_share",
            "period_sum",
            "sample_count",
            "top_symbols_preview",
        ],
        object_summary,
    )

    symbol_hotspots = aggregate_rows(
        rows,
        ["platform_id", "benchmark", "category_top", "category_sub", "shared_object", "symbol"],
        sort_by="self_share",
    )
    symbol_hotspots = rank_rows(symbol_hotspots, ["platform_id", "benchmark", "category_top"], "rank_in_category")
    symbol_hotspots = rank_rows(
        symbol_hotspots,
        ["platform_id", "benchmark", "category_top", "shared_object"],
        "rank_in_shared_object",
    )
    write_csv_rows(
        output_dir / "symbol_hotspots.csv",
        [
            "platform_id",
            "benchmark",
            "category_top",
            "category_sub",
            "shared_object",
            "symbol",
            "children_share",
            "self_share",
            "period_sum",
            "sample_count",
            "rank_in_category",
            "rank_in_shared_object",
        ],
        symbol_hotspots,
    )

    if args.script_input:
        metadata_source = rows[0] if rows else {}
        ip_hotspots = summarize_ip_hotspots_from_script(
            read_csv_rows(args.script_input),
            symbol_hotspots,
            platform_id=metadata_source.get("platform_id", ""),
            benchmark=metadata_source.get("benchmark", ""),
        )
    else:
        ip_hotspots = aggregate_rows(
            rows,
            ["platform_id", "benchmark", "category_top", "shared_object", "symbol", "ip"],
            sort_by="self_share",
        )
        ip_hotspots = [row for row in ip_hotspots if row.get("ip", "").strip()]
        for row in ip_hotspots:
            row["hotspot_self"] = row.get("self_share", "0")
        ip_hotspots.sort(
            key=lambda row: (
                -float(row.get("hotspot_self", "0") or 0),
                -float(row.get("self_share", "0") or 0),
                -int(float(row.get("period_sum", "0") or 0)),
            )
        )
        ip_hotspots = rank_rows(
            ip_hotspots,
            ["platform_id", "benchmark", "shared_object", "symbol"],
            "rank_in_symbol",
        )
    write_csv_rows(
        output_dir / "ip_hotspots.csv",
        [
            "platform_id",
            "benchmark",
            "category_top",
            "shared_object",
            "symbol",
            "ip",
            "children_share",
            "self_share",
            "period_sum",
            "sample_count",
            "instruction_offset",
            "instruction_share",
            "hotspot_self",
            "instruction_text",
            "rank_in_symbol",
        ],
        ip_hotspots,
    )

    LOGGER.info("单平台摘要完成: input=%s output_dir=%s rows=%s", args.input, output_dir, len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
