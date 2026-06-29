#!/usr/bin/env python3
"""为正式版热点函数生成机器码 Markdown 报告。"""

from __future__ import annotations

import argparse
from pathlib import Path

try:
    from .perf_analysis_common import format_float, parse_number, read_csv_rows, render_markdown_table
    from .render_platform_report import filter_rows, group_rows, normalize_offset_width, select_report_rows, sort_rows
except ImportError:
    from perf_analysis_common import format_float, parse_number, read_csv_rows, render_markdown_table
    from render_platform_report import filter_rows, group_rows, normalize_offset_width, select_report_rows, sort_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=Path, help="单平台摘要目录")
    parser.add_argument("-o", "--output", type=Path, help="输出 Markdown 文件；不提供时打印到 stdout")
    parser.add_argument("-n", "--top-n", type=int, default=10, help="每个分类最多处理前 N 个热点函数")
    parser.add_argument("-c", "--category", help="只显示指定分类")
    parser.add_argument("-d", "--shared-object", help="只显示指定归属对象")
    return parser.parse_args()


def colorize_instruction_share(value: float) -> str:
    if value <= 0:
        return ""
    if value >= 20:
        color = "#c62828"
    elif value >= 5:
        color = "#ef6c00"
    elif value >= 1:
        color = "#9e9d24"
    else:
        color = "#757575"
    return f"<span style=\"color:{color}\">{format_float(value)}</span>"


def _render_symbol_instruction_tables(rows: list[dict[str, str]]) -> list[str]:
    if not rows:
        return ["_No instructions_", ""]

    normalized_rows = normalize_offset_width(rows)
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in normalized_rows:
        grouped.setdefault(row.get("segment_id", "0"), []).append(row)

    parts: list[str] = []
    multiple_segments = len(grouped) > 1
    for segment_id, segment_rows in grouped.items():
        if multiple_segments:
            parts.extend([f"##### IP 段 {int(segment_id) + 1}", ""])
        first_row = segment_rows[0]
        parts.extend(
            [
                f"- IP: `{first_row.get('ip', '')}`",
                f"- IP Self%: {first_row.get('ip_self_share', '') or 'N/A'}",
                "",
            ]
        )
        parts.append(
            render_markdown_table(
                segment_rows,
                [
                    ("instruction_share", "Instruction%"),
                    ("ip", "IP"),
                    ("instruction_offset", "Offset"),
                    ("instruction_text", "Instruction"),
                ],
                empty_message="_No instructions_",
            )
        )
        parts.append("")
    return parts


def render_report(
    input_dir: Path,
    top_n: int,
    *,
    category: str | None = None,
    shared_object: str | None = None,
) -> str:
    category_rows = sort_rows(
        filter_rows(read_csv_rows(input_dir / "category_summary.csv"), category=category, shared_object=None),
        "self",
    )[:top_n]
    symbol_rows = sort_rows(
        filter_rows(read_csv_rows(input_dir / "symbol_hotspots.csv"), category=category, shared_object=shared_object),
        "self",
    )
    ip_rows = sort_rows(
        filter_rows(read_csv_rows(input_dir / "ip_hotspots.csv"), category=category, shared_object=shared_object),
        "self",
    )
    instruction_path = input_dir / "instruction_hotspots.csv"
    instruction_rows = filter_rows(read_csv_rows(instruction_path), category=category, shared_object=shared_object) if instruction_path.exists() else []

    header_source = category_rows or symbol_rows or ip_rows or instruction_rows
    platform_id = header_source[0].get("platform_id", "") if header_source else ""
    benchmark = header_source[0].get("benchmark", "") if header_source else ""
    symbols_by_category = group_rows(symbol_rows, ("category_top",))
    ip_rows_by_symbol = group_rows(ip_rows, ("category_top", "shared_object", "symbol"))
    instructions_by_symbol = group_rows(instruction_rows, ("category_top", "shared_object", "symbol"))

    parts = [
        "# 单平台机器码报告",
        "",
        f"- 平台: {platform_id}",
        f"- 基准: {benchmark}",
        "- 视图来源: formal",
        "- Instruction% 口径: perf annotate 输出的函数内局部百分比",
        "- 热点机器码阈值: Instruction% >= 0.5",
    ]
    if category:
        parts.append(f"- 分类过滤: {category}")
    if shared_object:
        parts.append(f"- 归属对象过滤: {shared_object}")
    parts.extend(["", "## 分类与热点函数机器码", ""])

    for index, category_row in enumerate(category_rows, start=1):
        category_name = category_row.get("category_top", "")
        category_symbols = select_report_rows(
            symbols_by_category.get((category_name,), []),
            "self_share",
            report_style="formal",
            top_n=top_n,
            threshold=0.5,
        )
        parts.extend(
            [
                f"### {index}. `{category_name}`",
                "",
                f"- Category Self%: {category_row.get('self_share', '0')}",
                "",
            ]
        )
        for symbol_row in category_symbols:
            symbol_key = (
                symbol_row.get("category_top", ""),
                symbol_row.get("shared_object", ""),
                symbol_row.get("symbol", ""),
            )
            symbol_instructions = []
            ip_self_by_ip = {
                row.get("ip", ""): row.get("hotspot_self", "0") or row.get("self_share", "0")
                for row in ip_rows_by_symbol.get(symbol_key, [])
                if row.get("ip", "").strip()
            }
            for row in instructions_by_symbol.get(symbol_key, []):
                instruction_share = parse_number(row.get("instruction_share", "0"))
                symbol_instructions.append(
                    {
                        "segment_id": row.get("segment_id", "0"),
                        "line_index": row.get("line_index", "0"),
                        "_instruction_share_value": format_float(instruction_share),
                        "instruction_share": colorize_instruction_share(instruction_share),
                        "ip": row.get("ip", ""),
                        "ip_self_share": ip_self_by_ip.get(row.get("ip", ""), ""),
                        "instruction_offset": row.get("instruction_offset", ""),
                        "instruction_text": row.get("instruction_text", ""),
                    }
                )
            parts.extend(
                [
                    f"#### 函数: `{symbol_row.get('symbol', '')}`",
                    "",
                    f"- DSO: `{symbol_row.get('shared_object', '')}`",
                    f"- Function Self%: {symbol_row.get('self_share', '0')}",
                    "",
                ]
            )
            parts.extend(_render_symbol_instruction_tables(symbol_instructions))

    return "\n".join(parts).rstrip() + "\n"


def main() -> int:
    args = parse_args()
    text = render_report(
        args.input_dir,
        args.top_n,
        category=args.category,
        shared_object=args.shared_object,
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
