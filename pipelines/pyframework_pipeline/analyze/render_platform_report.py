#!/usr/bin/env python3
"""将单平台聚合结果渲染为 Markdown 报告。"""

from __future__ import annotations

import argparse
from pathlib import Path

try:
    from .perf_analysis_common import format_float, parse_number, read_csv_rows, render_markdown_table
except ImportError:
    from perf_analysis_common import format_float, parse_number, read_csv_rows, render_markdown_table


PLATFORM_SORT_CHOICES = (
    "self",
    "period",
    "symbol",
)
REPORT_STYLE_CHOICES = (
    "formal",
    "full",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=Path, help="单平台摘要目录")
    parser.add_argument("-o", "--output", type=Path, help="输出文本文件；不提供时打印到 stdout")
    parser.add_argument("-n", "--top-n", type=int, default=10, help="每个 section 展示前 N 条")
    parser.add_argument(
        "-s",
        "--sort-by",
        choices=PLATFORM_SORT_CHOICES,
        default="self",
        help="报告排序方式",
    )
    parser.add_argument("-c", "--category", help="只显示指定分类")
    parser.add_argument("-d", "--shared-object", help="只显示指定归属对象")
    parser.add_argument(
        "-m",
        "--report-style",
        choices=REPORT_STYLE_CHOICES,
        default="formal",
        help="报告视图：formal 为裁剪后的正式版，full 为完整补充版",
    )
    return parser.parse_args()


def _sort_key(row: dict[str, str], sort_by: str) -> tuple[float | str, float]:
    if sort_by == "self":
        return (-parse_number(row.get("self_share", "0")), -parse_number(row.get("period_sum", "0")))
    if sort_by == "period":
        return (-parse_number(row.get("period_sum", "0")), -parse_number(row.get("self_share", "0")))
    if sort_by == "symbol":
        return (row.get("symbol", "") or row.get("shared_object", "") or row.get("category_top", ""), "")
    return (-parse_number(row.get("self_share", "0")), -parse_number(row.get("period_sum", "0")))


def _filter_rows(
    rows: list[dict[str, str]],
    *,
    category: str | None,
    shared_object: str | None,
) -> list[dict[str, str]]:
    filtered = rows
    if category:
        filtered = [row for row in filtered if row.get("category_top", "") == category]
    if shared_object:
        filtered = [row for row in filtered if row.get("shared_object", "") == shared_object]
    return filtered


def filter_rows(
    rows: list[dict[str, str]],
    *,
    category: str | None,
    shared_object: str | None,
) -> list[dict[str, str]]:
    return _filter_rows(rows, category=category, shared_object=shared_object)


def _sort_rows(rows: list[dict[str, str]], sort_by: str) -> list[dict[str, str]]:
    return sorted(rows, key=lambda row: _sort_key(row, sort_by))


def sort_rows(rows: list[dict[str, str]], sort_by: str) -> list[dict[str, str]]:
    return _sort_rows(rows, sort_by)


def _group_rows(rows: list[dict[str, str]], keys: tuple[str, ...]) -> dict[tuple[str, ...], list[dict[str, str]]]:
    grouped: dict[tuple[str, ...], list[dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(tuple(row.get(key, "") for key in keys), []).append(row)
    return grouped


def group_rows(rows: list[dict[str, str]], keys: tuple[str, ...]) -> dict[tuple[str, ...], list[dict[str, str]]]:
    return _group_rows(rows, keys)


def _trim_hotspots(rows: list[dict[str, str]], value_key: str, *, threshold: float = 0.5) -> list[dict[str, str]]:
    positive_rows = [row for row in rows if parse_number(row.get(value_key, "0")) > 0]
    if len(positive_rows) <= 3:
        return positive_rows[:3]
    selected = [row for row in positive_rows if parse_number(row.get(value_key, "0")) > threshold]
    if len(selected) >= 3:
        return selected
    return positive_rows[:3]

def select_report_rows(
    rows: list[dict[str, str]],
    value_key: str,
    *,
    report_style: str,
    top_n: int,
    threshold: float = 0.5,
) -> list[dict[str, str]]:
    return _select_rows(rows, value_key, report_style=report_style, top_n=top_n, threshold=threshold)


def _normalize_offset_width(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    parsed_rows: list[tuple[dict[str, str], int | None]] = []
    max_digits = 0
    for row in rows:
        raw_offset = row.get("instruction_offset", "").strip().lower()
        try:
            value = int(raw_offset, 16) if raw_offset.startswith("0x") else None
        except ValueError:
            value = None
        if value is not None:
            digits = len(format(value, "x"))
            max_digits = max(max_digits, digits)
        parsed_rows.append((dict(row), value))

    if max_digits == 0:
        return rows

    normalized: list[dict[str, str]] = []
    for row, value in parsed_rows:
        if value is not None:
            row["instruction_offset"] = f"0x{value:0{max_digits}x}"
        normalized.append(row)
    return normalized


def normalize_offset_width(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return _normalize_offset_width(rows)


def _select_rows(
    rows: list[dict[str, str]],
    value_key: str,
    *,
    report_style: str,
    top_n: int,
    threshold: float,
) -> list[dict[str, str]]:
    if report_style == "full":
        return [row for row in rows if parse_number(row.get(value_key, "0")) > 0][:top_n]
    return _trim_hotspots(rows, value_key, threshold=threshold)[:top_n]


def _build_address_rows(ip_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        {
            "ip": row.get("ip", ""),
            "address_self_share": format_float(parse_number(row.get("hotspot_self", "0") or row.get("self_share", "0"))),
            "_address_self_value": row.get("hotspot_self", "0") or row.get("self_share", "0"),
        }
        for row in ip_rows
        if row.get("ip", "").strip()
    ]


def _render_hot_symbol_block(symbol_row: dict[str, str], address_rows: list[dict[str, str]]) -> list[str]:
    parts = [
        f"#### 函数: `{symbol_row.get('symbol', '')}`",
        "",
        f"- DSO: `{symbol_row.get('shared_object', '')}`",
        f"- Self%: {symbol_row.get('self_share', '0')}",
        "",
        render_markdown_table(
            address_rows,
            [
                ("ip", "IP"),
                ("address_self_share", "Address Self%"),
            ],
            empty_message="_No addresses_",
        ),
        "",
    ]
    return parts


def render_report(
    input_dir: Path,
    top_n: int,
    *,
    sort_by: str = "self",
    category: str | None = None,
    shared_object: str | None = None,
    report_style: str = "formal",
) -> str:
    category_rows = _sort_rows(
        _filter_rows(read_csv_rows(input_dir / "category_summary.csv"), category=category, shared_object=None),
        sort_by,
    )[:top_n]
    object_rows = _sort_rows(
        _filter_rows(read_csv_rows(input_dir / "shared_object_summary.csv"), category=category, shared_object=shared_object),
        sort_by,
    )[:top_n]
    symbol_rows = _sort_rows(
        _filter_rows(read_csv_rows(input_dir / "symbol_hotspots.csv"), category=category, shared_object=shared_object),
        sort_by,
    )
    ip_rows = _sort_rows(
        _filter_rows(read_csv_rows(input_dir / "ip_hotspots.csv"), category=category, shared_object=shared_object),
        sort_by,
    )

    header_source = category_rows or object_rows or symbol_rows or ip_rows
    platform_id = header_source[0].get("platform_id", "") if header_source else ""
    benchmark = header_source[0].get("benchmark", "") if header_source else ""
    symbols_by_category = _group_rows(symbol_rows, ("category_top",))
    ips_by_symbol = _group_rows(ip_rows, ("category_top", "shared_object", "symbol"))

    parts = [
        "# 单平台性能报告",
        "",
        f"- 平台: {platform_id}",
        f"- 基准: {benchmark}",
        f"- 排序: {sort_by}",
        f"- 视图: {report_style}",
    ]
    if category:
        parts.append(f"- 分类过滤: {category}")
    if shared_object:
        parts.append(f"- 归属对象过滤: {shared_object}")
    parts.extend(
        [
            "",
            "## 热点 DSO",
            render_markdown_table(
                object_rows[:top_n],
                [
                    ("shared_object", "Shared Object"),
                    ("self_share", "Self%"),
                ],
            ),
            "",
            "## 热点分类总表",
            render_markdown_table(
                [
                    {
                        "category_top": row.get("category_top", ""),
                        "self_share": row.get("self_share", "0"),
                    }
                    for row in category_rows[:top_n]
                ],
                [
                    ("category_top", "Category"),
                    ("self_share", "Self%"),
                ],
            ),
        ]
    )
    parts.extend(["", "## 热点分类", ""])

    for index, category_row in enumerate(category_rows[:top_n], start=1):
        category_name = category_row.get("category_top", "")
        category_symbols = _select_rows(
            symbols_by_category.get((category_name,), []),
            "self_share",
            report_style=report_style,
            top_n=top_n,
            threshold=0.5,
        )
        parts.extend(
            [
                f"### {index}. `{category_name}`",
                "",
                f"- Self%: {category_row.get('self_share', '0')}",
                "",
                render_markdown_table(
                    category_symbols,
                    [
                        ("symbol", "Symbol"),
                        ("shared_object", "Shared Object"),
                        ("self_share", "Self%"),
                    ],
                    empty_message="_No symbols_",
                ),
                "",
            ]
        )
        for symbol_row in category_symbols:
            symbol_key = (
                symbol_row.get("category_top", ""),
                symbol_row.get("shared_object", ""),
                symbol_row.get("symbol", ""),
            )
            symbol_address_rows = _build_address_rows(list(ips_by_symbol.get(symbol_key, [])))
            selected_addresses = _select_rows(
                symbol_address_rows,
                "_address_self_value",
                report_style=report_style,
                top_n=top_n,
                threshold=0.1,
            )
            parts.extend(_render_hot_symbol_block(symbol_row, selected_addresses))

    return "\n".join(parts).rstrip() + "\n"


def main() -> int:
    args = parse_args()
    text = render_report(
        args.input_dir,
        args.top_n,
        sort_by=args.sort_by,
        category=args.category,
        shared_object=args.shared_object,
        report_style=args.report_style,
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
