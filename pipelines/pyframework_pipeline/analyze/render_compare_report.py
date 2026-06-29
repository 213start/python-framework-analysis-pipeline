#!/usr/bin/env python3
"""将双平台对比结果渲染为 Markdown 报告。"""

from __future__ import annotations

import argparse
from pathlib import Path

try:
    from .perf_analysis_common import parse_number, read_csv_rows, render_markdown_table
except ImportError:
    from perf_analysis_common import parse_number, read_csv_rows, render_markdown_table


COMPARE_SORT_CHOICES = (
    "delta_time",
    "baseline_time",
    "target_time",
    "delta_share",
    "baseline_rank",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=Path, help="双平台对比结果目录")
    parser.add_argument("-o", "--output", type=Path, help="输出文本文件；不提供时打印到 stdout")
    parser.add_argument("-n", "--top-n", type=int, default=10, help="每个 section 展示前 N 条")
    parser.add_argument(
        "-s",
        "--sort-by",
        choices=COMPARE_SORT_CHOICES,
        default="delta_time",
        help="报告排序方式",
    )
    parser.add_argument("-c", "--category", help="只显示指定分类")
    parser.add_argument("-d", "--shared-object", help="只显示指定归属对象")
    return parser.parse_args()


def _delta_marker(value: str, *, baseline_platform: str, target_platform: str) -> str:
    number = parse_number(value)
    if number > 0:
        return f"{target_platform} faster"
    if number < 0:
        return f"{target_platform} slower"
    return "flat"


def _sort_key(row: dict[str, str], sort_by: str) -> tuple[float, float]:
    if sort_by == "baseline_time":
        return (-parse_number(row.get("baseline_est_time", "0")), -parse_number(row.get("delta_time", "0")))
    if sort_by == "target_time":
        return (-parse_number(row.get("target_est_time", "0")), -parse_number(row.get("delta_time", "0")))
    if sort_by == "delta_share":
        return (-parse_number(row.get("delta_share", "0")), -parse_number(row.get("delta_time", "0")))
    if sort_by == "baseline_rank":
        return (parse_number(row.get("baseline_rank", "0")), -parse_number(row.get("delta_time", "0")))
    return (-parse_number(row.get("delta_time", "0")), -parse_number(row.get("delta_share", "0")))


def _filter_rows(
    rows: list[dict[str, str]],
    *,
    category: str | None,
    shared_object: str | None,
) -> list[dict[str, str]]:
    filtered = rows
    filtered = [
        row
        for row in filtered
        if parse_number(row.get("baseline_est_time", "0")) != 0 or parse_number(row.get("target_est_time", "0")) != 0
    ]
    if category:
        filtered = [row for row in filtered if row.get("category_top", "") == category]
    if shared_object:
        filtered = [row for row in filtered if row.get("shared_object", "") == shared_object]
    return filtered


def _prepare_rows(rows: list[dict[str, str]], sort_by: str) -> list[dict[str, str]]:
    prepared = []
    for row in rows:
        item = dict(row)
        item["delta_note"] = _delta_marker(
            item.get("delta_time", "0"),
            baseline_platform=item.get("baseline_platform", "baseline"),
            target_platform=item.get("target_platform", "target"),
        )
        prepared.append(item)
    return sorted(prepared, key=lambda row: _sort_key(row, sort_by))


def render_report(
    input_dir: Path,
    top_n: int,
    *,
    sort_by: str = "delta_time",
    category: str | None = None,
    shared_object: str | None = None,
) -> str:
    category_rows = _prepare_rows(
        _filter_rows(read_csv_rows(input_dir / "category_compare.csv"), category=category, shared_object=None),
        sort_by,
    )[:top_n]
    object_rows = _prepare_rows(
        _filter_rows(read_csv_rows(input_dir / "shared_object_compare.csv"), category=category, shared_object=shared_object),
        sort_by,
    )[:top_n]
    symbol_rows = _prepare_rows(
        _filter_rows(read_csv_rows(input_dir / "symbol_compare.csv"), category=category, shared_object=shared_object),
        sort_by,
    )[:top_n]

    header_source = category_rows or object_rows or symbol_rows
    benchmark = header_source[0].get("benchmark", "") if header_source else ""
    baseline_platform = header_source[0].get("baseline_platform", "") if header_source else ""
    target_platform = header_source[0].get("target_platform", "") if header_source else ""

    parts = [
        "# 双平台对比报告",
        "",
        f"- 基准: {benchmark}",
        f"- 基线平台: {baseline_platform}",
        f"- 对比平台: {target_platform}",
        f"- 排序: {sort_by}",
    ]
    if category:
        parts.append(f"- 分类过滤: {category}")
    if shared_object:
        parts.append(f"- 归属对象过滤: {shared_object}")
    parts.extend(
        [
            "",
            "## 分类差异",
            render_markdown_table(
                category_rows,
                [
                    ("category_top", "Category"),
                    ("baseline_share", f"{baseline_platform}%"),
                    ("target_share", f"{target_platform}%"),
                    ("baseline_est_time", f"{baseline_platform} Time (s)"),
                    ("target_est_time", f"{target_platform} Time (s)"),
                    ("delta_time", "Delta Time (s)"),
                    ("delta_note", "Note"),
                ],
            ),
            "",
            "## 归属对象差异",
            render_markdown_table(
                object_rows,
                [
                    ("shared_object", "Shared Object"),
                    ("baseline_est_time", f"{baseline_platform} Time (s)"),
                    ("target_est_time", f"{target_platform} Time (s)"),
                    ("delta_time", "Delta Time (s)"),
                    ("delta_note", "Note"),
                ],
            ),
            "",
            "## 热点函数差异",
            render_markdown_table(
                symbol_rows,
                [
                    ("category_top", "Category"),
                    ("symbol", "Symbol"),
                    ("baseline_est_time", f"{baseline_platform} Time (s)"),
                    ("target_est_time", f"{target_platform} Time (s)"),
                    ("delta_time", "Delta Time (s)"),
                    ("delta_note", "Note"),
                ],
            ),
        ]
    )
    return "\n".join(parts).rstrip() + "\n"


def main() -> int:
    args = parse_args()
    text = render_report(
        args.input_dir,
        args.top_n,
        sort_by=args.sort_by,
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
