#!/usr/bin/env python3
"""将单平台摘要渲染为 SVG 图表。"""

from __future__ import annotations

import argparse
import html
from pathlib import Path

try:
    from .perf_analysis_common import parse_number, read_csv_rows
except ImportError:
    from perf_analysis_common import parse_number, read_csv_rows


WIDTH = 1200
LEFT_LABEL = 360
RIGHT_MARGIN = 80
TOP_MARGIN = 70
BOTTOM_MARGIN = 50
ROW_HEIGHT = 34
BAR_HEIGHT = 34
GRID_COLOR = "#D7D0C7"
TEXT_COLOR = "#1F1A14"
BG_COLOR = "#FFF8EF"
PALETTE = [
    "#D96C06",
    "#2A6F97",
    "#6C9A8B",
    "#C44536",
    "#7A5C61",
    "#8C6A43",
    "#3B7A57",
    "#B56576",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=Path, help="单平台摘要目录")
    parser.add_argument("-o", "--output-dir", type=Path, required=True, help="图片输出目录")
    parser.add_argument("-n", "--top-n", type=int, default=10, help="每张图展示前 N 条")
    parser.add_argument("-c", "--category", help="只渲染指定分类")
    return parser.parse_args()


def _escape(value: str) -> str:
    return html.escape(value, quote=True)


def _truncate(text: str, max_len: int = 48) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def _sort_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return sorted(rows, key=lambda row: -parse_number(row.get("self_share", "0")))


def _stacked_distribution_svg(rows: list[dict[str, str]], *, title: str, label_key: str, value_key: str) -> str:
    chart_rows = rows or []
    legend_rows = chart_rows[:]
    total_share = sum(parse_number(row.get(value_key, "0")) for row in chart_rows)
    if total_share < 100:
        legend_rows = legend_rows + [{label_key: "Others", value_key: f"{100 - total_share:.6f}"}]
    legend_start = TOP_MARGIN + BAR_HEIGHT + 60
    height = legend_start + max(2, len(legend_rows)) * ROW_HEIGHT + BOTTOM_MARGIN
    chart_width = WIDTH - LEFT_LABEL - RIGHT_MARGIN

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{height}" viewBox="0 0 {WIDTH} {height}">',
        f'<rect width="{WIDTH}" height="{height}" fill="{BG_COLOR}"/>',
        f'<text x="32" y="42" font-size="28" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{_escape(title)}</text>',
    ]

    for tick in range(6):
        value = tick * 20
        x = LEFT_LABEL + chart_width * value / 100
        parts.append(f'<line x1="{x}" y1="{TOP_MARGIN - 10}" x2="{x}" y2="{TOP_MARGIN + BAR_HEIGHT + 10}" stroke="{GRID_COLOR}" stroke-width="1"/>')
        parts.append(f'<text x="{x}" y="{TOP_MARGIN + BAR_HEIGHT + 32}" text-anchor="middle" font-size="12" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{value}%</text>')

    if not legend_rows:
        parts.append(
            f'<text x="{LEFT_LABEL}" y="{TOP_MARGIN + 30}" font-size="18" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">No data</text>'
        )
    else:
        parts.append(f'<rect x="{LEFT_LABEL}" y="{TOP_MARGIN}" width="{chart_width}" height="{BAR_HEIGHT}" rx="4" fill="none" stroke="{GRID_COLOR}" stroke-width="1"/>')
        current_x = LEFT_LABEL
        for index, row in enumerate(legend_rows):
            value = parse_number(row.get(value_key, "0"))
            segment_width = chart_width * max(value, 0) / 100.0
            color = PALETTE[index % len(PALETTE)]
            parts.append(f'<rect x="{current_x}" y="{TOP_MARGIN}" width="{segment_width}" height="{BAR_HEIGHT}" fill="{color}"/>')
            current_x += segment_width

        for index, row in enumerate(legend_rows):
            y = legend_start + index * ROW_HEIGHT
            color = PALETTE[index % len(PALETTE)]
            label = _truncate(str(row.get(label_key, "")), 56)
            value = parse_number(row.get(value_key, "0"))
            parts.append(f'<rect x="48" y="{y - 14}" width="18" height="18" rx="3" fill="{color}"/>')
            parts.append(f'<text x="80" y="{y}" font-size="15" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{_escape(label)}</text>')
            parts.append(f'<text x="{WIDTH - 48}" y="{y}" text-anchor="end" font-size="15" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{value:.2f}%</text>')

    parts.append("</svg>")
    return "\n".join(parts)


def render_visuals(input_dir: Path, output_dir: Path, top_n: int, category: str | None = None) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    category_rows = _sort_rows(read_csv_rows(input_dir / "category_summary.csv"))
    object_rows = _sort_rows(read_csv_rows(input_dir / "shared_object_summary.csv"))
    symbol_rows = _sort_rows(read_csv_rows(input_dir / "symbol_hotspots.csv"))

    if category:
        category_rows = [row for row in category_rows if row.get("category_top", "") == category]
        object_rows = [row for row in object_rows if row.get("category_top", "") == category]
        symbol_rows = [row for row in symbol_rows if row.get("category_top", "") == category]

    files = [
        (
            output_dir / "category_share.svg",
            _stacked_distribution_svg(category_rows[:top_n], title="Category Distribution", label_key="category_top", value_key="self_share"),
        ),
        (
            output_dir / "shared_object_share.svg",
            _stacked_distribution_svg(object_rows[:top_n], title="Shared Object Distribution", label_key="shared_object", value_key="self_share"),
        ),
        (
            output_dir / "symbol_hotspots.svg",
            _stacked_distribution_svg(symbol_rows[:top_n], title="Hot Symbol Distribution", label_key="symbol", value_key="self_share"),
        ),
    ]

    written: list[Path] = []
    for path, content in files:
        path.write_text(content, encoding="utf-8")
        written.append(path)
    return written


def main() -> int:
    args = parse_args()
    render_visuals(args.input_dir, args.output_dir, args.top_n, category=args.category)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
