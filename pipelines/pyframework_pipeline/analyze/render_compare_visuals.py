#!/usr/bin/env python3
"""将双平台对比结果渲染为 SVG 图表。"""

from __future__ import annotations

import argparse
import html
from pathlib import Path

try:
    from .perf_analysis_common import parse_number, read_csv_rows
except ImportError:
    from perf_analysis_common import parse_number, read_csv_rows


WIDTH = 1560
LEFT_LABEL = 520
RIGHT_MARGIN = 120
TOP_MARGIN = 120
BOTTOM_MARGIN = 70
ROW_HEIGHT = 56
BAR_HEIGHT = 18
LABEL_GAP = 28
BASELINE_COLOR = "#D96C06"
TARGET_COLOR = "#006E7F"
DELTA_POS_COLOR = "#B11E3B"
DELTA_NEG_COLOR = "#1A7F37"
GRID_COLOR = "#D7D0C7"
TEXT_COLOR = "#1F1A14"
BG_COLOR = "#FFF8EF"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=Path, help="双平台对比结果目录")
    parser.add_argument("-o", "--output-dir", type=Path, required=True, help="图片输出目录")
    parser.add_argument("-n", "--top-n", type=int, default=10, help="每张图展示前 N 条")
    parser.add_argument("-c", "--category", help="只渲染指定分类")
    return parser.parse_args()


def _escape(value: str) -> str:
    return html.escape(value, quote=True)


def _truncate(text: str, max_len: int = 72) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def _sort_rows(rows: list[dict[str, str]], key: str) -> list[dict[str, str]]:
    return sorted(rows, key=lambda row: -parse_number(row.get(key, "0")))


def _dual_bar_chart_svg(
    rows: list[dict[str, str]],
    *,
    title: str,
    label_key: str,
    baseline_key: str,
    target_key: str,
    baseline_label: str,
    target_label: str,
    suffix: str,
) -> str:
    chart_rows = rows or []
    height = TOP_MARGIN + BOTTOM_MARGIN + max(1, len(chart_rows)) * ROW_HEIGHT
    chart_width = WIDTH - LEFT_LABEL - RIGHT_MARGIN
    max_value = max(
        (
            max(parse_number(row.get(baseline_key, "0")), parse_number(row.get(target_key, "0")))
            for row in chart_rows
        ),
        default=1.0,
    )
    max_value = max(max_value, 1.0)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{height}" viewBox="0 0 {WIDTH} {height}">',
        f'<rect width="{WIDTH}" height="{height}" fill="{BG_COLOR}"/>',
        f'<text x="32" y="42" font-size="28" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{_escape(title)}</text>',
        f'<rect x="32" y="52" width="18" height="10" fill="{BASELINE_COLOR}"/><text x="58" y="62" font-size="13" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{_escape(baseline_label)}</text>',
        f'<rect x="180" y="52" width="18" height="10" fill="{TARGET_COLOR}"/><text x="206" y="62" font-size="13" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{_escape(target_label)}</text>',
    ]

    for tick in range(5):
        value = max_value * tick / 4
        x = LEFT_LABEL + chart_width * tick / 4
        parts.append(f'<line x1="{x}" y1="{TOP_MARGIN - 10}" x2="{x}" y2="{height - BOTTOM_MARGIN + 6}" stroke="{GRID_COLOR}" stroke-width="1"/>')
        parts.append(
            f'<text x="{x}" y="{height - 12}" text-anchor="middle" font-size="12" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{value:.2f}{suffix}</text>'
        )

    if not chart_rows:
        parts.append(
            f'<text x="{LEFT_LABEL}" y="{TOP_MARGIN + 30}" font-size="18" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">No data</text>'
        )
    else:
        for index, row in enumerate(chart_rows):
            y = TOP_MARGIN + index * ROW_HEIGHT
            label = _truncate(str(row.get(label_key, "")))
            baseline_value = parse_number(row.get(baseline_key, "0"))
            target_value = parse_number(row.get(target_key, "0"))
            baseline_width = 0 if max_value == 0 else chart_width * baseline_value / max_value
            target_width = 0 if max_value == 0 else chart_width * target_value / max_value
            parts.append(
                f'<text x="{LEFT_LABEL - 12}" y="{y + 18}" text-anchor="end" font-size="15" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{_escape(label)}</text>'
            )
            parts.append(
                f'<rect x="{LEFT_LABEL}" y="{y}" width="{baseline_width}" height="{BAR_HEIGHT}" rx="4" fill="{BASELINE_COLOR}"/>'
            )
            parts.append(
                f'<rect x="{LEFT_LABEL}" y="{y + 16}" width="{target_width}" height="{BAR_HEIGHT}" rx="4" fill="{TARGET_COLOR}"/>'
            )
            parts.append(
                f'<text x="{LEFT_LABEL + max(baseline_width, target_width) + 8}" y="{y + 13}" font-size="13" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{baseline_value:.2f}{suffix} / {target_value:.2f}{suffix}</text>'
            )

    parts.append("</svg>")
    return "\n".join(parts)


def _delta_bar_chart_svg(rows: list[dict[str, str]], *, title: str, label_key: str, value_key: str, suffix: str) -> str:
    chart_rows = rows or []
    height = TOP_MARGIN + BOTTOM_MARGIN + max(1, len(chart_rows)) * ROW_HEIGHT
    chart_left = LEFT_LABEL + LABEL_GAP
    chart_right = WIDTH - RIGHT_MARGIN
    center_x = chart_left + (chart_right - chart_left) / 2
    half_width = (chart_right - chart_left) / 2
    max_value = max((abs(parse_number(row.get(value_key, "0"))) for row in chart_rows), default=1.0)
    max_value = max(max_value, 1.0)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{height}" viewBox="0 0 {WIDTH} {height}">',
        f'<rect width="{WIDTH}" height="{height}" fill="{BG_COLOR}"/>',
        f'<text x="32" y="42" font-size="28" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{_escape(title)}</text>',
        f'<text x="32" y="70" font-size="14" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">Right = compare platform faster, Left = compare platform slower, unit = {suffix}</text>',
        f'<line x1="{center_x}" y1="{TOP_MARGIN - 10}" x2="{center_x}" y2="{height - BOTTOM_MARGIN + 6}" stroke="{GRID_COLOR}" stroke-width="2"/>',
    ]

    if not chart_rows:
        parts.append(
            f'<text x="{LEFT_LABEL}" y="{TOP_MARGIN + 30}" font-size="18" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">No data</text>'
        )
    else:
        for index, row in enumerate(chart_rows):
            y = TOP_MARGIN + index * ROW_HEIGHT
            label = _truncate(str(row.get(label_key, "")))
            raw_value = parse_number(row.get(value_key, "0"))
            value = raw_value
            width = 0 if max_value == 0 else half_width * abs(value) / max_value
            x = center_x - width if value < 0 else center_x
            color = DELTA_NEG_COLOR if value < 0 else DELTA_POS_COLOR
            parts.append(f'<text x="{LEFT_LABEL}" y="{y + 24}" text-anchor="end" font-size="15" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{_escape(label)}</text>')
            parts.append(
                f'<rect x="{x}" y="{y + 6}" width="{width}" height="{BAR_HEIGHT + 8}" rx="4" fill="{color}"/>'
            )
            parts.append(
                f'<text x="{center_x + width + 12 if value >= 0 else center_x - width - 12}" y="{y + 46}" text-anchor="{"start" if value >= 0 else "end"}" font-size="13" font-family="Menlo, Consolas, monospace" fill="{TEXT_COLOR}">{abs(raw_value):.2f}{suffix}</text>'
            )

    parts.append("</svg>")
    return "\n".join(parts)


def render_visuals(input_dir: Path, output_dir: Path, top_n: int, category: str | None = None) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    category_rows = _sort_rows(read_csv_rows(input_dir / "category_compare.csv"), "delta_time")
    object_rows = _sort_rows(read_csv_rows(input_dir / "shared_object_compare.csv"), "delta_time")
    symbol_rows = _sort_rows(read_csv_rows(input_dir / "symbol_compare.csv"), "delta_time")
    category_rows = [row for row in category_rows if parse_number(row.get("baseline_est_time", "0")) != 0 or parse_number(row.get("target_est_time", "0")) != 0]
    object_rows = [row for row in object_rows if parse_number(row.get("baseline_est_time", "0")) != 0 or parse_number(row.get("target_est_time", "0")) != 0]
    symbol_rows = [row for row in symbol_rows if parse_number(row.get("baseline_est_time", "0")) != 0 or parse_number(row.get("target_est_time", "0")) != 0]

    if category:
        category_rows = [row for row in category_rows if row.get("category_top", "") == category]
        object_rows = [row for row in object_rows if row.get("category_top", "") == category]
        symbol_rows = [row for row in symbol_rows if row.get("category_top", "") == category]

    header_source = category_rows or object_rows or symbol_rows
    target_label = header_source[0].get("target_platform", "target") if header_source else "target"

    files = [
        (
            output_dir / "category_compare.svg",
            _delta_bar_chart_svg(
                category_rows[:top_n],
                title="Category Delta",
                label_key="category_top",
                value_key="delta_time",
                suffix="s",
            ),
        ),
        (
            output_dir / "shared_object_compare.svg",
            _delta_bar_chart_svg(
                object_rows[:top_n],
                title="Shared Object Delta",
                label_key="shared_object",
                value_key="delta_time",
                suffix="s",
            ),
        ),
        (
            output_dir / "symbol_delta.svg",
            _delta_bar_chart_svg(
                symbol_rows[:top_n],
                title="Hot Symbol Delta",
                label_key="symbol",
                value_key="delta_time",
                suffix="s",
            ),
        ),
    ]

    files = [
        (path, content.replace("compare platform", target_label))
        for path, content in files
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
