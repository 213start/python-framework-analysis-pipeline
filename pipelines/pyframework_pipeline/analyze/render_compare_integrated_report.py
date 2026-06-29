#!/usr/bin/env python3
"""生成双平台整合 HTML 报告。"""

from __future__ import annotations

import argparse
import base64
import html
from pathlib import Path

try:
    from .perf_analysis_common import normalize_ip, parse_number, read_csv_rows
    from .render_platform_machine_code_report import colorize_instruction_share
    from .render_platform_report import group_rows, select_report_rows, sort_rows
except ImportError:
    from perf_analysis_common import normalize_ip, parse_number, read_csv_rows
    from render_platform_machine_code_report import colorize_instruction_share
    from render_platform_report import group_rows, select_report_rows, sort_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=Path, help="双平台对比结果目录（tables）")
    parser.add_argument("-o", "--output", type=Path, help="输出 HTML 文件；不提供时打印到 stdout")
    parser.add_argument("-n", "--top-n", type=int, default=10, help="每个 section 展示前 N 条")
    parser.add_argument("-b", "--baseline-root", type=Path, help="baseline 单平台输出根目录")
    parser.add_argument("-t", "--target-root", type=Path, help="target 单平台输出根目录")
    parser.add_argument("-v", "--visuals-dir", type=Path, help="差异图目录，默认取 input_dir 的同级 visuals/")
    return parser.parse_args()


def _escape(value: str) -> str:
    return html.escape(value, quote=True)


def _delta_marker(value: str, *, target_platform: str) -> str:
    number = parse_number(value)
    if number > 0:
        return f"{target_platform} faster"
    if number < 0:
        return f"{target_platform} slower"
    return "flat"


def _sort_compare_rows(rows: list[dict[str, str]], sort_by: str = "delta_time") -> list[dict[str, str]]:
    def key(row: dict[str, str]) -> tuple[float, float]:
        if sort_by == "baseline_rank":
            return (parse_number(row.get("baseline_rank", "0")), -parse_number(row.get("delta_time", "0")))
        return (-parse_number(row.get(sort_by, "0")), -parse_number(row.get("delta_share", "0")))

    prepared = []
    for row in rows:
        item = dict(row)
        item["delta_note"] = _delta_marker(item.get("delta_time", "0"), target_platform=item.get("target_platform", "target"))
        prepared.append(item)
    prepared = [
        row
        for row in prepared
        if parse_number(row.get("baseline_est_time", "0")) != 0 or parse_number(row.get("target_est_time", "0")) != 0
    ]
    return sorted(prepared, key=key)


def _svg_to_data_uri(path: Path) -> str:
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}"


def _render_visual_block(title: str, image_path: Path) -> str:
    if not image_path.exists():
        return f'<section class="compare-block"><h3>{_escape(title)}</h3><p class="muted">Image missing</p></section>'
    return (
        f'<section class="compare-block">'
        f'<h3>{_escape(title)}</h3>'
        f'<img class="compare-visual" alt="{_escape(title)}" src="{_svg_to_data_uri(image_path)}" />'
        f"</section>"
    )


def _render_html_table(rows: list[dict[str, str]], columns: list[tuple[str, str]]) -> str:
    header = "".join(f"<th>{_escape(label)}</th>" for _, label in columns)
    body_rows: list[str] = []
    for row in rows:
        cells = "".join(f"<td>{_escape(row.get(key, ''))}</td>" for key, _ in columns)
        body_rows.append(f"<tr>{cells}</tr>")
    body = "".join(body_rows) if body_rows else f'<tr><td colspan="{len(columns)}" class="muted">No data</td></tr>'
    return f'<table class="report-table"><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>'


def _render_instruction_share_cell(value: str) -> str:
    number = parse_number(value)
    colored = colorize_instruction_share(number)
    return colored or ""


def _render_instruction_block(rows: list[dict[str, str]], ip_self_by_ip: dict[str, str]) -> str:
    if not rows:
        return '<p class="muted">未找到对应函数。</p>'

    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(row.get("segment_id", "0"), []).append(row)

    blocks: list[str] = []
    for _, segment_rows in sorted(grouped.items(), key=lambda item: parse_number(item[0])):
        segment_rows = sorted(segment_rows, key=lambda row: parse_number(row.get("line_index", "0")))
        segment_ip = segment_rows[0].get("ip", "")
        ip_self = ip_self_by_ip.get(normalize_ip(segment_ip), "N/A")
        lines: list[str] = []
        for row in segment_rows:
            share = row.get("instruction_share", "").strip()
            share_html = _render_instruction_share_cell(share)
            offset = _escape(row.get("instruction_offset", ""))
            instruction = _escape(row.get("instruction_text", ""))
            share_part = f'<span class="share-cell">{share_html}</span>' if share_html else '<span class="share-cell"></span>'
            lines.append(f'<div class="instruction-line">{share_part}<span class="offset-cell">{offset}</span><span class="instruction-cell">{instruction}</span></div>')
        blocks.append(
            "".join(
                [
                    '<details class="ip-segment">',
                    f'<summary>IP={_escape(segment_ip)} <span class="summary-meta">IP Self%={_escape(ip_self)}%</span></summary>',
                    '<div class="ip-body">',
                    "".join(lines),
                    "</div>",
                    "</details>",
                ]
            )
        )
    return "".join(blocks)


def _select_baseline_hot_symbols_by_category(
    baseline_root: Path,
    ordered_categories: list[str],
    top_n: int,
) -> list[tuple[str, list[dict[str, str]]]]:
    symbol_rows = sort_rows(read_csv_rows(baseline_root / "tables" / "symbol_hotspots.csv"), "self")
    symbols_by_category = group_rows(symbol_rows, ("category_top",))
    selected: list[tuple[str, list[dict[str, str]]]] = []
    for category_name in ordered_categories:
        selected.append(
            (
                category_name,
                select_report_rows(
                    symbols_by_category.get((category_name,), []),
                    "self_share",
                    report_style="formal",
                    top_n=top_n,
                    threshold=0.5,
                ),
            )
        )
    return selected


def _load_instruction_map(root: Path | None) -> dict[tuple[str, str], list[dict[str, str]]]:
    if root is None:
        return {}
    instruction_path = root / "tables" / "instruction_hotspots.csv"
    if not instruction_path.exists():
        return {}
    rows = read_csv_rows(instruction_path)
    rows = sorted(rows, key=lambda row: (parse_number(row.get("segment_id", "0")), parse_number(row.get("line_index", "0"))))
    grouped: dict[tuple[str, str], list[dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault((row.get("shared_object", ""), row.get("symbol", "")), []).append(row)
    return grouped


def _load_ip_self_map(root: Path | None) -> dict[tuple[str, str], dict[str, str]]:
    if root is None:
        return {}
    ip_path = root / "tables" / "ip_hotspots.csv"
    if not ip_path.exists():
        return {}
    grouped: dict[tuple[str, str], dict[str, str]] = {}
    for row in read_csv_rows(ip_path):
        key = (row.get("shared_object", ""), row.get("symbol", ""))
        grouped.setdefault(key, {})[normalize_ip(row.get("ip", ""))] = row.get("hotspot_self", "0") or row.get("self_share", "0")
    return grouped


def _lookup_ip_self_map(
    ip_self_map: dict[tuple[str, str], dict[str, str]],
    *,
    shared_object: str,
    symbol: str,
) -> dict[str, str]:
    exact = ip_self_map.get((shared_object, symbol))
    if exact:
        return exact

    candidates = [ip_map for (candidate_so, candidate_symbol), ip_map in ip_self_map.items() if candidate_symbol == symbol]
    if len(candidates) == 1:
        return candidates[0]
    return {}


def _render_machine_compare_section(
    *,
    baseline_root: Path | None,
    target_root: Path | None,
    baseline_platform: str,
    target_platform: str,
    ordered_categories: list[str],
    top_n: int,
) -> str:
    if baseline_root is None or target_root is None:
        return '<p class="muted">未提供 baseline/target 单平台输出根目录，无法生成机器码对照区。</p>'

    baseline_instruction_map = _load_instruction_map(baseline_root)
    target_instruction_map = _load_instruction_map(target_root)
    baseline_ip_self_map = _load_ip_self_map(baseline_root)
    target_ip_self_map = _load_ip_self_map(target_root)

    category_sections: list[str] = []
    for category_name, symbol_rows in _select_baseline_hot_symbols_by_category(baseline_root, ordered_categories, top_n):
        if not symbol_rows:
            continue

        function_sections: list[str] = []
        for symbol_row in symbol_rows:
            key = (symbol_row.get("shared_object", ""), symbol_row.get("symbol", ""))
            baseline_text = _render_instruction_block(
                baseline_instruction_map.get(key, []),
                _lookup_ip_self_map(
                    baseline_ip_self_map,
                    shared_object=symbol_row.get("shared_object", ""),
                    symbol=symbol_row.get("symbol", ""),
                ),
            )
            target_text = _render_instruction_block(
                target_instruction_map.get(key, []),
                _lookup_ip_self_map(
                    target_ip_self_map,
                    shared_object=symbol_row.get("shared_object", ""),
                    symbol=symbol_row.get("symbol", ""),
                ),
            )
            function_sections.append(
                "".join(
                    [
                        '<details class="function-compare">',
                        f'<summary><span class="symbol-name">{_escape(symbol_row.get("symbol", ""))}</span>'
                        f'<span class="summary-meta">{_escape(symbol_row.get("shared_object", ""))} · { _escape(baseline_platform) } Self%={ _escape(symbol_row.get("self_share", "0")) }%</span></summary>',
                        '<div class="function-body">',
                        f'<div class="function-meta"><span>Shared Object: <code>{_escape(symbol_row.get("shared_object", ""))}</code></span>'
                        f'<span>{_escape(baseline_platform)} Function Self%: { _escape(symbol_row.get("self_share", "0")) }%</span></div>',
                        '<div class="code-compare">',
                        f'<section class="code-pane"><h5>{_escape(baseline_platform)}</h5><div class="code-scroll">{baseline_text}</div></section>',
                        f'<section class="code-pane"><h5>{_escape(target_platform)}</h5><div class="code-scroll">{target_text}</div></section>',
                        "</div>",
                        "</div>",
                        "</details>",
                    ]
                )
            )

        category_sections.append(
            "".join(
                [
                    f'<section class="analysis-category">',
                    f'<h3>{_escape(category_name)}</h3>',
                    "".join(function_sections),
                    "</section>",
                ]
            )
        )
    return "".join(category_sections) if category_sections else '<p class="muted">未找到可用于机器码对照的热点函数。</p>'


def render_report(
    input_dir: Path,
    *,
    top_n: int,
    baseline_root: Path | None = None,
    target_root: Path | None = None,
    visuals_dir: Path | None = None,
) -> str:
    visuals_dir = visuals_dir or input_dir.parent / "visuals"
    category_rows = _sort_compare_rows(read_csv_rows(input_dir / "category_compare.csv"))[:top_n]
    object_rows = _sort_compare_rows(read_csv_rows(input_dir / "shared_object_compare.csv"))[:top_n]
    symbol_rows = _sort_compare_rows(read_csv_rows(input_dir / "symbol_compare.csv"))[:top_n]

    header_source = category_rows or object_rows or symbol_rows
    benchmark = header_source[0].get("benchmark", "") if header_source else ""
    baseline_platform = header_source[0].get("baseline_platform", "baseline") if header_source else "baseline"
    target_platform = header_source[0].get("target_platform", "target") if header_source else "target"
    ordered_categories = [row.get("category_top", "") for row in category_rows if row.get("category_top", "")]

    machine_compare_html = _render_machine_compare_section(
        baseline_root=baseline_root,
        target_root=target_root,
        baseline_platform=baseline_platform,
        target_platform=target_platform,
        ordered_categories=ordered_categories,
        top_n=top_n,
    )

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>双平台整合报告</title>
  <style>
    :root {{
      --bg: #f7f7f4;
      --card: #ffffff;
      --line: #d8d5ce;
      --text: #1f2328;
      --muted: #6a737d;
      --accent: #204b57;
      --shadow: 0 10px 30px rgba(31, 35, 40, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: linear-gradient(180deg, #f5f4ef 0%, #f9f9f7 100%);
      color: var(--text);
      font-family: "SF Pro Text", "PingFang SC", "Noto Sans SC", sans-serif;
      line-height: 1.5;
    }}
    .page {{
      max-width: 1500px;
      margin: 0 auto;
      padding: 32px 24px 48px;
    }}
    h1, h2, h3, h4, h5 {{ margin: 0 0 12px; }}
    h1 {{ font-size: 32px; }}
    h2 {{ font-size: 24px; margin-top: 28px; }}
    h3 {{ font-size: 20px; margin-top: 24px; }}
    .meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px 20px;
      margin: 16px 0 24px;
      color: var(--muted);
    }}
    .compare-block, .analysis-category, .section-card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: var(--shadow);
      padding: 20px;
      margin-bottom: 20px;
    }}
    .compare-visual {{
      display: block;
      width: 100%;
      max-width: 100%;
      height: auto;
      margin: 12px 0 16px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fff;
    }}
    .report-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
      table-layout: fixed;
    }}
    .report-table th, .report-table td {{
      border: 1px solid var(--line);
      padding: 8px 10px;
      text-align: left;
      vertical-align: top;
      word-break: break-word;
    }}
    .report-table thead {{
      background: #f2f4f1;
    }}
    .muted {{
      color: var(--muted);
    }}
    .analysis-category > h3 {{
      margin-bottom: 14px;
      color: var(--accent);
    }}
    .function-compare {{
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fcfcfa;
      margin-top: 14px;
      overflow: hidden;
    }}
    .function-compare summary {{
      cursor: pointer;
      list-style: none;
      padding: 14px 16px;
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: baseline;
      background: #f7f8f5;
    }}
    .function-compare summary::-webkit-details-marker {{
      display: none;
    }}
    .symbol-name {{
      font-weight: 700;
      word-break: break-word;
    }}
    .summary-meta {{
      color: var(--muted);
      font-size: 13px;
      text-align: right;
    }}
    .function-body {{
      padding: 14px 16px 16px;
    }}
    .function-meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px 18px;
      margin-bottom: 12px;
      color: var(--muted);
      font-size: 13px;
    }}
    .code-compare {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
      align-items: start;
    }}
    .code-pane {{
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 12px;
      overflow: hidden;
      background: #fff;
    }}
    .code-pane h5 {{
      padding: 10px 12px;
      background: #eef1ec;
      border-bottom: 1px solid var(--line);
      font-size: 13px;
      color: var(--accent);
    }}
    .code-scroll {{
      margin: 0;
      padding: 12px;
      overflow: auto;
      max-height: 520px;
      font-size: 12px;
      line-height: 1.45;
      font-family: "SFMono-Regular", "Cascadia Code", "Menlo", monospace;
      white-space: pre-wrap;
      background: #fff;
    }}
    .ip-segment {{
      border-top: 1px solid var(--line);
    }}
    .ip-segment:first-child {{
      border-top: 0;
    }}
    .ip-segment summary {{
      cursor: pointer;
      list-style: none;
      padding: 10px 12px;
      background: #fafaf7;
      font-weight: 600;
    }}
    .ip-segment summary::-webkit-details-marker {{
      display: none;
    }}
    .ip-body {{
      padding: 8px 12px 12px;
    }}
    .instruction-line {{
      display: grid;
      grid-template-columns: 72px 72px 1fr;
      gap: 12px;
      align-items: baseline;
      min-width: 0;
      padding: 2px 0;
    }}
    .share-cell {{
      display: inline-block;
      min-width: 60px;
    }}
    .offset-cell {{
      color: var(--muted);
    }}
    .instruction-cell {{
      min-width: 0;
      word-break: break-word;
    }}
    @media (max-width: 1100px) {{
      .code-compare {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <h1>双平台整合报告</h1>
    <div class="meta">
      <span>基准: {_escape(benchmark)}</span>
      <span>基线平台: {_escape(baseline_platform)}</span>
      <span>对比平台: {_escape(target_platform)}</span>
    </div>

    <section class="section-card">
      <h2>热点对比</h2>
      {_render_visual_block("DSO 差异", visuals_dir / "shared_object_compare.svg")}
      {_render_html_table(object_rows, [
          ("shared_object", "Shared Object"),
          ("baseline_est_time", f"{baseline_platform} Time (s)"),
          ("target_est_time", f"{target_platform} Time (s)"),
          ("delta_time", "Delta Time (s)"),
          ("delta_note", "Note"),
      ])}

      {_render_visual_block("分类差异", visuals_dir / "category_compare.svg")}
      {_render_html_table(category_rows, [
          ("category_top", "Category"),
          ("baseline_share", f"{baseline_platform}%"),
          ("target_share", f"{target_platform}%"),
          ("baseline_est_time", f"{baseline_platform} Time (s)"),
          ("target_est_time", f"{target_platform} Time (s)"),
          ("delta_time", "Delta Time (s)"),
          ("delta_note", "Note"),
      ])}

      {_render_visual_block("热点函数差异", visuals_dir / "symbol_delta.svg")}
      {_render_html_table(symbol_rows, [
          ("category_top", "Category"),
          ("symbol", "Symbol"),
          ("baseline_est_time", f"{baseline_platform} Time (s)"),
          ("target_est_time", f"{target_platform} Time (s)"),
          ("delta_time", "Delta Time (s)"),
          ("delta_note", "Note"),
      ])}
    </section>

    <section class="section-card">
      <h2>差异分析</h2>
      {machine_compare_html}
    </section>
  </main>
</body>
</html>
"""


def write_report_bundle(
    output_path: Path,
    *,
    input_dir: Path,
    top_n: int,
    baseline_root: Path | None = None,
    target_root: Path | None = None,
    visuals_dir: Path | None = None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_report(
            input_dir,
            top_n=top_n,
            baseline_root=baseline_root,
            target_root=target_root,
            visuals_dir=visuals_dir,
        ),
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    text = render_report(
        args.input_dir,
        top_n=args.top_n,
        baseline_root=args.baseline_root,
        target_root=args.target_root,
        visuals_dir=args.visuals_dir,
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
