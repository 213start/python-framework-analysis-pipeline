#!/usr/bin/env python3
"""对单平台热点函数补充 perf annotate 机器码注解。"""

from __future__ import annotations

import argparse
import logging
import re
import shutil
import subprocess
from pathlib import Path

try:
    from .perf_analysis_common import (
        NORMALIZED_FIELDS,
        aggregate_rows,
        clean_symbol_name,
        normalize_ip,
        parse_number,
        read_csv_rows,
        write_csv_rows,
    )
except ImportError:
    from perf_analysis_common import (
        NORMALIZED_FIELDS,
        aggregate_rows,
        clean_symbol_name,
        normalize_ip,
        parse_number,
        read_csv_rows,
        write_csv_rows,
    )


LOGGER = logging.getLogger("annotate_perf_hotspots")

INSTRUCTION_FIELDS = [
    "platform_id",
    "benchmark",
    "category_top",
    "shared_object",
    "symbol",
    "segment_id",
    "line_index",
    "ip",
    "instruction_offset",
    "instruction_share",
    "instruction_text",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="输入平台记录 CSV")
    parser.add_argument("-d", "--perf-data", type=Path, required=True, help="perf.data 路径")
    parser.add_argument("-o", "--output-dir", type=Path, required=True, help="输出目录")
    parser.add_argument("-p", "--perf-bin", default="perf", help="perf 可执行文件路径")
    parser.add_argument("-n", "--top-n", type=int, default=20, help="选取前 N 个热点函数做注解")
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="日志级别",
    )
    return parser.parse_args()


def resolve_perf_binary(perf_bin: str) -> str:
    if Path(perf_bin).is_file():
        return perf_bin
    resolved = shutil.which(perf_bin)
    if resolved:
        return resolved
    raise RuntimeError("未找到 perf 可执行文件，无法执行 perf annotate。")


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    LOGGER.debug("执行命令: %s", " ".join(command))
    return subprocess.run(command, check=False, capture_output=True, text=True, encoding="utf-8")


def build_annotate_commands(perf_bin: str, perf_data: Path, symbol: str, shared_object: str) -> list[list[str]]:
    clean_symbol = clean_symbol_name(symbol)
    base = [perf_bin, "annotate", "--stdio", "-i", str(perf_data), "--symbol", clean_symbol]
    commands = []
    if shared_object:
        commands.append(base + ["--dsos", shared_object])
    commands.append(base)
    return commands


def parse_annotate_text(
    text: str,
    *,
    platform_id: str,
    benchmark: str,
    category_top: str,
    shared_object: str,
    symbol: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    parsed_lines: list[tuple[int, int, float, str, str]] = []
    segment_id = 0
    line_index = 0
    saw_instruction = False
    pending_new_segment = False
    for line in text.splitlines():
        match = re.match(
            r"^\s*(?P<share>\d+(?:\.\d+)?)\s*:\s*(?P<ip>0x[0-9a-fA-F]+|[0-9a-fA-F]+):\s*(?P<inst>.+?)\s*$",
            line,
        )
        if not match:
            if saw_instruction:
                pending_new_segment = True
            continue
        if pending_new_segment:
            segment_id += 1
            pending_new_segment = False
        saw_instruction = True
        line_index += 1
        parsed_lines.append(
            (
                segment_id,
                line_index,
                float(match.group("share")),
                normalize_ip(match.group("ip")),
                match.group("inst").strip(),
            )
        )

    if not parsed_lines:
        return []

    base_ip = min(int(ip, 16) for _, _, _, ip, _ in parsed_lines)
    for current_segment_id, current_line_index, share, ip, instruction_text in parsed_lines:
        offset = int(ip, 16) - base_ip
        rows.append(
            {
                "platform_id": platform_id,
                "benchmark": benchmark,
                "category_top": category_top,
                "shared_object": shared_object,
                "symbol": symbol,
                "segment_id": str(current_segment_id),
                "line_index": str(current_line_index),
                "ip": ip,
                "instruction_offset": hex(offset),
                "instruction_share": f"{share:g}",
                "instruction_text": instruction_text,
            }
        )
    return rows


def annotate_symbol(
    perf_bin: str,
    perf_data: Path,
    *,
    platform_id: str,
    benchmark: str,
    category_top: str,
    shared_object: str,
    symbol: str,
) -> list[dict[str, str]]:
    errors: list[str] = []
    for command in build_annotate_commands(perf_bin, perf_data, symbol, shared_object):
        completed = run_command(command)
        if completed.returncode != 0:
            errors.append(f"exit={completed.returncode} stderr={completed.stderr.strip()} cmd={' '.join(command)}")
            continue
        rows = parse_annotate_text(
            completed.stdout,
            platform_id=platform_id,
            benchmark=benchmark,
            category_top=category_top,
            shared_object=shared_object,
            symbol=symbol,
        )
        if rows:
            return rows
        errors.append(f"未解析出机器码行 cmd={' '.join(command)}")

    LOGGER.warning("无法注解符号 %s: %s", symbol, " | ".join(errors))
    return []


def merge_instruction_rows(
    record_rows: list[dict[str, str]],
    instruction_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    instruction_map = {
        (
            row.get("shared_object", ""),
            row.get("symbol", ""),
            normalize_ip(row.get("ip", "")),
        ): row
        for row in instruction_rows
    }

    enriched = []
    for row in record_rows:
        key = (
            row.get("shared_object", ""),
            row.get("symbol", ""),
            normalize_ip(row.get("ip", "")),
        )
        annotation = instruction_map.get(key)
        new_row = dict(row)
        if annotation:
            new_row["instruction_text"] = annotation.get("instruction_text", "")
            new_row["instruction_offset"] = annotation.get("instruction_offset", "")
            new_row["instruction_share"] = annotation.get("instruction_share", "")
        enriched.append(new_row)
    return enriched


def select_hot_symbols(rows: list[dict[str, str]], top_n: int) -> list[dict[str, str]]:
    aggregated = aggregate_rows(rows, ["platform_id", "benchmark", "category_top", "shared_object", "symbol"])
    return aggregated[:top_n]


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    )

    perf_bin = resolve_perf_binary(args.perf_bin)
    record_rows = read_csv_rows(args.input)
    hot_symbols = select_hot_symbols(record_rows, args.top_n)

    instruction_rows: list[dict[str, str]] = []
    for symbol_row in hot_symbols:
        instruction_rows.extend(
            annotate_symbol(
                perf_bin,
                args.perf_data,
                platform_id=symbol_row.get("platform_id", ""),
                benchmark=symbol_row.get("benchmark", ""),
                category_top=symbol_row.get("category_top", ""),
                shared_object=symbol_row.get("shared_object", ""),
                symbol=symbol_row.get("symbol", ""),
            )
        )

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    enriched_rows = merge_instruction_rows(record_rows, instruction_rows)
    write_csv_rows(output_dir / "records_enriched.csv", NORMALIZED_FIELDS, enriched_rows)
    write_csv_rows(output_dir / "instruction_hotspots.csv", INSTRUCTION_FIELDS, instruction_rows)

    LOGGER.info(
        "机器码注解完成: symbols=%s instructions=%s output_dir=%s",
        len(hot_symbols),
        len(instruction_rows),
        output_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
