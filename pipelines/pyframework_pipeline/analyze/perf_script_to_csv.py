#!/usr/bin/env python3
"""将 perf script 输出转换为逐地址采样 CSV。"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import shutil
import subprocess
import sys
from pathlib import Path


LOGGER = logging.getLogger("perf_script_to_csv")
CSV_FIELDS = ["Period", "Pid:Command", "IP", "Symbol", "Shared Object"]


class PerfScriptConversionError(RuntimeError):
    """perf script 转换错误。"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="perf.data 文件路径")
    parser.add_argument("-o", "--output", type=Path, help="输出 CSV 文件路径；不提供时输出到 stdout")
    parser.add_argument("-p", "--perf-bin", default="perf", help="perf 可执行文件路径")
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="日志级别",
    )
    parser.add_argument(
        "-s",
        "--perf-script-arg",
        action="append",
        default=[],
        help="额外透传给 perf script 的参数，可重复传入",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    )


def resolve_perf_binary(perf_bin: str) -> str:
    if Path(perf_bin).is_file():
        return perf_bin
    resolved = shutil.which(perf_bin)
    if resolved:
        return resolved
    raise PerfScriptConversionError("未找到 perf 可执行文件。")


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    LOGGER.debug("执行命令: %s", " ".join(command))
    try:
        return subprocess.run(command, check=False, capture_output=True, text=True, encoding="utf-8")
    except OSError as exc:
        raise PerfScriptConversionError(f"执行命令失败: {' '.join(command)} | 错误: {exc}") from exc


def normalize_symbol(symbol: str) -> str:
    return re.sub(r"^\[[^]]+\]\s*", "", symbol.strip())


def parse_script_text(script_text: str) -> list[dict[str, str]]:
    patterns = [
        re.compile(
            r"(?P<ip>0x[0-9a-fA-F]{4,}|[0-9a-fA-F]{6,})\s+"
            r"(?P<symbol>.+?)\s+"
            r"\((?P<dso>[^)]+)\)"
            r"(?:\s+(?P<period>\d+(?:\.\d+)?))?\s*$"
        ),
        re.compile(
            r"(?P<ip>0x[0-9a-fA-F]{4,}|[0-9a-fA-F]{6,})\s+"
            r"(?P<symbol>.+?)\s+"
            r"(?P<dso>(/[^ ]+|\[[^]]+\]|[^ ]+\.so(?:\.[^ ]*)?|[^ ]+python[^ ]*))"
            r"(?:\s+(?P<period>\d+(?:\.\d+)?))?\s*$"
        ),
    ]
    rows: list[dict[str, str]] = []
    for line_number, line in enumerate(script_text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        matched = None
        for pattern in patterns:
            matched = pattern.search(stripped)
            if matched is not None:
                break
        if matched is None:
            LOGGER.debug("perf script 第 %s 行不匹配地址模式，跳过: %r", line_number, line)
            continue

        prefix = stripped[: matched.start()].strip()
        prefix_parts = prefix.split()
        comm = prefix_parts[0] if prefix_parts else ""
        pid = ""
        for token in prefix_parts[1:]:
            pid_match = re.search(r"\b(\d+)(?:/\d+)?\b", token)
            if pid_match:
                pid = pid_match.group(1)
                break

        rows.append(
            {
                "Period": matched.group("period") or "1",
                "Pid:Command": f"{pid}:{comm}" if pid and comm else comm,
                "IP": matched.group("ip").strip(),
                "Symbol": normalize_symbol(matched.group("symbol")),
                "Shared Object": matched.group("dso").strip(),
            }
        )

    if not rows:
        raise PerfScriptConversionError("perf script 执行成功，但未解析出任何逐地址采样行。")
    return rows


def write_csv(rows: list[dict[str, str]], output_path: Path | None) -> None:
    if output_path is None:
        writer = csv.DictWriter(sys.stdout, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    perf_bin = resolve_perf_binary(args.perf_bin)
    command = [
        perf_bin,
        "script",
        "-i",
        str(args.input),
        "-F",
        "comm,pid,ip,sym,dso,period",
        *args.perf_script_arg,
    ]
    completed = run_command(command)
    if completed.returncode != 0:
        raise PerfScriptConversionError(
            f"perf script 执行失败: {' '.join(command)} | exit={completed.returncode} | stderr={completed.stderr.strip()}"
        )
    rows = parse_script_text(completed.stdout)
    write_csv(rows, args.output)
    LOGGER.info("perf script 地址 CSV 生成完成: rows=%s", len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
