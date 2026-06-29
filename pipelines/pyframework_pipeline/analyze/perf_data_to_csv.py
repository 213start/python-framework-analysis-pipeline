#!/usr/bin/env python3
"""将 perf.data 转换为包含固定字段的 CSV。"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


LOGGER = logging.getLogger("perf_data_to_csv")
CSV_FIELDS = [
    "Children",
    "Self",
    "Period",
    "Pid:Command",
    "IP",
    "Symbol",
    "Shared Object",
]
STRICT_REQUIRED_FIELDS = [
    "Children",
    "Self",
    "Period",
    "Symbol",
    "Shared Object",
]


class PerfConversionError(RuntimeError):
    """可定位、可向上层展示的转换错误。"""


@dataclass(frozen=True)
class ParsedReport:
    rows: list[dict[str, str]]
    source_command: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="perf.data 文件路径")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="输出 CSV 文件路径；不提供时输出到标准输出",
    )
    parser.add_argument(
        "-p",
        "--perf-bin",
        default="perf",
        help="perf 可执行文件路径，默认使用 PATH 中的 perf",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="日志级别，默认 INFO",
    )
    parser.add_argument(
        "-e",
        "--encoding",
        default="utf-8",
        help="读取 perf 文本输出时使用的编码，默认 utf-8",
    )
    parser.add_argument(
        "-r",
        "--perf-report-arg",
        action="append",
        default=[],
        help="额外透传给 perf report 的参数，可重复传入",
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


def ensure_paths(args: argparse.Namespace) -> None:
    if not args.input.exists():
        raise PerfConversionError(f"输入文件不存在: {args.input}")
    if not args.input.is_file():
        raise PerfConversionError(f"输入路径不是文件: {args.input}")
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)


def resolve_perf_binary(perf_bin: str) -> str:
    if Path(perf_bin).is_file():
        return perf_bin
    resolved = shutil.which(perf_bin)
    if resolved:
        return resolved
    raise PerfConversionError(
        "未找到 perf 可执行文件。请确认 perf 已安装，或通过 --perf-bin 指定路径。"
    )


def run_command(command: list[str], encoding: str) -> subprocess.CompletedProcess[str]:
    LOGGER.debug("执行命令: %s", " ".join(command))
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            encoding=encoding,
        )
    except OSError as exc:
        raise PerfConversionError(
            f"执行命令失败: {' '.join(command)}; 错误: {exc}"
        ) from exc
    LOGGER.debug("命令退出码: %s", completed.returncode)
    if completed.stderr.strip():
        LOGGER.debug("命令 stderr:\n%s", completed.stderr.strip())
    return completed


def build_candidate_commands(
    perf_bin: str,
    input_path: Path,
    extra_args: Iterable[str],
) -> list[list[str]]:
    base = [
        perf_bin,
        "report",
        "--stdio",
        "--percent-limit",
        "0",
        "--show-total-period",
        "-g",
        "none",
        "-i",
        str(input_path),
    ]
    extra = list(extra_args)
    return [
        base
        + ["-F", "overhead_children,overhead,period,pid,comm,ip,symbol,dso"]
        + extra,
        base + extra,
    ]


def normalize_header_name(name: str) -> str:
    normalized = " ".join(name.strip().lower().replace(":", " : ").split())
    mapping = {
        "children": "Children",
        "overhead children": "Children",
        "children overhead": "Children",
        "self": "Self",
        "overhead": "Self",
        "period": "Period",
        "pid : command": "Pid:Command",
        "pid:command": "Pid:Command",
        "pid : comm": "Pid:Command",
        "pid:comm": "Pid:Command",
        "pid": "Pid",
        "command": "Command",
        "comm": "Command",
        "ip": "IP",
        "symbol": "Symbol",
        "shared object": "Shared Object",
        "dso": "Shared Object",
    }
    return mapping.get(normalized, name.strip())


def split_perf_columns(line: str) -> list[str]:
    stripped = strip_perf_comment_prefix(line).strip()
    if not stripped:
        return []
    return [part.strip() for part in re.split(r"\s{2,}", stripped) if part.strip()]


def build_raw_row(headers: list[str], line: str) -> dict[str, str]:
    values = split_perf_columns(line)
    if not values:
        return {}
    if len(values) < len(headers):
        return {header: values[index] if index < len(values) else "" for index, header in enumerate(headers)}
    if len(values) == len(headers):
        return dict(zip(headers, values))

    merged = values[: len(headers) - 1]
    merged.append("  ".join(values[len(headers) - 1 :]))
    return dict(zip(headers, merged))


def maybe_build_row(raw: dict[str, str]) -> dict[str, str] | None:
    if not raw:
        return None

    pid_command = raw.get("Pid:Command", "")
    if not pid_command:
        pid = raw.get("Pid", "").strip()
        command = raw.get("Command", "").strip()
        if pid and command:
            pid_command = f"{pid}:{command}"
        elif command:
            pid_command = command

    symbol = raw.get("Symbol", "").strip()
    shared_object = raw.get("Shared Object", "").strip()
    if symbol and not shared_object:
        match = re.match(r"^(?P<symbol>.+?)\s+(?P<dso>(/.*|\[.*\]|[^ ]+\.so(?:\.[^ ]*)?))$", symbol)
        if match:
            symbol = match.group("symbol").strip()
            shared_object = match.group("dso").strip()

    row = {
        "Children": raw.get("Children", "").strip(),
        "Self": raw.get("Self", "").strip(),
        "Period": raw.get("Period", "").strip(),
        "Pid:Command": pid_command,
        "IP": raw.get("IP", "").strip(),
        "Symbol": symbol,
        "Shared Object": shared_object,
    }
    if not any(row.values()):
        return None
    return row


def is_separator_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    return all(char in "-=#." for char in stripped)


def strip_perf_comment_prefix(line: str) -> str:
    return re.sub(r"^(\s*)#", r"\1 ", line, count=1)


def parse_report_text(report_text: str) -> list[dict[str, str]]:
    lines = report_text.splitlines()
    header_index = None
    headers: list[str] = []

    for index, line in enumerate(lines):
        normalized_line = strip_perf_comment_prefix(line)
        if "Children" in normalized_line and "Self" in normalized_line and "Symbol" in normalized_line:
            split_headers = [normalize_header_name(name) for name in split_perf_columns(normalized_line)]
            columns = set(split_headers)
            if {"Children", "Self", "Symbol"} <= columns:
                header_index = index
                headers = split_headers
                break

    if header_index is None:
        raise PerfConversionError("未在 perf report 输出中找到表头，无法解析。")

    LOGGER.debug("识别到表头列: %s", headers)
    missing_headers = [field for field in CSV_FIELDS if field not in headers]
    if missing_headers:
        LOGGER.warning(
            "perf report 表头缺少目标列 %s，将在可行时回填，否则以空值写入 CSV",
            missing_headers,
        )
    rows: list[dict[str, str]] = []
    for line_number, line in enumerate(lines[header_index + 1 :], start=header_index + 2):
        if not line.strip():
            continue
        if line.lstrip().startswith("#") or is_separator_line(line):
            continue

        raw = build_raw_row(headers, line)
        row = maybe_build_row(raw)
        if row is None:
            LOGGER.debug("跳过空行或无法识别的行 %s: %r", line_number, line)
            continue

        missing = [field for field in STRICT_REQUIRED_FIELDS if not row[field]]
        if missing:
            raise PerfConversionError(
                f"第 {line_number} 行缺少必需字段 {missing}: {line.rstrip()}"
            )
        rows.append(row)

    if not rows:
        raise PerfConversionError("表头已识别，但未解析出任何数据行。")
    return rows


def normalize_symbol_for_match(symbol: str) -> str:
    return re.sub(r"^\[[^]]+\]\s*", "", symbol.strip())


def parse_perf_script_text(script_text: str) -> dict[tuple[str, str, str], tuple[str, int]]:
    ip_hints: dict[tuple[str, str, str], tuple[str, int]] = {}
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
            LOGGER.debug("perf script 第 %s 行不匹配 IP/Symbol/DSO 模式，跳过: %r", line_number, line)
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

        ip = matched.group("ip")
        symbol = matched.group("symbol").strip()
        dso = matched.group("dso").strip()
        period_token = matched.group("period") or "0"

        pid_command = f"{pid}:{comm}" if pid and comm else comm
        keys = [
            (pid_command, normalize_symbol_for_match(symbol), dso),
            (comm, normalize_symbol_for_match(symbol), dso),
            ("", normalize_symbol_for_match(symbol), dso),
        ]
        period = int(float(period_token))
        for key in keys:
            previous = ip_hints.get(key)
            if previous is None or period > previous[1]:
                ip_hints[key] = (ip, period)
    return ip_hints


def load_ip_hints_from_perf_script(
    perf_bin: str,
    input_path: Path,
    encoding: str,
    extra_args: Iterable[str],
) -> dict[tuple[str, str, str], tuple[str, int]]:
    command = [
        perf_bin,
        "script",
        "-i",
        str(input_path),
        "-F",
        "comm,pid,ip,sym,dso,period",
        *list(extra_args),
    ]
    completed = run_command(command, encoding=encoding)
    if completed.returncode != 0:
        raise PerfConversionError(
            f"perf script 执行失败: {' '.join(command)} | exit={completed.returncode} | stderr={completed.stderr.strip()}"
        )
    hints = parse_perf_script_text(completed.stdout)
    if not hints:
        raise PerfConversionError("perf script 执行成功，但未解析出任何 IP 提示。")
    LOGGER.info("通过 perf script 解析到 %s 条 IP 提示", len(hints))
    return hints


def fill_missing_ips(
    rows: list[dict[str, str]],
    ip_hints: dict[tuple[str, str, str], tuple[str, int]],
) -> list[dict[str, str]]:
    filled = 0
    result: list[dict[str, str]] = []
    for row in rows:
        new_row = dict(row)
        if not new_row.get("IP", "").strip():
            pid_command = new_row.get("Pid:Command", "").strip()
            symbol = normalize_symbol_for_match(new_row.get("Symbol", ""))
            dso = new_row.get("Shared Object", "").strip()
            command_only = pid_command.split(":", 1)[1] if ":" in pid_command else pid_command
            hint = None
            for key in (
                (pid_command, symbol, dso),
                (command_only, symbol, dso),
                ("", symbol, dso),
            ):
                hint = ip_hints.get(key)
                if hint is not None:
                    break
            if hint is not None:
                new_row["IP"] = hint[0]
                filled += 1
        result.append(new_row)
    if filled:
        LOGGER.info("已通过 perf script 回填 %s 条缺失 IP", filled)
    else:
        LOGGER.warning("尝试通过 perf script 回填 IP，但没有命中任何行")
    return result


def load_rows_from_perf_report(
    perf_bin: str,
    input_path: Path,
    encoding: str,
    extra_args: Iterable[str],
) -> ParsedReport:
    attempt_errors: list[str] = []
    for command in build_candidate_commands(perf_bin, input_path, extra_args):
        completed = run_command(command, encoding=encoding)
        if completed.returncode != 0:
            attempt_errors.append(
                f"命令失败: {' '.join(command)} | exit={completed.returncode} | stderr={completed.stderr.strip()}"
            )
            continue
        try:
            rows = parse_report_text(completed.stdout)
            return ParsedReport(rows=rows, source_command=command)
        except PerfConversionError as exc:
            attempt_errors.append(
                f"命令输出无法解析: {' '.join(command)} | 原因={exc}"
            )

    joined = "\n".join(attempt_errors) if attempt_errors else "无可用尝试记录"
    raise PerfConversionError(
        "无法从 perf.data 生成目标 CSV。\n"
        "已尝试的 perf report 命令如下：\n"
        f"{joined}\n"
        "如果你的 perf 版本输出格式不同，请通过 --perf-report-arg 追加兼容参数后重试。"
    )


def write_csv(rows: list[dict[str, str]], output_path: Path | None) -> None:
    if output_path is None:
        writer = csv.DictWriter(sys.stdout, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
        return

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    try:
        ensure_paths(args)
        perf_bin = resolve_perf_binary(args.perf_bin)
        parsed = load_rows_from_perf_report(
            perf_bin=perf_bin,
            input_path=args.input,
            encoding=args.encoding,
            extra_args=args.perf_report_arg,
        )
        rows = parsed.rows
        if any(not row.get("IP", "").strip() for row in rows):
            ip_hints = load_ip_hints_from_perf_script(
                perf_bin=perf_bin,
                input_path=args.input,
                encoding=args.encoding,
                extra_args=args.perf_script_arg,
            )
            rows = fill_missing_ips(rows, ip_hints)
        write_csv(rows, args.output)
        LOGGER.info(
            "转换完成: rows=%s, input=%s, output=%s, command=%s",
            len(rows),
            args.input,
            args.output or "<stdout>",
            " ".join(parsed.source_command),
        )
        return 0
    except PerfConversionError as exc:
        LOGGER.error("转换失败: %s", exc)
        return 1
    except Exception:
        LOGGER.exception("发生未预期异常")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
