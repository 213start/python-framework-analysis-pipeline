#!/usr/bin/env python3
"""共享的 perf 归一化、分类和聚合逻辑。"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


NORMALIZED_FIELDS = [
    "platform_id",
    "arch",
    "python_version",
    "build_id",
    "benchmark",
    "event",
    "children",
    "self",
    "period",
    "pid",
    "command",
    "pid_command",
    "shared_object",
    "symbol",
    "ip",
    "category_top",
    "category_sub",
    "category_reason",
    "source_report",
    "sample_count",
    "instruction_text",
    "instruction_offset",
    "instruction_share",
]


@dataclass(frozen=True)
class Rule:
    name: str
    category_top: str
    category_sub: str
    match: dict[str, list[str]]


def load_rules(path: Path) -> list[Rule]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rules = []
    for item in payload["rules"]:
        rules.append(
            Rule(
                name=item["name"],
                category_top=item["category_top"],
                category_sub=item.get("category_sub", ""),
                match=item.get("match", {}),
            )
        )
    return rules


def _value_for_field(record: dict[str, str], field: str) -> str:
    mapping = {
        "symbol": record.get("symbol", ""),
        "shared_object": record.get("shared_object", ""),
    }
    return mapping[field]


def _first_positive_reason(field: str, kind: str, patterns: list[str], value: str) -> str | None:
    for pattern in patterns:
        if kind == "exact" and value == pattern:
            return f"{field}_exact:{pattern}"
        if kind == "startswith" and value.startswith(pattern):
            return f"{field}_startswith:{pattern}"
        if kind == "contains" and pattern.lower() in value.lower():
            return f"{field}_contains:{pattern}"
        if kind == "regex" and re.search(pattern, value):
            return f"{field}_regex:{pattern}"
    return None


def _match_group(record: dict[str, str], rule: Rule) -> str | None:
    reason: str | None = None
    matched_positive = False
    for key, patterns in rule.match.items():
        if not patterns:
            continue
        if key.endswith("_not_regex"):
            field = key[: -len("_not_regex")]
            value = _value_for_field(record, field)
            if any(re.search(pattern, value) for pattern in patterns):
                return None
            continue

        if key.endswith("_exact"):
            field = key[: -len("_exact")]
            kind = "exact"
        elif key.endswith("_startswith"):
            field = key[: -len("_startswith")]
            kind = "startswith"
        elif key.endswith("_prefix"):
            field = key[: -len("_prefix")]
            kind = "startswith"
        elif key.endswith("_contains"):
            field = key[: -len("_contains")]
            kind = "contains"
        elif key.endswith("_regex"):
            field = key[: -len("_regex")]
            kind = "regex"
        else:
            return None

        value = _value_for_field(record, field)
        matched_reason = _first_positive_reason(field, kind, patterns, value)
        if matched_reason is not None:
            matched_positive = True
        if reason is None and matched_reason is not None:
            reason = matched_reason
    if matched_positive:
        return reason or f"rule:{rule.name}"
    return None


def classify_record(record: dict[str, str], rules: list[Rule]) -> tuple[str, str, str]:
    for rule in rules:
        if rule.category_top.startswith("CPython.") and not is_cpython_shared_object(record.get("shared_object", "")):
            continue
        reason = _match_group(record, rule)
        if reason is not None:
            return rule.category_top, rule.category_sub, reason
    return "Unknown", "", "fallback:unknown"


def parse_percent(value: str) -> float:
    raw = value.strip()
    if not raw:
        return 0.0
    if raw.endswith("%"):
        raw = raw[:-1]
    return float(raw)


def parse_number(value: str) -> float:
    raw = value.strip()
    if not raw:
        return 0.0
    return float(raw.replace(",", ""))


def parse_period(value: str) -> int:
    raw = value.strip()
    if not raw:
        return 0
    return int(float(raw.replace(",", "")))


def split_pid_command(value: str) -> tuple[str, str]:
    raw = value.strip()
    if not raw:
        return "", ""
    if ":" not in raw:
        return "", raw
    pid, command = raw.split(":", 1)
    return pid.strip(), command.strip()


def format_float(value: float, digits: int = 6) -> str:
    return f"{value:.{digits}f}".rstrip("0").rstrip(".") if value != 0 else "0"


def clean_symbol_name(symbol: str) -> str:
    cleaned = re.sub(r"^\[[^]]+\]\s*", "", symbol.strip())
    cleaned = re.sub(r"\.(?:lto_priv|constprop|isra)(?:\.\d+)+$", "", cleaned)
    cleaned = re.sub(r"\s+\[clone[^\]]*\]$", "", cleaned)
    return cleaned


def is_cpython_shared_object(shared_object: str) -> bool:
    raw = shared_object.strip()
    if not raw:
        return False
    return bool(re.search(r"(^|/)(python[0-9.]*|libpython[0-9.]+)(?:$|[./-])", raw))


def normalize_shared_object_for_compare(value: str) -> str:
    raw = value.strip().replace("\\", "/").lower()
    if not raw:
        return ""
    normalized = re.sub(
        r"(?i)(^|[/._-])(x86_64|amd64|aarch64|arm64)(?=$|[/._-])",
        r"\1<arch>",
        raw,
    )
    normalized = re.sub(r"/+", "/", normalized)
    normalized = re.sub(r"[_-]{2,}", "-", normalized)
    return normalized


def normalize_raw_row(
    raw: dict[str, str],
    *,
    platform_id: str,
    arch: str,
    python_version: str,
    build_id: str,
    benchmark: str,
    event: str,
    source_report: str,
    rules: list[Rule],
) -> dict[str, str]:
    pid, command = split_pid_command(raw.get("Pid:Command", ""))
    symbol = clean_symbol_name(raw.get("Symbol", ""))
    shared_object = raw.get("Shared Object", "").strip()
    ip = raw.get("IP", "").strip()

    category_top, category_sub, category_reason = classify_record(
        {
            "symbol": symbol,
            "shared_object": shared_object,
        },
        rules,
    )

    return {
        "platform_id": platform_id,
        "arch": arch,
        "python_version": python_version,
        "build_id": build_id,
        "benchmark": benchmark,
        "event": event,
        "children": format_float(parse_percent(raw.get("Children", ""))),
        "self": format_float(parse_percent(raw.get("Self", ""))),
        "period": str(parse_period(raw.get("Period", ""))),
        "pid": pid,
        "command": command,
        "pid_command": raw.get("Pid:Command", "").strip() or command,
        "shared_object": shared_object,
        "symbol": symbol,
        "ip": ip,
        "category_top": category_top,
        "category_sub": category_sub,
        "category_reason": category_reason,
        "source_report": source_report,
        "sample_count": "1",
        "instruction_text": "",
        "instruction_offset": "",
        "instruction_share": "",
    }


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, fieldnames: list[str], rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _first_non_empty(rows: list[dict[str, str]], key: str) -> str:
    for row in rows:
        value = row.get(key, "").strip()
        if value:
            return value
    return ""


def aggregate_rows(
    rows: list[dict[str, str]],
    group_keys: list[str],
    *,
    sort_by: str = "self_share",
) -> list[dict[str, str]]:
    grouped: dict[tuple[str, ...], list[dict[str, str]]] = {}
    for row in rows:
        key = tuple(row.get(group_key, "") for group_key in group_keys)
        grouped.setdefault(key, []).append(row)

    result: list[dict[str, str]] = []
    for key, grouped_rows in grouped.items():
        aggregated = {group_key: key[index] for index, group_key in enumerate(group_keys)}
        aggregated["children_share"] = format_float(sum(parse_number(row.get("children", "0")) for row in grouped_rows))
        aggregated["self_share"] = format_float(sum(parse_number(row.get("self", "0")) for row in grouped_rows))
        aggregated["period_sum"] = str(sum(parse_period(row.get("period", "0")) for row in grouped_rows))
        aggregated["sample_count"] = str(sum(int(parse_number(row.get("sample_count", "0"))) for row in grouped_rows))
        for passthrough_key in (
            "platform_id",
            "benchmark",
            "category_sub",
            "instruction_text",
            "instruction_offset",
            "instruction_share",
        ):
            if passthrough_key not in aggregated:
                aggregated[passthrough_key] = _first_non_empty(grouped_rows, passthrough_key)
        result.append(aggregated)

    result.sort(
        key=lambda item: (
            -parse_number(item.get(sort_by, "0")),
            -parse_number(item.get("self_share", "0")),
            -parse_period(item.get("period_sum", "0")),
        )
    )
    return result


def build_preview(
    rows: list[dict[str, str]],
    scope_keys: list[str],
    label_key: str,
    *,
    top_n: int = 3,
) -> dict[tuple[str, ...], str]:
    aggregated = aggregate_rows(rows, scope_keys + [label_key], sort_by="self_share")
    previews: dict[tuple[str, ...], list[str]] = {}
    for row in aggregated:
        scope = tuple(row.get(key, "") for key in scope_keys)
        bucket = previews.setdefault(scope, [])
        if len(bucket) >= top_n:
            continue
        bucket.append(f"{row.get(label_key, '')} ({row.get('self_share', '0')}%)")
    return {scope: "; ".join(labels) for scope, labels in previews.items()}


def rank_rows(rows: list[dict[str, str]], scope_keys: list[str], rank_field: str) -> list[dict[str, str]]:
    ranked = list(rows)
    counters: dict[tuple[str, ...], int] = {}
    for row in ranked:
        scope = tuple(row.get(key, "") for key in scope_keys)
        counters[scope] = counters.get(scope, 0) + 1
        row[rank_field] = str(counters[scope])
    return ranked


def compare_aggregates(
    baseline_rows: list[dict[str, str]],
    target_rows: list[dict[str, str]],
    group_keys: list[str],
    *,
    baseline_platform: str,
    target_platform: str,
    baseline_e2e_time: float,
    target_e2e_time: float,
    include_target_only: bool = True,
) -> list[dict[str, str]]:
    baseline_agg = aggregate_rows(baseline_rows, group_keys, sort_by="self_share")
    target_agg = aggregate_rows(target_rows, group_keys, sort_by="self_share")

    baseline_map = {tuple(row.get(key, "") for key in group_keys): row for row in baseline_agg}
    target_map = {tuple(row.get(key, "") for key in group_keys): row for row in target_agg}

    ordered_keys = [tuple(row.get(key, "") for key in group_keys) for row in baseline_agg]
    if include_target_only:
        for key in target_map:
            if key not in baseline_map:
                ordered_keys.append(key)

    results: list[dict[str, str]] = []
    for baseline_rank, key in enumerate(ordered_keys, start=1):
        baseline_row = baseline_map.get(key, {})
        target_row = target_map.get(key, {})
        baseline_share = parse_number(baseline_row.get("self_share", "0"))
        target_share = parse_number(target_row.get("self_share", "0"))
        baseline_est = baseline_e2e_time * baseline_share / 100.0
        target_est = target_e2e_time * target_share / 100.0

        row = {group_keys[index]: key[index] for index in range(len(group_keys))}
        row.update(
            {
                "benchmark": baseline_row.get("benchmark", "") or target_row.get("benchmark", ""),
                "baseline_platform": baseline_platform,
                "target_platform": target_platform,
                "baseline_rank": str(baseline_rank),
                "baseline_share": format_float(baseline_share),
                "target_share": format_float(target_share),
                "baseline_e2e_time": format_float(baseline_e2e_time),
                "target_e2e_time": format_float(target_e2e_time),
                "baseline_est_time": format_float(baseline_est),
                "target_est_time": format_float(target_est),
                "delta_time": format_float(baseline_est - target_est),
                "delta_share": format_float(baseline_share - target_share),
            }
        )
        results.append(row)
    return results


def normalize_ip(ip: str) -> str:
    raw = ip.strip().lower()
    if raw.startswith("0x"):
        raw = raw[2:]
    return raw


def render_text_table(
    rows: list[dict[str, str]],
    columns: list[tuple[str, str]],
    *,
    empty_message: str = "(empty)",
) -> str:
    if not rows:
        return empty_message

    widths: dict[str, int] = {}
    for key, title in columns:
        widths[key] = len(title)
    for row in rows:
        for key, _ in columns:
            widths[key] = max(widths[key], len(str(row.get(key, ""))))

    header = " | ".join(title.ljust(widths[key]) for key, title in columns)
    divider = "-+-".join("-" * widths[key] for key, _ in columns)
    body = [
        " | ".join(str(row.get(key, "")).ljust(widths[key]) for key, _ in columns)
        for row in rows
    ]
    return "\n".join([header, divider, *body])


def render_markdown_table(
    rows: list[dict[str, str]],
    columns: list[tuple[str, str]],
    *,
    empty_message: str = "_No data_",
) -> str:
    if not rows:
        return empty_message

    header = "| " + " | ".join(title for _, title in columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    body = [
        "| " + " | ".join(str(row.get(key, "")).replace("\n", " ") for key, _ in columns) + " |"
        for row in rows
    ]
    return "\n".join([header, divider, *body])
