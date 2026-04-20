"""Load and parse environment.yaml configuration files.

The YAML subset used by environment.yaml is richer than project.yaml, so this
module provides a structured parser instead of the simple key-value parser in
config.py.

Supported YAML features:
  - key: value (scalar)
  - key: (block mapping follows)
  - - item (sequence entries, each item is a mapping)
  - Nested indentation (2-space)

Not supported:
  - Flow syntax ({}, [])
  - Multiline strings (|, >)
  - Anchors/aliases (&, *)
  - Quoted strings with embedded colons
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


def load_environment_yaml(path: Path) -> dict[str, Any]:
    """Parse an environment.yaml file into a nested dict."""
    text = path.read_text(encoding="utf-8")
    return _parse_yaml(text)


def parse_yaml(text: str) -> dict[str, Any]:
    """Parse a YAML string into a nested dict (public API)."""
    return _parse_yaml(text)


def _parse_yaml(text: str) -> dict[str, Any]:
    lines = text.splitlines()
    result: dict[str, Any] = {}
    _parse_mapping(lines, 0, len(lines), 0, result)
    return result


def _get_indent(line: str) -> int:
    """Return the number of leading spaces."""
    return len(line) - len(line.lstrip(" "))


def _parse_mapping(lines: list[str], start: int, end: int, min_indent: int, target: dict[str, Any]) -> int:
    """Parse key: value lines into *target*. Returns next unprocessed line."""
    i = start
    while i < end:
        stripped = lines[i].rstrip()
        if not stripped or stripped.lstrip().startswith("#"):
            i += 1
            continue

        indent = _get_indent(lines[i])
        if indent < min_indent:
            break

        content = stripped.strip()

        # Sequence entry at this indent means we've left the mapping
        if content.startswith("- "):
            break

        colon_pos = content.find(":")
        if colon_pos < 0:
            i += 1
            continue

        key = content[:colon_pos].strip()
        value_part = content[colon_pos + 1:].strip()

        if value_part:
            # Inline scalar value
            target[key] = _parse_scalar(value_part)
            i += 1
        else:
            # Value is a block — either a sub-mapping or a sequence
            i += 1
            if i < end:
                next_stripped = lines[i].rstrip()
                next_content = next_stripped.strip()
                if next_content.startswith("- "):
                    seq: list[Any] = []
                    i = _parse_sequence(lines, i, end, indent + 2, seq)
                    target[key] = seq
                else:
                    nested: dict[str, Any] = {}
                    i = _parse_mapping(lines, i, end, indent + 2, nested)
                    target[key] = nested if nested else {}
            else:
                target[key] = {}

    return i


def _parse_sequence(lines: list[str], start: int, end: int, min_indent: int, target: list[Any]) -> int:
    """Parse ``- item`` lines into *target*.

    Each sequence item is expected to be a mapping whose first key is on the
    same line as the dash.  Sibling keys at indent + 2 belong to the same item.
    """
    i = start
    while i < end:
        stripped = lines[i].rstrip()
        if not stripped or stripped.lstrip().startswith("#"):
            i += 1
            continue

        indent = _get_indent(lines[i])
        if indent < min_indent:
            break

        content = stripped.strip()
        if not content.startswith("- "):
            break

        after_dash = content[2:].strip()

        colon_pos = after_dash.find(":")
        if colon_pos >= 0:
            item: dict[str, Any] = {}
            first_key = after_dash[:colon_pos].strip()
            first_value = after_dash[colon_pos + 1:].strip()

            if first_value:
                item[first_key] = _parse_scalar(first_value)
            else:
                # First key's value is a block starting on the next line
                # Peek at next line to determine mapping vs sequence
                if i + 1 < end:
                    next_stripped = lines[i + 1].rstrip()
                    next_content = next_stripped.strip()
                    next_indent = _get_indent(lines[i + 1])
                    if next_content.startswith("- ") and next_indent >= indent + 2:
                        seq: list[Any] = []
                        i = _parse_sequence(lines, i + 1, end, indent + 2, seq)
                        item[first_key] = seq
                        target.append(item)
                        continue
                    else:
                        nested: dict[str, Any] = {}
                        i = _parse_mapping(lines, i + 1, end, indent + 2, nested)
                        item[first_key] = nested if nested else {}
                        target.append(item)
                        continue
                else:
                    item[first_key] = {}

            # Collect sibling keys at indent + 2
            i += 1
            i = _parse_mapping(lines, i, end, indent + 2, item)
            target.append(item)
        else:
            target.append(_parse_scalar(after_dash))
            i += 1

    return i


def _parse_scalar(value: str) -> str | int | float | bool:
    # Strip inline comments (e.g. "perf  # description" -> "perf")
    # Only strip if '#' is preceded by whitespace (not inside a quoted string)
    if "#" in value and not (value.startswith('"') or value.startswith("'")):
        # Find first '#' preceded by whitespace
        for idx in range(1, len(value)):
            if value[idx] == "#" and value[idx - 1] in (" ", "\t"):
                value = value[:idx].rstrip()
                break

    # Strip quotes
    if len(value) >= 2:
        if (value[0] == '"' and value[-1] == '"') or (value[0] == "'" and value[-1] == "'"):
            return value[1:-1]
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value
