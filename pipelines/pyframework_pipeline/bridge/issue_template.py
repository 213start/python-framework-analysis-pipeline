"""Issue body generation for ASM diff analysis.

Generates the prompt + source + machine code body for each hotspot function.
Handles chunking for long functions and single-platform degradation.

The template follows the real-world pattern from
https://github.com/sisibeloved/pytorch/discussions/2 -- the prompt instructs
the LLM to produce a structured analysis with 分类、行为模式、总览、分段详情、
根因汇总（含 perf stat/PMU 证据链）、优化机会 and 优化策略.
"""

from __future__ import annotations

from typing import Any


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_COMMENT_PREFIX_DUAL = "## 跨平台机器码差异分析："
_COMMENT_PREFIX_ARM = "## Kunpeng 机器码分析："
_COMMENT_PREFIX_X86 = "## Zen4 机器码分析："

_PLATFORM_LABEL_ARM = "Kunpeng"
_PLATFORM_LABEL_X86 = "Zen4"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_lines(text: str) -> int:
    """Count the number of non-empty lines in *text*."""
    return sum(1 for line in text.splitlines() if line.strip())


def _truncate_asm(asm: str, max_lines: int) -> str:
    """Truncate assembly text to *max_lines* non-empty lines.

    Returns the (possibly truncated) text. When truncation occurs a footer
    line is appended indicating the total line count and how many were shown.
    """
    lines = asm.splitlines()
    total = len(lines)
    if total <= max_lines:
        return asm

    shown = lines[:max_lines]
    shown.append(
        f"; [截断: 共{total}行，已展示前{max_lines}行]"
    )
    return "\n".join(shown)


def _resolve_component_display(component: str) -> str:
    """Map internal component id to a human-readable display name."""
    _map: dict[str, str] = {
        "cpython": "CPython",
        "glibc": "glibc",
        "kernel": "Kernel",
        "third_party": "Third Party",
        "bridge_runtime": "Bridge Runtime",
    }
    return _map.get(component, component or "Unknown")


def _resolve_category_display(category_l1: str) -> str:
    """Map internal L1 category id to a human-readable display name."""
    _map: dict[str, str] = {
        "interpreter": "Interpreter",
        "memory": "Memory",
        "gc": "GC",
        "object_model": "Object Model",
        "type_operations": "Type Operations",
        "calls_dispatch": "Calls / Dispatch",
        "native_boundary": "Native Boundary",
        "kernel": "Kernel",
        "unknown": "Unknown",
    }
    return _map.get(category_l1, category_l1 or "Unknown")


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

def _build_dual_prompt(symbol: str, framework: str) -> str:
    """Build the prompt section for dual-platform (ARM + x86) analysis."""
    return (
        f"你是一个CPU微架构性能优化专家和Python软件专家，现在正在进行"
        f"{framework}在Kunpeng（ARM）和AMD Zen（x86）上的性能差异分析。"
        f"阅读本Discussion并在本Discussion底下评论。根据两个平台的机器码和对应源码"
        f"分析Arm相对x86性能相对劣势的根因，把整个函数对应源码的机器码差异、"
        f"根因汇总、优化机会和优化策略都列出来。\n"
        f"\n"
        f"差异分析应该能完整覆盖整个函数对应源码，保留源码、两个平台的机器码；"
        f"根因汇总中，需要给出推理证据链所需的perf stat/PMU数据；"
        f"优化机会中，如相同数量、相同意义的指令，x86执行更高效，也可以统计进来；"
        f"结尾的优化策略仅保留Arm收益比x86高的。\n"
        f"\n"
        f"评论格式要求：\n"
        f"- 以 `{_COMMENT_PREFIX_DUAL}{symbol}` 开头\n"
        f"- `### 分类` 节：标注本函数所属组件和L1分类\n"
        f"- `### 行为模式` 节：概述函数的控制流特征、热路径走向、"
        f"分支预测友好度\n"
        f"- `### 总览` 节：表格列出每段源码的ARM/x86指令数和差异概要\n"
        f"- 分段详情节：每段源码配源码原文 + ARM汇编 + x86汇编 + 比较表 + "
        f"ARM劣势说明，完整覆盖整个函数，无差异行注明\"无差异\"\n"
        f"- `### 根因汇总` 节：表格汇总ARM性能劣势来源"
        f"（编号、劣势来源、出现位置、热路径影响、根因类别、perf stat/PMU证据）\n"
        f"- `### 优化机会` 节：表格列出可优化点"
        f"（编号、优化点、ARM现状、x86对应实现、差异说明）\n"
        f"- `### 优化策略` 节：表格列出优化建议"
        f"（编号、优化点、策略、受益方、ARM收益更高的原因、实施方）\n"
        f"- 优化策略仅保留ARM收益比x86高的，注明受益方"
        f"（仅ARM/ARM收益>x86）和实施方"
        f"（CPython/编译器/硬件/OS/Python库/其它自行补充）"
    )


def _build_single_prompt(symbol: str, framework: str, platform: str) -> str:
    """Build the prompt for single-platform analysis."""
    comment_prefix = (
        _COMMENT_PREFIX_ARM if platform == "arm"
        else _COMMENT_PREFIX_X86
    )
    platform_display = (
        _PLATFORM_LABEL_ARM if platform == "arm"
        else _PLATFORM_LABEL_X86
    )
    return (
        f"你是一个CPU微架构性能优化专家和Python软件专家，现在正在进行"
        f"{framework}在{platform_display}上的性能分析。"
        f"阅读本Discussion并在本Discussion底下评论。"
        f"本函数仅在{platform_display}平台出现，请分析该平台机器码的质量和潜在优化点。\n"
        f"\n"
        f"评论格式要求：\n"
        f"- 以 `{comment_prefix}{symbol}` 开头\n"
        f"- `### 分类` 节：标注本函数所属组件和L1分类\n"
        f"- `### 行为模式` 节：概述函数的控制流特征、热路径走向\n"
        f"- `### 总览` 节：列出函数的指令统计和关键特征\n"
        f"- 分段详情节：源码配对应汇编 + 优化说明，完整覆盖整个函数\n"
        f"- `### 根因汇总` 节：表格列出性能瓶颈来源"
        f"（编号、瓶颈来源、出现位置、热路径影响、根因类别、perf stat/PMU证据）\n"
        f"- `### 优化建议` 节：表格列出优化建议"
        f"（编号、优化点、策略、实施方）\n"
        f"- 注明实施方（CPython/编译器/硬件/OS/Python库/其它自行补充）"
    )


# ---------------------------------------------------------------------------
# Body builders
# ---------------------------------------------------------------------------

def _build_source_section(source_code: str | None) -> str:
    """Build the source code markdown section."""
    if not source_code:
        return "## 源码\n\n（无源码）\n"
    return f"## 源码\n\n```c\n{source_code}\n```"


def _build_dual_body(
    symbol: str,
    arm_asm: str,
    x86_asm: str,
    source_code: str | None,
    framework: str,
    binary_path: str,
    component: str,
    category_l1: str,
    max_lines: int,
) -> str:
    """Build the full issue body for a dual-platform function.

    Returns body containing prompt + source + ASM.  If posting fails with 403,
    call :func:`split_asm_from_body` to extract ASM into separate comments.
    """
    prompt = _build_dual_prompt(symbol, framework)
    source_section = _build_source_section(source_code)

    arm_truncated = _truncate_asm(arm_asm, max_lines)
    x86_truncated = _truncate_asm(x86_asm, max_lines)

    return "\n".join([
        f"## 提示词\n\n{prompt}",
        "",
        f"## 组件\n\n- {component}",
        "",
        f"## 分类\n\n- {category_l1}",
        "",
        f"## 环境\n\n- {framework}",
        "",
        source_section,
        "",
        f"## 机器码 — Kunpeng\n\n```\n{arm_truncated}\n```",
        "",
        f"## 机器码 — Zen4\n\n```\n{x86_truncated}\n```",
    ])


def _build_single_body(
    symbol: str,
    asm: str,
    platform: str,
    source_code: str | None,
    framework: str,
    binary_path: str,
    component: str,
    category_l1: str,
    max_lines: int,
) -> str:
    """Build the full issue body for a single-platform function.

    Returns body containing prompt + source + ASM.
    """
    truncated = _truncate_asm(asm, max_lines)

    prompt = _build_single_prompt(symbol, framework, platform)
    source_section = _build_source_section(source_code)

    platform_label = (
        _PLATFORM_LABEL_ARM if platform == "arm"
        else _PLATFORM_LABEL_X86
    )

    return "\n".join([
        f"## 提示词\n\n{prompt}",
        "",
        f"## 组件\n\n- {component}",
        "",
        f"## 分类\n\n- {category_l1}",
        "",
        f"## 环境\n\n- {framework}",
        "",
        source_section,
        "",
        f"## 机器码 — {platform_label}\n\n```\n{truncated}\n```",
    ])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_asm_diff_issue(
    function: dict[str, Any],
    arm_asm: str | None,
    x86_asm: str | None,
    source_code: str | None = None,
    framework_name: str = "CPython 3.14",
    binary_path: str = "",
    max_lines: int = 2000,
) -> dict[str, Any]:
    """Build issue title, body, and ASM comments for one hotspot function.

    Parameters
    ----------
    function : dict
        Function descriptor from Dataset.functions[].  Must contain at least
        ``symbol``.  May contain ``component`` and ``categoryL1``.
    arm_asm : str | None
        ARM (Kunpeng) assembly text, or ``None`` if unavailable.
    x86_asm : str | None
        x86 (Zen4) assembly text, or ``None`` if unavailable.
    source_code : str | None
        Optional C/Python source code for the function.
    framework_name : str
        Framework display name used in the prompt.
    binary_path : str
        Path to the binary used for objdump (displayed in the issue body).
    max_lines : int
        Maximum assembly lines to include before truncation.

    Returns
    -------
    dict[str, Any]
        ``{'title': str, 'body': str, 'comments': list[str]}``
        Body contains prompt + source code + ASM.  ``comments`` is empty
        by default.  If posting the body triggers a 403, call
        :func:`split_asm_from_body` to extract ASM into ``comments``.

    Raises
    ------
    ValueError
        If both *arm_asm* and *x86_asm* are ``None``.
    """
    if arm_asm is None and x86_asm is None:
        raise ValueError(
            "At least one of arm_asm or x86_asm must be provided"
        )

    symbol = function.get("symbol", "<unknown>")
    component = _resolve_component_display(
        function.get("component", "")
    )
    category_l1 = _resolve_category_display(
        function.get("categoryL1", "")
    )

    # Determine mode: dual or single-platform
    if arm_asm is not None and x86_asm is not None:
        title = f"{symbol}跨平台机器码差异分析"
        body = _build_dual_body(
            symbol=symbol,
            arm_asm=arm_asm,
            x86_asm=x86_asm,
            source_code=source_code,
            framework=framework_name,
            binary_path=binary_path,
            component=component,
            category_l1=category_l1,
            max_lines=max_lines,
        )
    elif arm_asm is not None:
        title = f"{symbol} ({_PLATFORM_LABEL_ARM} only) 机器码分析"
        body = _build_single_body(
            symbol=symbol,
            asm=arm_asm,
            platform="arm",
            source_code=source_code,
            framework=framework_name,
            binary_path=binary_path,
            component=component,
            category_l1=category_l1,
            max_lines=max_lines,
        )
    else:
        title = f"{symbol} ({_PLATFORM_LABEL_X86} only) 机器码分析"
        body = _build_single_body(
            symbol=symbol,
            asm=x86_asm,  # type: ignore[arg-type]
            platform="x86",
            source_code=source_code,
            framework=framework_name,
            binary_path=binary_path,
            component=component,
            category_l1=category_l1,
            max_lines=max_lines,
        )

    return {"title": title, "body": body, "comments": []}


def split_asm_from_body(body: str) -> tuple[str, list[str]]:
    """Split ASM sections out of *body* for posting as separate comments.

    Finds all ``## 机器码 —`` sections, extracts their content into
    individual comment strings, and returns the trimmed body (everything
    before the first ASM section) plus the list of comments.

    Returns
    -------
    tuple[str, list[str]]
        ``(trimmed_body, comments)``
    """
    lines = body.splitlines()
    comments: list[str] = []
    trimmed_lines: list[str] = []

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Detect "## 机器码 — ..." heading
        if stripped.startswith("## 机器码 —"):
            heading = stripped
            # Collect lines until the next heading (any ## line) or end
            asm_lines: list[str] = []
            i += 1
            while i < len(lines):
                next_stripped = lines[i].strip()
                if next_stripped.startswith("## "):
                    break
                asm_lines.append(lines[i])
                i += 1
            # Trim trailing blank lines from asm block
            while asm_lines and not asm_lines[-1].strip():
                asm_lines.pop()
            comments.append(f"{heading}\n\n" + "\n".join(asm_lines))
        else:
            trimmed_lines.append(line)
            i += 1

    # Trim trailing blank lines from body
    while trimmed_lines and not trimmed_lines[-1].strip():
        trimmed_lines.pop()

    return "\n".join(trimmed_lines), comments


def check_chunking(body: str, max_chars: int = 60000) -> dict[str, Any]:
    """Check if body needs chunking due to excessive length.

    Some issue platforms have character limits on issue bodies. This function
    checks whether the generated body exceeds the threshold and reports the
    line count for downstream splitting logic.

    Parameters
    ----------
    body : str
        The issue body text.
    max_chars : int
        Maximum character count before chunking is needed.

    Returns
    -------
    dict
        ``{'needs_chunking': bool, 'line_count': int}``
    """
    line_count = len(body.splitlines())
    needs_chunking = len(body) > max_chars
    return {
        "needs_chunking": needs_chunking,
        "line_count": line_count,
    }
