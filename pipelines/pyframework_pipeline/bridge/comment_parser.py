"""Parser for structured Markdown LLM comments.

Extracts overview table, line-by-line analysis sections, root cause summary,
and optimization strategies from the LLM's comment on an analysis issue.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ParsedAnalysis:
    """Result of parsing an LLM comment."""

    symbol: str
    overview_table: list[dict[str, str]]
    sections: list[dict]  # per-line analysis sections
    root_causes: list[dict[str, str]]
    optimizations: list[dict[str, str]]
    opportunities: list[dict[str, str]]
    raw_body: str
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Regex patterns (compiled once)
# ---------------------------------------------------------------------------

# Cross-platform heading: ## 跨平台机器码差异分析：{symbol}
_RE_CROSS = re.compile(
    r"^##\s*跨平台机器码差异分析[：:]\s*(.+)",
    re.MULTILINE,
)

# Single-platform heading: ## {platform} 机器码分析：{symbol}
_RE_SINGLE = re.compile(
    r"^##\s*\S+\s*机器码分析[：:]\s*(.+)",
    re.MULTILINE,
)

# Sub-section heading ### or ####
_RE_HEADING = re.compile(r"^(#{2,4})\s+(.+)", re.MULTILINE)

_RE_REVIEW_BLOCKED = re.compile(
    r"(?:^|\n)\s*#{0,4}\s*\*{0,2}\s*"
    r"(?:"
    r"not\s+approved|cannot\s+approve|can't\s+approve|do\s+not\s+approve|"
    r"needs?\s+revisions?|needs?\s+changes?|request\s+changes?|"
    r"changes?\s+requested|major\s+revisions?|blockers?"
    r")",
    re.IGNORECASE | re.MULTILINE,
)

_RE_REVIEW_APPROVED = re.compile(
    r"(?:^|\n)\s*#{0,4}\s*\*{0,2}\s*"
    r"(?:"
    r"approved|approve\s+with\s+minor\s+revisions?|"
    r"lgtm|looks\s+good\s+to\s+me"
    r")",
    re.IGNORECASE | re.MULTILINE,
)

_REVIEW_BLOCKED_TERMS = (
    "未通过",
    "不通过",
    "不能通过",
    "无法通过",
    "不予通过",
    "需要修改",
    "需要修订",
    "请求修改",
    "请修改",
    "阻塞",
)

_REVIEW_APPROVED_TERMS = (
    "审核通过",
    "评审通过",
    "结论通过",
    "可以接受",
    "同意发布",
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_analysis_comment(comments: list[dict[str, Any]]) -> ParsedAnalysis | None:
    """Find and parse the analysis comment from a list of issue comments.

    Takes the **last** matching comment (LLM corrections).
    Each *comment* dict is expected to have a ``"body"`` key (str).
    """

    matched_body: str | None = None
    for comment in comments:
        body = comment.get("body", "")
        if not body:
            continue
        if _RE_CROSS.search(body) or _RE_SINGLE.search(body):
            matched_body = body

    if matched_body is None:
        return None
    return parse_comment_body(matched_body)


def find_approved_analysis_comment(
    comments: list[dict[str, Any]],
) -> ParsedAnalysis | None:
    """Find the latest analysis comment only if later review approves it.

    Review comments before the latest analysis are ignored. Later decisive
    review comments override earlier ones, so an approval followed by a change
    request does not pass the gate.
    """

    matched_index: int | None = None
    matched_body: str | None = None
    for index, comment in enumerate(comments):
        body = comment.get("body", "")
        if body and _is_analysis_body(body):
            matched_index = index
            matched_body = body

    if matched_index is None or matched_body is None:
        return None

    review_state: str | None = None
    for comment in comments[matched_index + 1:]:
        state = _review_state(comment.get("body", ""))
        if state is not None:
            review_state = state

    if review_state != "approved":
        return None
    return parse_comment_body(matched_body)


def parse_comment_body(body: str) -> ParsedAnalysis | None:
    """Parse a single comment body for analysis structure."""

    # Detect symbol name
    m = _RE_CROSS.search(body)
    single = False
    if m is None:
        m = _RE_SINGLE.search(body)
        single = True
    if m is None:
        return None

    symbol = m.group(1).strip()
    warnings: list[str] = []

    # Extract top-level sections via heading positions
    heading_matches = list(_RE_HEADING.finditer(body))

    def _section_text(heading: str) -> str:
        """Return text under *heading* (### level) until next same-level."""
        return _find_section(body, heading, heading_matches)

    # --- Overview table -------------------------------------------------------
    # Only take the first contiguous table under ### 总览 (stop at blank line).
    overview_raw = _first_contiguous_table(_section_text("总览"))
    overview_table = _extract_markdown_table(overview_raw)

    # --- Per-line analysis sections (#### N. ...) -----------------------------
    sections: list[dict] = []
    # Collect all #### headings (level 4)
    level4 = [hm for hm in heading_matches if hm.group(1) == "####"]
    for idx, hm in enumerate(level4):
        sec_title = hm.group(2).strip()
        start = hm.end()
        end = level4[idx + 1].start() if idx + 1 < len(level4) else len(body)
        # Also stop at next ### or ## heading
        remaining = body[start:end]
        stop = _find_lower_heading_pos(remaining, min_level=3)
        if stop >= 0:
            sec_body = remaining[:stop]
        else:
            sec_body = remaining

        # Inside each section look for a comparison table
        sec_table = _extract_markdown_table(sec_body)
        sections.append({
            "title": sec_title,
            "body": sec_body.strip(),
            "table": sec_table,
        })

    # --- Root causes ----------------------------------------------------------
    root_cause_raw = _section_text("根因汇总")
    root_causes = _extract_markdown_table(root_cause_raw)

    # --- Optimization opportunities -------------------------------------------
    opp_raw = _section_text("优化机会")
    opportunities = _extract_markdown_table(opp_raw)

    # --- Optimization strategies ----------------------------------------------
    opt_raw = _section_text("优化策略")
    optimizations = _extract_markdown_table(opt_raw)

    # Basic validation warnings
    if not overview_table:
        warnings.append("overview table is empty or missing")
    if not sections:
        warnings.append("no per-line analysis sections found")
    if not root_causes:
        warnings.append("root-cause table is empty or missing")
    if not optimizations:
        warnings.append("optimization table is empty or missing")

    return ParsedAnalysis(
        symbol=symbol,
        overview_table=overview_table,
        sections=sections,
        root_causes=root_causes,
        optimizations=optimizations,
        opportunities=opportunities,
        raw_body=body,
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_analysis_body(body: str) -> bool:
    return bool(_RE_CROSS.search(body) or _RE_SINGLE.search(body))


def _review_state(body: str) -> str | None:
    """Return the decisive review state for one comment, if present."""

    if not body:
        return None

    normalized = body.lower()
    if _RE_REVIEW_BLOCKED.search(normalized) or any(
        term in body for term in _REVIEW_BLOCKED_TERMS
    ):
        return "blocked"
    if _RE_REVIEW_APPROVED.search(normalized) or any(
        term in body for term in _REVIEW_APPROVED_TERMS
    ):
        return "approved"
    return None


def find_approved_discussion_analysis(
    comments: list[dict[str, Any]],
) -> tuple[ParsedAnalysis | None, str]:
    """Find the latest approved analysis from threaded Discussion comments.

    Parameters
    ----------
    comments:
        Threaded comment list from ``DiscussionClient.get_discussion_comments``.
        Each item has ``body`` and ``replies`` (list of ``{body}`` dicts).

    Returns
    -------
    tuple of (parsed_analysis_or_None, status_str)

    Status values:
    - ``"approved"`` — last analysis has an approved review reply
    - ``"review-pending"`` — last analysis has review replies but none approved
    - ``"analysis-only"`` — last analysis has no review replies
    - ``"no-analysis"`` — no analysis comments at all
    """
    last_analysis_index: int | None = None
    last_analysis_body: str | None = None

    for idx, comment in enumerate(comments):
        body = comment.get("body", "")
        if body and _is_analysis_body(body):
            last_analysis_index = idx
            last_analysis_body = body

    if last_analysis_index is None or last_analysis_body is None:
        return None, "no-analysis"

    # Check replies of the last analysis comment.
    replies = comments[last_analysis_index].get("replies", [])
    if not replies:
        # No reviews yet — backward-compatible: accept as-is.
        return parse_comment_body(last_analysis_body), "analysis-only"

    # Scan replies for a decisive review state.
    review_state: str | None = None
    for reply in replies:
        state = _review_state(reply.get("body", ""))
        if state is not None:
            review_state = state

    if review_state == "approved":
        return parse_comment_body(last_analysis_body), "approved"

    return None, "review-pending"


def _first_contiguous_table(text: str) -> str:
    """Return the first contiguous Markdown table block from *text*.

    A table block starts at the first ``|``-prefixed line and extends through
    consecutive ``|``-prefixed lines.  A blank line ends the block.
    """

    lines: list[str] = []
    in_table = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("|"):
            in_table = True
            lines.append(line)
        elif in_table and stripped == "":
            break
        elif in_table:
            break
    return "\n".join(lines)


def _extract_markdown_table(text: str) -> list[dict[str, str]]:
    """Parse a Markdown table into a list of dicts.

    Handles:
    - ``| col | col |`` style tables
    - separator rows (``|---|---|``) are skipped
    - missing cells are tolerated (padded with empty string)
    - leading/trailing whitespace in cells is stripped
    """

    rows: list[list[str]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        # Split on |, drop leading and trailing empty fragments
        cells = [c.strip() for c in stripped.split("|")]
        if cells and cells[0] == "":
            cells = cells[1:]
        if cells and cells[-1] == "":
            cells = cells[:-1]
        # Skip separator row like |---|---|
        if cells and all(re.match(r"^[-:]+$", c) for c in cells):
            continue
        if cells:
            rows.append(cells)

    if not rows:
        return []

    # First row is the header
    headers = rows[0]
    result: list[dict[str, str]] = []
    for data_row in rows[1:]:
        # Pad short rows
        padded = data_row + [""] * (len(headers) - len(data_row))
        # Truncate long rows
        padded = padded[: len(headers)]
        result.append({h: v for h, v in zip(headers, padded)})

    return result


def _find_section(
    text: str,
    heading: str,
    heading_matches: list[re.Match[str]] | None = None,
) -> str:
    """Extract text under a heading (### level) until the next same-level heading.

    *heading* is matched as a substring within the heading text.
    If *heading_matches* is provided it is reused (avoids re-scanning).
    """

    if heading_matches is None:
        heading_matches = list(_RE_HEADING.finditer(text))

    for idx, hm in enumerate(heading_matches):
        level = len(hm.group(1))
        title = hm.group(2).strip()
        if heading not in title:
            continue
        # Determine end position: next heading with level <= current
        start = hm.end()
        end = len(text)
        for later in heading_matches[idx + 1 :]:
            later_level = len(later.group(1))
            if later_level <= level:
                end = later.start()
                break
        return text[start:end]

    return ""


def _find_lower_heading_pos(text: str, min_level: int = 3) -> int:
    """Return the position of the first heading with level <= *min_level*.

    Returns -1 if no such heading is found.
    """

    for hm in _RE_HEADING.finditer(text):
        if len(hm.group(1)) <= min_level:
            return hm.start()
    return -1
