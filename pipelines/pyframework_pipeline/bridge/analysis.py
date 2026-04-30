"""Step 7 publish/fetch orchestration.

publish: create analysis issues for all hotspot functions.
fetch: pull LLM comments, parse structured results, backfill into Dataset.
"""

from __future__ import annotations

import json
import logging
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .comment_parser import (
    ParsedAnalysis,
    find_approved_analysis_comment,
    find_approved_discussion_analysis,
)
from .issue_client import IssueClient, create_client
from .issue_template import build_asm_diff_issue, check_chunking, split_asm_from_body
from .manifest import BridgeIssueEntry, BridgeManifest, load_bridge_manifest

logger = logging.getLogger(__name__)

_LABEL_ASM_DIFF = "asm-diff"
_LABEL_COLOR = "1d76db"


def _is_body_too_large(exc: Exception) -> bool:
    """Check if the exception indicates the body exceeded platform limits."""
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code == 403
    if isinstance(exc, RuntimeError):
        msg = str(exc).lower()
        return "403" in msg or "forbidden" in msg
    return False


def _find_existing_comment(
    existing_comments: list[dict[str, Any]],
    heading: str,
) -> dict[str, Any] | None:
    """Find a comment whose body starts with *heading*."""
    for c in existing_comments:
        body = c.get("body", "")
        if body.strip().startswith(heading):
            return c
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _upsert_manifest_entry(
    manifest: BridgeManifest,
    function_id: str,
    platform: str,
    repo: str,
    issue_number: int,
    issue_url: str,
) -> None:
    """Insert or update a manifest entry for *function_id*."""
    for e in manifest.issues:
        if e.function_id == function_id:
            e.issue_number = issue_number
            e.issue_url = issue_url
            e.status = "created"
            return
    manifest.issues.append(BridgeIssueEntry(
        issue_type="asm-diff",
        function_id=function_id,
        platform=platform,
        repo=repo,
        issue_number=issue_number,
        issue_url=issue_url,
        status="created",
        created_at=_now_iso(),
    ))


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _find_dataset(root: Path) -> Path | None:
    ds_dir = root / "datasets"
    if ds_dir.is_dir():
        files = list(ds_dir.glob("*.dataset.json"))
        if files:
            return files[0]
    return None


def _find_source(root: Path) -> Path | None:
    src_dir = root / "sources"
    if src_dir.is_dir():
        files = list(src_dir.glob("*.source.json"))
        if files:
            return files[0]
    return None


def _read_asm_content(source_data: dict[str, Any], artifact_id: str) -> str | None:
    """Read assembly text from a source artifact's file path or inline content."""
    for art in source_data.get("artifactIndex", []):
        if art.get("id") == artifact_id:
            # Prefer file path, fall back to inline content.
            file_path = art.get("filePath")
            if file_path:
                p = Path(file_path)
                if p.exists():
                    return p.read_text(encoding="utf-8", errors="replace")
            return art.get("content")
    return None


def _read_source_snippet(source_data: dict[str, Any], function: dict) -> str | None:
    """Read source code snippet for a function from sourceAnchors."""
    for anchor in source_data.get("sourceAnchors", []):
        # Match by function symbol reference or source location overlap.
        if anchor.get("functionId") == function.get("id"):
            return anchor.get("snippet", "")
    return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------

def publish(
    project_path: Path,
    repo: str,
    platform: str,
    token: str,
    *,
    bridge_type: str = "discussion",
    discussion_category: str = "General",
    dry_run: bool = False,
    max_lines: int = 2000,
    base_url: str | None = None,
) -> dict[str, Any]:
    """Create analysis issues or discussions for all hotspot functions.

    Parameters
    ----------
    project_path:
        Path to ``project.yaml``.
    repo:
        ``"owner/repo"`` on the target platform.
    platform:
        ``"github"`` or ``"gitcode"``.
    token:
        API personal-access token.
    bridge_type:
        ``"discussion"`` (default) or ``"issue"``.
    discussion_category:
        Discussion category name (only used when bridge_type is discussion).
    dry_run:
        If *True*, build issue bodies but do not create issues.
    max_lines:
        Maximum assembly lines per issue before truncation.
    base_url:
        Override default API base URL.

    Returns
    -------
    dict with summary stats.
    """
    from ..config import resolve_four_layer_root

    root = resolve_four_layer_root(project_path)
    dataset_path = _find_dataset(root)
    source_path = _find_source(root)

    if dataset_path is None:
        logger.error(
            "No dataset JSON found under %s. "
            "Run step 6 (backfill) first to generate four-layer data.",
            root,
        )
        return {
            "total_functions": 0,
            "published": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 1,
            "issues": [],
        }

    dataset = _load_json(dataset_path)
    source_data = _load_json(source_path) if source_path else {}

    owner, repo_name = repo.split("/", 1)

    # Load or create manifest.
    manifest_path = root / "bridge-manifest.json"
    manifest = load_bridge_manifest(manifest_path)
    manifest.project_id = dataset.get("id", "")

    # Resolve framework name for prompt.
    framework_id = dataset.get("frameworkId", "")
    framework_name = _resolve_framework_display(framework_id)

    # Build the appropriate client.
    use_discussion = bridge_type == "discussion"
    issue_client: IssueClient | None = None
    discussion_client = None
    repo_id: str | None = None

    if not dry_run:
        if use_discussion:
            from .discussion_client import DiscussionClient
            discussion_client = DiscussionClient(token=token, base_url=base_url)
            repo_id = discussion_client.get_repo_id(owner, repo_name)
        else:
            issue_client = create_client(platform, token, base_url=base_url)
            try:
                issue_client.ensure_label(
                    owner, repo_name, _LABEL_ASM_DIFF, _LABEL_COLOR,
                )
            except Exception:
                logger.warning("Failed to create label (may already exist)")

    # Fetch remote state: map title → {number, url}.
    remote_map: dict[str, dict[str, Any]] = {}
    if not dry_run:
        if use_discussion:
            assert discussion_client is not None
            remote_map = discussion_client.list_discussions(owner, repo_name)
        else:
            assert issue_client is not None
            remote_map = issue_client.list_issues_by_label(
                owner, repo_name, _LABEL_ASM_DIFF,
            )
        logger.info("Remote has %d existing discussions/issues", len(remote_map))

    functions = dataset.get("functions", [])
    published: list[dict[str, Any]] = []
    skipped = 0
    updated = 0
    errors = 0

    for func in functions:
        func_id = func.get("id", "")
        symbol = func.get("symbol", "<unknown>")

        # Collect ARM and x86 assembly.
        arm_asm = None
        x86_asm = None
        for aid in func.get("artifactIds", []):
            content = _read_asm_content(source_data, aid)
            if content is None:
                continue
            if "_arm_" in aid or aid.startswith("asm_arm64_"):
                arm_asm = content
            elif "_x86_" in aid or aid.startswith("asm_x86_64_"):
                x86_asm = content

        if arm_asm is None and x86_asm is None:
            logger.warning("No asm for %s, skipping", symbol)
            skipped += 1
            continue

        source_code = _read_source_snippet(source_data, func)

        try:
            issue = build_asm_diff_issue(
                function=func,
                arm_asm=arm_asm,
                x86_asm=x86_asm,
                source_code=source_code,
                framework_name=framework_name,
                max_lines=max_lines,
            )
        except ValueError as exc:
            logger.error("Template error for %s: %s", symbol, exc)
            errors += 1
            continue

        if dry_run:
            chunk_info = check_chunking(issue["body"])
            published.append({
                "symbol": symbol,
                "title": issue["title"],
                "body_length": len(issue["body"]),
                "needs_chunking": chunk_info.get("needs_chunking", False),
                "dry_run": True,
            })
            continue

        existing_remote = remote_map.get(issue["title"])

        try:
            if existing_remote:
                number = existing_remote["number"]
                url = existing_remote.get("url") or existing_remote.get("html_url", "")
                logger.info("Updating %s → #%s (already exists)", symbol, number)
                try:
                    if use_discussion:
                        assert discussion_client is not None
                        discussion_client.update_discussion_body(
                            owner, repo_name, number, issue["body"],
                        )
                    else:
                        assert issue_client is not None
                        issue_client.update_issue(
                            owner, repo_name, number, issue["body"],
                        )
                except Exception as exc:
                    if not _is_body_too_large(exc):
                        raise
                    logger.warning(
                        "Body too large for #%s (%s), splitting ASM into comments: %s",
                        number, symbol, exc,
                    )
                    body, asm_comments = split_asm_from_body(issue["body"])
                    # Fetch existing comments to avoid duplicates.
                    existing_comments = _fetch_existing_comments(
                        use_discussion=use_discussion,
                        discussion_client=discussion_client,
                        issue_client=issue_client,
                        owner=owner,
                        repo_name=repo_name,
                        number=number,
                    )
                    if use_discussion:
                        assert discussion_client is not None
                        discussion_client.update_discussion_body(
                            owner, repo_name, number, body,
                        )
                    else:
                        assert issue_client is not None
                        issue_client.update_issue(
                            owner, repo_name, number, body,
                        )
                    _post_comments(
                        use_discussion=use_discussion,
                        discussion_client=discussion_client,
                        issue_client=issue_client,
                        owner=owner,
                        repo_name=repo_name,
                        number=number,
                        comments=asm_comments,
                        symbol=symbol,
                        existing_comments=existing_comments,
                    )

                _upsert_manifest_entry(
                    manifest, func_id, platform, repo, number, url,
                )
                published.append({
                    "symbol": symbol,
                    "issue_number": number,
                    "issue_url": url,
                })
                updated += 1
            else:
                # Create new discussion/issue.
                try:
                    number, url = _create_issue(
                        use_discussion=use_discussion,
                        discussion_client=discussion_client,
                        issue_client=issue_client,
                        owner=owner,
                        repo_name=repo_name,
                        repo_id=repo_id,
                        issue=issue,
                        discussion_category=discussion_category,
                    )
                except Exception as exc:
                    if not _is_body_too_large(exc):
                        raise
                    # Body too large → split ASM into comments and retry.
                    logger.warning(
                        "Body too large for %s, splitting ASM into comments: %s",
                        symbol, exc,
                    )
                    body, asm_comments = split_asm_from_body(issue["body"])
                    issue["body"] = body
                    number, url = _create_issue(
                        use_discussion=use_discussion,
                        discussion_client=discussion_client,
                        issue_client=issue_client,
                        owner=owner,
                        repo_name=repo_name,
                        repo_id=repo_id,
                        issue=issue,
                        discussion_category=discussion_category,
                    )
                    # Post ASM as separate comments.
                    _post_comments(
                        use_discussion=use_discussion,
                        discussion_client=discussion_client,
                        issue_client=issue_client,
                        owner=owner,
                        repo_name=repo_name,
                        number=number,
                        comments=asm_comments,
                        symbol=symbol,
                    )

                _upsert_manifest_entry(
                    manifest, func_id, platform, repo, number, url,
                )
                published.append({
                    "symbol": symbol,
                    "issue_number": number,
                    "issue_url": url,
                })
                logger.info("Published %s → #%s", symbol, number)
        except Exception as exc:
            logger.error("Failed to publish %s: %s", symbol, exc, exc_info=True)
            errors += 1
            continue

    # Save manifest.
    manifest.write(manifest_path)

    return {
        "total_functions": len(functions),
        "published": len(published),
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "issues": published,
        "dry_run": dry_run,
    }


def _create_issue(
    *,
    use_discussion: bool,
    discussion_client: Any,
    issue_client: Any,
    owner: str,
    repo_name: str,
    repo_id: str | None,
    issue: dict[str, Any],
    discussion_category: str,
) -> tuple[int, str]:
    """Create a new discussion or issue. Returns (number, url)."""
    if use_discussion:
        assert discussion_client is not None
        assert repo_id is not None
        category_id = discussion_client.get_discussion_category_id(
            repo_id, discussion_category,
        )
        result = discussion_client.create_discussion(
            repo_id=repo_id,
            category_id=category_id,
            title=issue["title"],
            body=issue["body"],
        )
        return result.get("number", 0), result.get("url", "")
    else:
        assert issue_client is not None
        result = issue_client.create_issue(
            owner=owner,
            repo=repo_name,
            title=issue["title"],
            body=issue["body"],
            labels=[_LABEL_ASM_DIFF],
        )
        return result.get("number", 0), result.get("html_url", "")


def _fetch_existing_comments(
    *,
    use_discussion: bool,
    discussion_client: Any,
    issue_client: Any,
    owner: str,
    repo_name: str,
    number: int,
) -> list[dict[str, Any]]:
    """Fetch existing comments for a discussion or issue."""
    try:
        if use_discussion:
            assert discussion_client is not None
            return discussion_client.get_discussion_comments(
                owner, repo_name, number,
            )
        else:
            assert issue_client is not None
            return issue_client.get_issue_comments(
                owner, repo_name, number,
            )
    except Exception as exc:
        logger.warning("Failed to fetch existing comments for #%s: %s", number, exc)
        return []


def _post_comments(
    *,
    use_discussion: bool,
    discussion_client: Any,
    issue_client: Any,
    owner: str,
    repo_name: str,
    number: int,
    comments: list[str],
    symbol: str,
    existing_comments: list[dict[str, Any]] | None = None,
) -> None:
    """Post or update ASM comments on a discussion or issue.

    For each comment, checks *existing_comments* for a matching heading.
    If found, updates the existing comment; otherwise creates a new one.
    """
    existing = existing_comments or []
    for comment_body in comments:
        # Extract heading (first line) for matching.
        heading = comment_body.split("\n", 1)[0].strip()
        match = _find_existing_comment(existing, heading)

        try:
            if use_discussion:
                assert discussion_client is not None
                if match:
                    discussion_client.update_comment(match["id"], comment_body)
                else:
                    node_id = discussion_client._get_discussion_node_id(
                        owner, repo_name, number,
                    )
                    discussion_client.add_comment(node_id, comment_body)
            else:
                assert issue_client is not None
                if match:
                    issue_client.update_comment(
                        owner, repo_name, match["id"], comment_body,
                    )
                else:
                    issue_client.create_comment(
                        owner, repo_name, number, comment_body,
                    )
        except Exception as comment_exc:
            logger.warning(
                "Failed to post comment for %s: %s",
                symbol, comment_exc,
            )


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch(
    project_path: Path,
    repo: str,
    platform: str,
    token: str,
    *,
    bridge_type: str = "discussion",
    base_url: str | None = None,
) -> dict[str, Any]:
    """Fetch LLM analysis comments and backfill into Dataset.

    Parameters
    ----------
    project_path:
        Path to ``project.yaml``.
    repo:
        ``"owner/repo"`` on the target platform.
    platform:
        ``"github"`` or ``"gitcode"``.
    token:
        API personal-access token.
    bridge_type:
        ``"discussion"`` (default) or ``"issue"``.  Determines whether
        to use threaded Discussion comments or flat Issue comments.
    base_url:
        Override default API base URL.

    Returns
    -------
    dict with summary stats.
    """
    from ..config import resolve_four_layer_root

    root = resolve_four_layer_root(project_path)
    dataset_path = _find_dataset(root)

    if dataset_path is None:
        logger.error(
            "No dataset JSON found under %s. "
            "Run step 6 (backfill) first to generate four-layer data.",
            root,
        )
        return {"status": "no_dataset", "fetched": 0, "parsed": 0, "failed": 0}

    dataset = _load_json(dataset_path)

    manifest_path = root / "bridge-manifest.json"
    manifest = load_bridge_manifest(manifest_path)

    if not manifest.issues:
        return {"status": "no_issues", "fetched": 0, "parsed": 0, "failed": 0}

    owner, repo_name = repo.split("/", 1)

    use_discussion = bridge_type == "discussion"
    issue_client: IssueClient | None = None
    discussion_client = None

    if use_discussion:
        from .discussion_client import DiscussionClient
        discussion_client = DiscussionClient(token=token, base_url=base_url)
    else:
        issue_client = create_client(platform, token, base_url=base_url)

    func_map = {f["id"]: f for f in dataset.get("functions", []) if "id" in f}

    fetched = 0
    parsed = 0
    failed = 0
    review_pending = 0
    patterns_new: list[dict[str, Any]] = []
    root_causes_new: list[dict[str, Any]] = []
    opportunities_new: list[dict[str, Any]] = []

    for entry in manifest.issues:
        if entry.status not in ("created", "analysed", "review-pending"):
            continue

        parsed_result: ParsedAnalysis | None = None
        review_status: str = "no-analysis"

        try:
            if use_discussion:
                assert discussion_client is not None
                comments = discussion_client.get_discussion_comments(
                    owner, repo_name, entry.issue_number,
                )
                parsed_result, review_status = (
                    find_approved_discussion_analysis(comments)
                )
            else:
                assert issue_client is not None
                comments = issue_client.get_issue_comments(
                    owner, repo_name, entry.issue_number,
                )
                parsed_result = find_approved_analysis_comment(comments)
                review_status = "approved" if parsed_result else "no-analysis"
        except Exception as exc:
            logger.error(
                "Failed to fetch comments for #%s: %s",
                entry.issue_number, exc,
            )
            continue

        fetched += 1

        if review_status == "review-pending":
            entry.status = "review-pending"
            review_pending += 1
            logger.info(
                "#%s: review pending (last analysis not yet approved)",
                entry.issue_number,
            )
            continue

        if parsed_result is None:
            logger.info(
                "No approved analysis on #%s yet (status: %s)",
                entry.issue_number, review_status,
            )
            entry.status = "analysed"
            continue

        entry.status = "analysed"

        func = func_map.get(entry.function_id)
        if func is None:
            logger.warning(
                "Function %s not found in dataset", entry.function_id,
            )
            entry.status = "failed"
            failed += 1
            continue

        # Backfill diffView from parsed sections.
        _backfill_diff_view(func, parsed_result)

        # Extract root causes.
        for idx, rc in enumerate(parsed_result.root_causes):
            rc_id = f"rc_{entry.function_id}_{idx}"
            rc_entry = {
                "id": rc_id,
                "title": rc.get("劣势来源", rc.get("根因", "")),
                "category": rc.get("根因类别", ""),
                "location": rc.get("出现位置", ""),
                "impact": rc.get("热路径影响", ""),
                "evidence": rc.get("perf stat/PMU证据", ""),
                "functionId": entry.function_id,
            }
            root_causes_new.append(rc_entry)

        # Extract optimization opportunities.
        for idx, opp in enumerate(parsed_result.opportunities):
            opp_entry = {
                "id": f"opp_{entry.function_id}_{idx}",
                "title": opp.get("优化点", ""),
                "arm_status": opp.get("ARM现状", ""),
                "x86_equivalent": opp.get("x86对应实现", ""),
                "diff_note": opp.get("差异说明", ""),
                "functionId": entry.function_id,
            }
            opportunities_new.append(opp_entry)

        # Extract optimization strategies.
        for idx, opt in enumerate(parsed_result.optimizations):
            opt_entry = {
                "id": f"opt_{entry.function_id}_{idx}",
                "title": opt.get("优化点", ""),
                "strategy": opt.get("策略", ""),
                "beneficiary": opt.get("受益方", ""),
                "implementer": opt.get("实施方", ""),
                "functionId": entry.function_id,
            }
            patterns_new.append(opt_entry)

        entry.status = "parsed"
        entry.parsed_at = _now_iso()
        parsed += 1

        if parsed_result.warnings:
            logger.warning(
                "Parse warnings for #%s (%s): %s",
                entry.issue_number,
                parsed_result.symbol,
                "; ".join(parsed_result.warnings),
            )

    # Merge new patterns and root causes into dataset.
    _merge_list(dataset, "patterns", patterns_new, "id")
    _merge_list(dataset, "rootCauses", root_causes_new, "id")
    _merge_list(dataset, "opportunities", opportunities_new, "id")

    # Write back.
    _write_json(dataset_path, dataset)
    manifest.write(manifest_path)

    return {
        "fetched": fetched,
        "parsed": parsed,
        "failed": failed,
        "review_pending": review_pending,
        "patterns_added": len(patterns_new),
        "root_causes_added": len(root_causes_new),
    }


# ---------------------------------------------------------------------------
# Backfill helpers
# ---------------------------------------------------------------------------

def _backfill_diff_view(func: dict, parsed: ParsedAnalysis) -> None:
    """Populate func["diffView"] from a parsed analysis comment.

    Produces a structure compatible with the frontend ``FunctionDetail.diffView``
    type: top-level ``functionId``, ``sourceFile``, ``sourceLocation``,
    ``diffGuide``, and ``analysisBlocks`` with armRegions/x86Regions/mappings.
    """
    blocks: list[dict[str, Any]] = []
    for idx, sec in enumerate(parsed.sections):
        body = sec.get("body", "")
        table = sec.get("table", [])

        # Split the section body into ARM and x86 code regions.
        arm_snippets = _extract_code_blocks(body, arm=True)
        x86_snippets = _extract_code_blocks(body, arm=False)

        arm_regions = [
            {
                "id": f"arm_{idx:03d}_{j:03d}",
                "label": f"ARM snippet {j + 1}",
                "location": "",
                "role": "code",
                "snippet": s,
                "highlights": _extract_mnemonics(s),
                "defaultExpanded": j == 0,
            }
            for j, s in enumerate(arm_snippets)
        ]
        x86_regions = [
            {
                "id": f"x86_{idx:03d}_{j:03d}",
                "label": f"x86 snippet {j + 1}",
                "location": "",
                "role": "code",
                "snippet": s,
                "highlights": _extract_mnemonics(s),
                "defaultExpanded": j == 0,
            }
            for j, s in enumerate(x86_snippets)
        ]

        # Build mappings from comparison table rows.
        mappings: list[dict[str, Any]] = []
        if table:
            first_row = table[0]
            mappings.append({
                "id": f"map_{idx:03d}",
                "label": sec.get("title", f"Section {idx + 1}"),
                "sourceAnchorIds": [],
                "armRegionIds": [r["id"] for r in arm_regions],
                "x86RegionIds": [r["id"] for r in x86_regions],
                "note": first_row.get("差异", first_row.get("ARM劣势", "")),
            })

        # Diff signals from table rows that indicate ARM disadvantage.
        signals: list[str] = []
        for row in table:
            diff = row.get("差异", row.get("ARM劣势", ""))
            if diff and diff != "无差异":
                signals.append(diff[:80])

        block: dict[str, Any] = {
            "id": f"blk_{idx:03d}",
            "label": sec.get("title", f"Section {idx + 1}"),
            "summary": signals[0][:120] if signals else "",
            "sourceAnchors": [],
            "armRegions": arm_regions,
            "x86Regions": x86_regions,
            "mappings": mappings,
            "diffSignals": signals[:5],
            "alignmentNote": "",
            "performanceNote": signals[0] if signals else "",
            "defaultExpanded": idx < 3,
        }
        blocks.append(block)

    func["diffView"] = {
        "functionId": func.get("id", ""),
        "sourceFile": "",
        "sourceLocation": "",
        "diffGuide": "由 LLM 评论自动生成，按逐行对照分析分段。",
        "analysisBlocks": blocks,
    }


# ---------------------------------------------------------------------------
# Code-block extraction helpers
# ---------------------------------------------------------------------------

_ARM_LABELS = ("kunpeng", "arm", "aarch64")
_X86_LABELS = ("zen", "x86", "x86_64", "amd64")


def _extract_code_blocks(text: str, arm: bool) -> list[str]:
    """Extract code blocks labelled for the given platform from markdown text.

    Looks for patterns like ``### Kunpeng`` or ``ARM:`` followed by a fenced
    code block, or plain code blocks under a platform heading.
    """
    import re

    labels = _ARM_LABELS if arm else _X86_LABELS
    lines = text.split("\n")
    blocks: list[str] = []
    current_block: list[str] = []
    in_code_fence = False
    in_platform_section = False

    for line in lines:
        stripped = line.strip()

        # Detect platform section heading (### Kunpeng / ### Zen4 / etc.)
        if stripped.startswith("#"):
            heading_lower = stripped.lower()
            is_target = any(lbl in heading_lower for lbl in labels)
            if is_target:
                in_platform_section = True
                continue
            elif in_platform_section and not is_target:
                # Hit a different heading — flush current block.
                in_platform_section = False
                if current_block:
                    blocks.append("\n".join(current_block))
                    current_block = []

        # Detect fenced code blocks.
        if stripped.startswith("```"):
            if in_code_fence:
                # End of code block.
                in_code_fence = False
                if current_block:
                    blocks.append("\n".join(current_block))
                    current_block = []
            else:
                in_code_fence = True
            continue

        if in_code_fence and in_platform_section:
            current_block.append(line)

    # Flush trailing block.
    if current_block:
        blocks.append("\n".join(current_block))

    return blocks


def _extract_mnemonics(snippet: str) -> list[str]:
    """Extract instruction mnemonics from assembly text for highlighting."""
    mnemonics: list[str] = []
    for line in snippet.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith(";") or stripped.startswith("//"):
            continue
        parts = stripped.split()
        if parts:
            mnemonic = parts[0].rstrip(":")
            if mnemonic and mnemonic not in mnemonics:
                mnemonics.append(mnemonic)
    return mnemonics[:10]


def _merge_list(
    dataset: dict[str, Any],
    key: str,
    new_items: list[dict[str, Any]],
    id_key: str,
) -> None:
    """Merge *new_items* into ``dataset[key]`` by *id_key*, avoiding duplicates."""
    existing = dataset.setdefault(key, [])
    existing_ids = {item.get(id_key, "") for item in existing}
    for item in new_items:
        if item.get(id_key, "") not in existing_ids:
            existing.append(item)
            existing_ids.add(item[id_key])


def _resolve_framework_display(framework_id: str) -> str:
    """Map internal framework ID to display name for issue prompt."""
    _map: dict[str, str] = {
        "pyflink": "PyFlink",
        "pyspark": "PySpark",
        "cpython": "CPython 3.14",
    }
    return _map.get(framework_id, framework_id or "Python Framework")


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def status(project_path: Path) -> dict[str, Any]:
    """Report current bridge status for a project."""
    from ..config import resolve_four_layer_root

    root = resolve_four_layer_root(project_path)
    manifest_path = root / "bridge-manifest.json"
    manifest = load_bridge_manifest(manifest_path)

    counts: dict[str, int] = {}
    for entry in manifest.issues:
        counts[entry.status] = counts.get(entry.status, 0) + 1

    return {
        "project_id": manifest.project_id,
        "total_issues": len(manifest.issues),
        "by_status": counts,
        "manifest_path": str(manifest_path),
    }
