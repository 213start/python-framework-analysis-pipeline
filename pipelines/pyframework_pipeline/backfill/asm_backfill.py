"""Backfill Source.artifactIndex and Dataset.functions[].diffView from asm files.

Globs .s files from arm/x86 run directories, generates artifact entries,
and builds initial diffView skeletons for each hotspot function.
"""

from __future__ import annotations

import hashlib
from pathlib import Path


# ---------------------------------------------------------------------------
# Platform directory name normalisation
# ---------------------------------------------------------------------------

# Known sub-directory names for each logical platform.  The acquisition step
# creates ``<run_dir>/asm/<platform>/`` where *platform* comes from the
# acquisition config, but we also tolerate the shorter "arm" / "x86" variants.
_ARM_DIRS = ("arm64", "arm")
_X86_DIRS = ("x86_64", "x86")

_PLATFORM_MAP: dict[str, str] = {}
for _d in _ARM_DIRS:
    _PLATFORM_MAP[_d] = "arm64"
for _d in _X86_DIRS:
    _PLATFORM_MAP[_d] = "x86_64"

# Canonical platform labels used in artifact IDs and JSON fields.
_CANONICAL_ARM = "arm64"
_CANONICAL_X86 = "x86_64"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _symbol_to_hash(symbol: str) -> str:
    """Return first 8 hex chars of MD5(symbol)."""
    return hashlib.md5(symbol.encode()).hexdigest()[:8]


def _artifact_id(platform: str, symbol: str) -> str:
    """Generate ``asm_<platform>_<hash>`` style artifact ID."""
    return f"asm_{platform}_{_symbol_to_hash(symbol)}"


def _artifact_path(platform: str, symbol: str) -> str:
    """Relative artifact path used in the artifactIndex entry."""
    return f"artifacts/asm/{_platform_dir(platform)}/{_symbol_to_hash(symbol)}.s"


def _platform_dir(platform: str) -> str:
    """Canonical sub-directory name for *platform*."""
    if platform == _CANONICAL_ARM:
        return "arm"
    return "x86"


def _empty_diff_view() -> dict:
    """Return an initial diffView skeleton (to be populated by Step 7)."""
    return {
        "sourceAnchors": [],
        "analysisBlocks": [],
        "armRegions": [],
        "x86Regions": [],
        "mappings": [],
        "diffSignals": [],
        "alignmentNote": "",
        "performanceNote": "",
    }


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def _discover_asm_files(run_dir: Path, platform_dirs: tuple[str, ...]) -> dict[str, Path]:
    """Find .s files under ``<run_dir>/asm/<sub>/``.

    Returns ``{symbol_name: absolute_path}`` where *symbol_name* is the
    filename stem (without the ``.s`` extension).
    """
    result: dict[str, Path] = {}
    asm_root = run_dir / "asm"
    if not asm_root.is_dir():
        return result

    for sub in platform_dirs:
        sub_dir = asm_root / sub
        if not sub_dir.is_dir():
            continue
        for s_file in sorted(sub_dir.glob("*.s")):
            symbol = s_file.stem
            # First discovery wins — avoids duplicates when both arm64/ and
            # arm/ happen to exist with overlapping content.
            if symbol not in result:
                result[symbol] = s_file
    return result


# ---------------------------------------------------------------------------
# Artifact index helpers
# ---------------------------------------------------------------------------

def _existing_artifact_ids(source: dict) -> set[str]:
    """Return the set of IDs already present in ``source["artifactIndex"]``."""
    return {entry["id"] for entry in source.get("artifactIndex", [])}


def _build_artifact_entry(
    symbol: str,
    platform: str,
    rel_path: str,
) -> dict:
    """Build one artifactIndex entry for an assembly file."""
    platform_label = "Arm" if platform == _CANONICAL_ARM else "x86"
    return {
        "id": _artifact_id(platform, symbol),
        "title": f"{symbol} 的 {platform_label} 汇编",
        "type": "assembly",
        "description": f"objdump -S -d 反汇编输出",
        "platform": platform,
        "path": rel_path,
        "contentType": "text/plain",
    }


# ---------------------------------------------------------------------------
# Dataset.functions helpers
# ---------------------------------------------------------------------------

def _functions_by_symbol(dataset: dict) -> dict[str, dict]:
    """Index existing functions by symbol name."""
    result: dict[str, dict] = {}
    for func in dataset.get("functions", []):
        sym = func.get("symbol", "")
        if sym:
            result[sym] = func
    return result


def _ensure_diff_view(func: dict) -> None:
    """Add a diffView skeleton to *func* if it does not already have one."""
    if "diffView" not in func or not func["diffView"]:
        func["diffView"] = _empty_diff_view()


def _add_new_function(
    dataset: dict,
    symbol: str,
    arm_only: bool = False,
    x86_only: bool = False,
) -> dict:
    """Add a minimal function entry and return it.

    The entry contains only ``symbol`` and a ``diffView`` skeleton.  Metadata
    like ``component``, ``categoryL1``, ``caseIds`` will be filled by
    perf_backfill or later enrichment steps.
    """
    func: dict = {
        "symbol": symbol,
        "diffView": _empty_diff_view(),
    }
    if arm_only:
        func["platforms"] = ["arm64"]
    elif x86_only:
        func["platforms"] = ["x86_64"]
    dataset.setdefault("functions", []).append(func)
    return func


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def backfill_asm(
    arm_run_dir: Path,
    x86_run_dir: Path,
    source: dict,
    dataset: dict,
) -> dict:
    """Backfill Source.artifactIndex and initial Dataset.functions[].diffView.

    Parameters
    ----------
    arm_run_dir : Path
        Run output directory for the ARM platform.
    x86_run_dir : Path
        Run output directory for the x86 platform.
    source : dict
        The Source layer JSON (mutated in place).
    dataset : dict
        The Dataset layer JSON (mutated in place).

    Returns
    -------
    dict
        Summary with artifact and function counts.
    """
    # ------------------------------------------------------------------
    # 1. Discover .s files from both platforms
    # ------------------------------------------------------------------
    arm_files = _discover_asm_files(arm_run_dir, _ARM_DIRS)
    x86_files = _discover_asm_files(x86_run_dir, _X86_DIRS)

    # Union of all symbols across both platforms.
    all_symbols = sorted(set(arm_files.keys()) | set(x86_files.keys()))

    # ------------------------------------------------------------------
    # 2. Build artifact entries (dedup by ID)
    # ------------------------------------------------------------------
    existing_ids = _existing_artifact_ids(source)
    new_artifact_ids: list[str] = []

    for symbol in all_symbols:
        has_arm = symbol in arm_files
        has_x86 = symbol in x86_files

        if has_arm:
            arm_path = _artifact_path(_CANONICAL_ARM, symbol)
            aid = _artifact_id(_CANONICAL_ARM, symbol)
            if aid not in existing_ids:
                source.setdefault("artifactIndex", []).append(
                    _build_artifact_entry(symbol, _CANONICAL_ARM, arm_path)
                )
                existing_ids.add(aid)
                new_artifact_ids.append(aid)

        if has_x86:
            x86_path = _artifact_path(_CANONICAL_X86, symbol)
            aid = _artifact_id(_CANONICAL_X86, symbol)
            if aid not in existing_ids:
                source.setdefault("artifactIndex", []).append(
                    _build_artifact_entry(symbol, _CANONICAL_X86, x86_path)
                )
                existing_ids.add(aid)
                new_artifact_ids.append(aid)

    # ------------------------------------------------------------------
    # 3. Build / update Dataset.functions[] diffView skeletons
    # ------------------------------------------------------------------
    funcs_by_symbol = _functions_by_symbol(dataset)

    arm_only_count = 0
    x86_only_count = 0
    both_count = 0
    new_func_count = 0

    for symbol in all_symbols:
        has_arm = symbol in arm_files
        has_x86 = symbol in x86_files

        if has_arm and has_x86:
            both_count += 1
        elif has_arm:
            arm_only_count += 1
        else:
            x86_only_count += 1

        if symbol in funcs_by_symbol:
            # Function already exists (e.g. from perf_backfill) — just
            # ensure it has a diffView skeleton.
            _ensure_diff_view(funcs_by_symbol[symbol])
        else:
            # New function — add with minimal metadata.
            _add_new_function(
                dataset,
                symbol,
                arm_only=has_arm and not has_x86,
                x86_only=has_x86 and not has_arm,
            )
            new_func_count += 1

    # ------------------------------------------------------------------
    # 4. Return summary
    # ------------------------------------------------------------------
    total_arm = len(arm_files)
    total_x86 = len(x86_files)

    return {
        "status": "backfilled" if all_symbols else "skipped",
        "armFiles": total_arm,
        "x86Files": total_x86,
        "uniqueSymbols": len(all_symbols),
        "newArtifacts": len(new_artifact_ids),
        "newFunctions": new_func_count,
        "bothPlatforms": both_count,
        "armOnly": arm_only_count,
        "x86Only": x86_only_count,
    }
