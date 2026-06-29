#!/usr/bin/env python3
"""从两份 perf.data 或两份平台记录 CSV 一键产出双平台对比结果。"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

try:
    from .run_single_platform_pipeline import default_benchmark, script_path
except ImportError:
    from run_single_platform_pipeline import default_benchmark, script_path


LOGGER = logging.getLogger("run_compare_pipeline")


def infer_platform_root_from_records(records_path: Path | None) -> Path | None:
    if records_path is None:
        return None
    if records_path.name == "perf_records.csv" and records_path.parent.name == "data":
        root = records_path.parent.parent
        if (root / "tables").exists():
            return root
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-R", "--baseline-root", type=Path, help="baseline 单平台输出根目录（主模式）")
    parser.add_argument("-S", "--target-root", type=Path, help="target 单平台输出根目录（主模式）")
    parser.add_argument("baseline_input", nargs="?", type=Path, help="baseline perf.data 文件路径")
    parser.add_argument("target_input", nargs="?", type=Path, help="target perf.data 文件路径")
    parser.add_argument("-o", "--output-dir", type=Path, required=True, help="输出目录")
    parser.add_argument("-b", "--benchmark", help="基准名称，默认取 baseline perf.data 文件名")
    parser.add_argument("-B", "--baseline-records", type=Path, help="baseline 平台记录 CSV")
    parser.add_argument("-T", "--target-records", type=Path, help="target 平台记录 CSV")
    parser.add_argument("-p", "--baseline-platform-id", default="amd-baseline", help="baseline 平台标识")
    parser.add_argument("-q", "--target-platform-id", default="arm-target", help="target 平台标识")
    parser.add_argument("-a", "--baseline-arch", default="x86_64", help="baseline 架构")
    parser.add_argument("-A", "--target-arch", default="aarch64", help="target 架构")
    parser.add_argument("-x", "--baseline-e2e-time", type=float, required=True, help="baseline 端到端时间")
    parser.add_argument("-y", "--target-e2e-time", type=float, required=True, help="target 端到端时间")
    parser.add_argument("-V", "--python-version", default="", help="两侧共享的 Python 版本，可留空")
    parser.add_argument("-i", "--baseline-build-id", default="", help="baseline 构建标识")
    parser.add_argument("-j", "--target-build-id", default="", help="target 构建标识")
    parser.add_argument("-e", "--event", default="cycles", help="perf 事件名")
    parser.add_argument("-P", "--perf-bin", default="perf", help="perf 可执行文件路径")
    parser.add_argument("-n", "--top-n", type=int, default=20, help="报告和图片默认展示的前 N 条")
    parser.add_argument("--skip-annotate", action="store_true", help="跳过 perf annotate 步骤")
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="日志级别",
    )
    return parser.parse_args()


def use_records_inputs(args: argparse.Namespace) -> bool:
    return bool(args.baseline_records or args.target_records)


def use_root_inputs(args: argparse.Namespace) -> bool:
    return bool(args.baseline_root or args.target_root)


def validate_args(args: argparse.Namespace) -> None:
    has_root_inputs = bool(args.baseline_root and args.target_root)
    has_any_root_input = bool(args.baseline_root or args.target_root)
    has_raw_inputs = bool(args.baseline_input and args.target_input)
    has_any_raw_input = bool(args.baseline_input or args.target_input)
    has_records_inputs = bool(args.baseline_records and args.target_records)
    has_any_records_input = bool(args.baseline_records or args.target_records)

    if has_any_root_input and not has_root_inputs:
        raise ValueError("baseline_root 和 target_root 必须同时提供")
    if has_any_raw_input and not has_raw_inputs:
        raise ValueError("baseline_input 和 target_input 必须同时提供")
    if has_any_records_input and not has_records_inputs:
        raise ValueError("baseline_records 和 target_records 必须同时提供")
    selected_modes = sum(bool(value) for value in (has_root_inputs, has_records_inputs, has_raw_inputs))
    if selected_modes != 1:
        raise ValueError("必须三选一：平台输出目录、平台记录 CSV 或两份 perf.data")


def compare_command(
    *,
    baseline: Path,
    target: Path,
    baseline_platform_id: str,
    target_platform_id: str,
    baseline_e2e_time: float,
    target_e2e_time: float,
    output_dir: Path,
    log_level: str,
) -> list[str]:
    return [
        sys.executable,
        str(script_path("compare_platform_perf.py")),
        "-b",
        str(baseline),
        "-t",
        str(target),
        "-p",
        baseline_platform_id,
        "-q",
        target_platform_id,
        "-x",
        str(baseline_e2e_time),
        "-y",
        str(target_e2e_time),
        "-o",
        str(output_dir),
        "-l",
        log_level,
    ]


def build_output_paths(root: Path) -> dict[str, Path]:
    return {
        "root": root,
        "baseline_root": root / "baseline",
        "target_root": root / "target",
        "compare_root": root,
        "compare_tables": root / "tables",
        "compare_reports": root / "reports",
        "compare_visuals": root / "visuals",
        "compare_report": root / "reports" / "compare_report.md",
        "compare_report_html": root / "reports" / "compare_report.html",
    }


def run_step(command: list[str]) -> None:
    LOGGER.info("执行: %s", " ".join(command))
    completed = subprocess.run(command, check=False, text=True, capture_output=True)
    if completed.stdout.strip():
        LOGGER.debug("stdout:\n%s", completed.stdout.strip())
    if completed.stderr.strip():
        LOGGER.debug("stderr:\n%s", completed.stderr.strip())
    if completed.returncode != 0:
        raise RuntimeError(
            f"命令失败: {' '.join(command)} | exit={completed.returncode} | stderr={completed.stderr.strip()}"
        )


def main() -> int:
    args = parse_args()
    validate_args(args)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    )

    benchmark_source = args.baseline_input or args.baseline_root or args.baseline_records or Path("perf.data")
    benchmark = args.benchmark or default_benchmark(benchmark_source)
    paths = build_output_paths(args.output_dir)
    paths["root"].mkdir(parents=True, exist_ok=True)

    if use_root_inputs(args):
        baseline_root = args.baseline_root
        target_root = args.target_root
        baseline_records = baseline_root / "data" / "perf_records.csv"
        target_records = target_root / "data" / "perf_records.csv"
        LOGGER.info(
            "使用单平台输出目录进行双平台对比: baseline=%s target=%s",
            baseline_root,
            target_root,
        )
    elif use_records_inputs(args):
        baseline_records = args.baseline_records
        target_records = args.target_records
        baseline_root = infer_platform_root_from_records(baseline_records)
        target_root = infer_platform_root_from_records(target_records)
        LOGGER.info(
            "使用已有平台记录进行双平台对比: baseline=%s target=%s",
            baseline_records,
            target_records,
        )
    else:
        baseline_cmd = [
            sys.executable,
            str(script_path("run_single_platform_pipeline.py")),
            str(args.baseline_input),
            "-o",
            str(paths["baseline_root"]),
            "-b",
            benchmark,
            "-p",
            args.baseline_platform_id,
            "-a",
            args.baseline_arch,
            "-e",
            args.event,
            "-P",
            args.perf_bin,
            "-n",
            str(args.top_n),
            "-l",
            args.log_level,
        ]
        if args.python_version:
            baseline_cmd.extend(["-V", args.python_version])
        if args.baseline_build_id:
            baseline_cmd.extend(["-i", args.baseline_build_id])
        if args.skip_annotate:
            baseline_cmd.append("--skip-annotate")
        run_step(baseline_cmd)

        target_cmd = [
            sys.executable,
            str(script_path("run_single_platform_pipeline.py")),
            str(args.target_input),
            "-o",
            str(paths["target_root"]),
            "-b",
            benchmark,
            "-p",
            args.target_platform_id,
            "-a",
            args.target_arch,
            "-e",
            args.event,
            "-P",
            args.perf_bin,
            "-n",
            str(args.top_n),
            "-l",
            args.log_level,
        ]
        if args.python_version:
            target_cmd.extend(["-V", args.python_version])
        if args.target_build_id:
            target_cmd.extend(["-i", args.target_build_id])
        if args.skip_annotate:
            target_cmd.append("--skip-annotate")
        run_step(target_cmd)
        baseline_records = paths["baseline_root"] / "data" / "perf_records.csv"
        target_records = paths["target_root"] / "data" / "perf_records.csv"
        baseline_root = paths["baseline_root"]
        target_root = paths["target_root"]

    run_step(
        compare_command(
            baseline=baseline_records,
            target=target_records,
            baseline_platform_id=args.baseline_platform_id,
            target_platform_id=args.target_platform_id,
            baseline_e2e_time=args.baseline_e2e_time,
            target_e2e_time=args.target_e2e_time,
            output_dir=paths["compare_tables"],
            log_level=args.log_level,
        )
    )
    run_step(
        [
            sys.executable,
            str(script_path("render_compare_visuals.py")),
            str(paths["compare_tables"]),
            "-o",
            str(paths["compare_visuals"]),
            "-n",
            str(args.top_n),
        ]
    )
    run_step(
        [
            sys.executable,
            str(script_path("render_compare_report.py")),
            str(paths["compare_tables"]),
            "-n",
            str(args.top_n),
            "-o",
            str(paths["compare_report"]),
        ]
    )
    report_cmd = [
        sys.executable,
        str(script_path("render_compare_integrated_report.py")),
        str(paths["compare_tables"]),
        "-n",
        str(args.top_n),
        "-v",
        str(paths["compare_visuals"]),
        "-o",
        str(paths["compare_report_html"]),
    ]
    if baseline_root is not None:
        report_cmd.extend(["-b", str(baseline_root)])
    if target_root is not None:
        report_cmd.extend(["-t", str(target_root)])
    run_step(report_cmd)

    LOGGER.info(
        "双平台一键流水线完成: baseline=%s target=%s output_dir=%s benchmark=%s mode=%s",
        baseline_records,
        target_records,
        paths["root"],
        benchmark,
        "records" if use_records_inputs(args) else "raw",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
