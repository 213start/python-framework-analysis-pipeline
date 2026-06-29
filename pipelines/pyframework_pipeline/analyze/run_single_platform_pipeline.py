#!/usr/bin/env python3
"""从单个 perf.data 一键产出单平台分析结果。"""

from __future__ import annotations

import argparse
import logging
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


LOGGER = logging.getLogger("run_single_platform_pipeline")


def default_arch() -> str:
    return platform.machine() or "unknown"


def default_platform_id() -> str:
    return f"{default_arch()}-local"


def default_python_version() -> str:
    return ".".join(str(part) for part in sys.version_info[:3])


def default_benchmark(perf_data: Path) -> str:
    stem = perf_data.stem
    return stem if stem and stem != "perf" else "perf_run"


def script_path(name: str) -> Path:
    return Path(__file__).with_name(name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="perf.data 文件路径")
    parser.add_argument("-o", "--output-dir", type=Path, required=True, help="输出目录")
    parser.add_argument("-b", "--benchmark", help="基准名称，默认取 perf.data 文件名")
    parser.add_argument("-p", "--platform-id", help="平台标识，默认取 <arch>-local")
    parser.add_argument("-a", "--arch", default=default_arch(), help="架构，默认自动获取")
    parser.add_argument("-V", "--python-version", default=default_python_version(), help="Python 版本")
    parser.add_argument("-i", "--build-id", default="", help="构建标识")
    parser.add_argument("-e", "--event", default="cycles", help="perf 事件名")
    parser.add_argument("-P", "--perf-bin", default="perf", help="perf 可执行文件路径")
    parser.add_argument("-n", "--top-n", type=int, default=20, help="注解和报告默认展示的前 N 条")
    parser.add_argument("--skip-annotate", action="store_true", help="跳过 perf annotate 步骤")
    parser.add_argument("--no-print-report", action="store_true", help="完成后不在命令行打印报告")
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="日志级别",
    )
    return parser.parse_args()


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


def build_output_paths(root: Path) -> dict[str, Path]:
    return {
        "root": root,
        "data_dir": root / "data",
        "records_csv": root / "data" / "perf_records.csv",
        "script_csv": root / "data" / "perf_script.csv",
        "tables_dir": root / "tables",
        "instruction_hotspots_csv": root / "tables" / "instruction_hotspots.csv",
        "reports_dir": root / "reports",
        "visuals_dir": root / "visuals",
        "platform_report": root / "reports" / "platform_report.md",
        "platform_report_full": root / "reports" / "platform_report_full.md",
        "platform_machine_code_report": root / "reports" / "platform_machine_code_report.md",
    }


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    )

    benchmark = args.benchmark or default_benchmark(args.input)
    platform_id = args.platform_id or default_platform_id()
    paths = build_output_paths(args.output_dir)
    paths["root"].mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="perf_pipeline_") as temp_dir:
        temp_root = Path(temp_dir)
        raw_csv = temp_root / "perf_raw.csv"
        script_csv = temp_root / "perf_script.csv"
        records_base_csv = temp_root / "records_base.csv"
        annotate_dir = temp_root / "annotate"

        run_step(
            [
                sys.executable,
                str(script_path("perf_data_to_csv.py")),
                str(args.input),
                "-o",
                str(raw_csv),
                "-p",
                args.perf_bin,
                "-l",
                args.log_level,
            ]
        )
        run_step(
            [
                sys.executable,
                str(script_path("perf_script_to_csv.py")),
                str(args.input),
                "-o",
                str(script_csv),
                "-p",
                args.perf_bin,
                "-l",
                args.log_level,
            ]
        )
        run_step(
            [
                sys.executable,
                str(script_path("normalize_perf_records.py")),
                str(raw_csv),
                "-o",
                str(records_base_csv),
                "-p",
                platform_id,
                "-a",
                args.arch,
                "-V",
                args.python_version,
                "-i",
                args.build_id,
                "-b",
                benchmark,
                "-e",
                args.event,
                "-l",
                args.log_level,
            ]
        )

        final_records = records_base_csv
        if not args.skip_annotate:
            try:
                run_step(
                    [
                        sys.executable,
                        str(script_path("annotate_perf_hotspots.py")),
                        str(records_base_csv),
                        "-d",
                        str(args.input),
                        "-o",
                        str(annotate_dir),
                        "-p",
                        args.perf_bin,
                        "-n",
                        str(args.top_n),
                        "-l",
                        args.log_level,
                    ]
                )
                final_records = annotate_dir / "records_enriched.csv"
                paths["tables_dir"].mkdir(parents=True, exist_ok=True)
                shutil.copyfile(annotate_dir / "instruction_hotspots.csv", paths["instruction_hotspots_csv"])
            except RuntimeError as exc:
                LOGGER.warning("annotate 步骤失败，继续保留无机器码注解的记录表: %s", exc)

        paths["data_dir"].mkdir(parents=True, exist_ok=True)
        shutil.copyfile(final_records, paths["records_csv"])
        shutil.copyfile(script_csv, paths["script_csv"])

    run_step(
        [
            sys.executable,
            str(script_path("summarize_platform_perf.py")),
            str(paths["records_csv"]),
            "-o",
            str(paths["tables_dir"]),
            "-s",
            str(paths["script_csv"]),
            "-l",
            args.log_level,
        ]
    )
    run_step(
        [
            sys.executable,
            str(script_path("render_platform_report.py")),
            str(paths["tables_dir"]),
            "-n",
            str(args.top_n),
            "-m",
            "formal",
            "-o",
            str(paths["platform_report"]),
        ]
    )
    run_step(
        [
            sys.executable,
            str(script_path("render_platform_report.py")),
            str(paths["tables_dir"]),
            "-n",
            str(args.top_n),
            "-m",
            "full",
            "-o",
            str(paths["platform_report_full"]),
        ]
    )
    run_step(
        [
            sys.executable,
            str(script_path("render_platform_machine_code_report.py")),
            str(paths["tables_dir"]),
            "-n",
            str(args.top_n),
            "-o",
            str(paths["platform_machine_code_report"]),
        ]
    )
    run_step(
        [
            sys.executable,
            str(script_path("render_platform_visuals.py")),
            str(paths["tables_dir"]),
            "-o",
            str(paths["visuals_dir"]),
            "-n",
            str(args.top_n),
        ]
    )

    if not args.no_print_report:
        print(paths["platform_report"].read_text(encoding="utf-8"), end="")

    LOGGER.info(
        "单平台一键流水线完成: input=%s output_dir=%s benchmark=%s platform_id=%s",
        args.input,
        paths["root"],
        benchmark,
        platform_id,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
