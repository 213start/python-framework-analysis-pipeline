#!/usr/bin/env python3
"""Run Data-Juicer's upstream performance benchmark in CPU-only mode."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request
from pathlib import Path


DEFAULT_DATA_URL = (
    "http://dail-wlcb.oss-cn-wulanchabu.aliyuncs.com/"
    "data_juicer/perf_bench_data/perf_bench_data.tar.gz"
)
DEFAULT_TOKENIZER = "EleutherAI/pythia-6.9b-deduped"
CPU_MODALITIES = {"text"}
DEFAULT_TEXT_ROWS = 10_000


def main() -> int:
    args = parse_args()
    work_dir = Path.cwd()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    modalities = [m for m in _split_modalities(args.modalities) if m in CPU_MODALITIES]
    if not modalities:
        modalities = ["text"]

    data_dir = ensure_benchmark_data(
        work_dir,
        [args.data_url, args.data_mirror_url],
        timeout=args.download_timeout,
        modalities=modalities,
        text_url=args.text_data_url,
        use_full_archive=args.use_full_data_archive,
        text_rows=args.text_rows,
    )

    timing_cases = []
    raw_results = []
    for modality in modalities:
        if modality != "text":
            print(f"Skipping non-CPU modality: {modality}", flush=True)
            continue
        config_path = write_text_config(
            work_dir,
            data_dir,
            np=args.np,
            tokenizer=args.tokenizer,
        )
        write_cpu_only_runtime_patch(work_dir)
        child_env = os.environ.copy()
        child_env["PYTHONPATH"] = _prepend_pythonpath(
            str(work_dir),
            child_env.get("PYTHONPATH", ""),
        )
        started = time.perf_counter()
        completed = subprocess.run(
            ["dj-process", "--config", str(config_path)],
            cwd=work_dir,
            env=child_env,
            text=True,
            capture_output=True,
            check=False,
        )
        wall_seconds = time.perf_counter() - started
        sys.stdout.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        if completed.returncode != 0:
            return completed.returncode

        output_path = work_dir / "outputs" / "performance_benchmark_text"
        parsed = parse_datajuicer_logs(output_path)
        record_count = count_jsonl(output_path / "res.jsonl")
        if record_count <= 0:
            sys.stderr.write("Data-Juicer benchmark produced zero output records\n")
            return 2
        case = build_timing_case(
            "data-juicer-text",
            wall_seconds,
            parsed,
            record_count,
        )
        timing_cases.append(case)
        raw_results.append({
            "modality": modality,
            "wallClockSeconds": wall_seconds,
            "recordCount": record_count,
            "ops": parsed["ops"],
            "totalOpSeconds": parsed["totalOpSeconds"],
            "dataDir": str(data_dir),
            "config": str(config_path),
        })
        print(json.dumps({
            "type": "BENCHMARK_RESULT",
            "caseId": "data-juicer-text",
            "platform": args.platform,
            "wallClockSeconds": wall_seconds,
            "recordCount": record_count,
        }, sort_keys=True), flush=True)

    timing_dir = output_dir / "timing"
    timing_dir.mkdir(parents=True, exist_ok=True)
    (timing_dir / "timing-raw.json").write_text(
        json.dumps({
            "schemaVersion": 1,
            "platform": args.platform,
            "framework": "datajuicer",
            "results": raw_results,
        }, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (timing_dir / "timing-normalized.json").write_text(
        json.dumps({
            "schemaVersion": 1,
            "platform": args.platform,
            "cases": timing_cases,
        }, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--modalities",
        default=os.environ.get("DATA_JUICER_BENCH_MODALITIES", "text"),
    )
    parser.add_argument(
        "--data-url",
        default=os.environ.get("DATA_JUICER_BENCH_DATA_URL", DEFAULT_DATA_URL),
    )
    parser.add_argument(
        "--data-mirror-url",
        default=os.environ.get("DATA_JUICER_BENCH_DATA_MIRROR_URL", ""),
    )
    parser.add_argument(
        "--text-data-url",
        default=os.environ.get("DATA_JUICER_BENCH_TEXT_URL", ""),
    )
    parser.add_argument(
        "--use-full-data-archive",
        action="store_true",
        default=_env_bool("DATA_JUICER_BENCH_USE_FULL_ARCHIVE", False),
    )
    parser.add_argument(
        "--text-rows",
        type=int,
        default=int(
            os.environ.get("DATA_JUICER_BENCH_TEXT_ROWS", str(DEFAULT_TEXT_ROWS))
        ),
    )
    parser.add_argument(
        "--tokenizer",
        default=os.environ.get("DATA_JUICER_BENCH_TOKENIZER", DEFAULT_TOKENIZER),
    )
    parser.add_argument(
        "--np",
        type=int,
        default=int(os.environ.get("DATA_JUICER_BENCH_NP", "16")),
    )
    parser.add_argument("--download-timeout", type=int, default=600)
    return parser.parse_args()


def _split_modalities(value: str) -> list[str]:
    return [item.strip() for item in value.replace(",", " ").split() if item.strip()]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def ensure_benchmark_data(
    work_dir: Path,
    urls: list[str],
    *,
    timeout: int,
    modalities: list[str] | None = None,
    text_url: str = "",
    use_full_archive: bool = False,
    text_rows: int = DEFAULT_TEXT_ROWS,
) -> Path:
    data_dir = work_dir / "perf_bench_data"
    text_file = data_dir / "text" / "wiki-10k.jsonl"
    if text_file.exists() and text_file.stat().st_size > 0:
        return data_dir

    selected = modalities or ["text"]
    if set(selected).issubset(CPU_MODALITIES) and not use_full_archive:
        return ensure_text_benchmark_data(
            data_dir,
            text_url=text_url,
            timeout=timeout,
            rows=text_rows,
        )

    archive = work_dir / "perf_bench_data.tar.gz"
    errors = []
    for url in [u for u in urls if u]:
        try:
            print(f"Downloading benchmark data: {url}", flush=True)
            _download_url(url, archive, timeout)
            with tarfile.open(archive) as tar:
                tar.extractall(work_dir)
            if text_file.exists() and text_file.stat().st_size > 0:
                archive.unlink(missing_ok=True)
                return data_dir
            errors.append(f"{url}: archive missing {text_file}")
        except Exception as exc:
            errors.append(f"{url}: {exc}")
            archive.unlink(missing_ok=True)
    raise RuntimeError(
        "failed to prepare Data-Juicer benchmark data: " + "; ".join(errors)
    )


def ensure_text_benchmark_data(
    data_dir: Path,
    *,
    text_url: str,
    timeout: int,
    rows: int,
) -> Path:
    text_file = data_dir / "text" / "wiki-10k.jsonl"
    if text_file.exists() and text_file.stat().st_size > 0:
        return data_dir

    text_file.parent.mkdir(parents=True, exist_ok=True)
    if text_url:
        tmp_file = text_file.with_name(f"{text_file.name}.tmp")
        print(f"Downloading text-only benchmark data: {text_url}", flush=True)
        try:
            _download_url(text_url, tmp_file, timeout)
            tmp_file.replace(text_file)
        finally:
            tmp_file.unlink(missing_ok=True)
    else:
        print(
            "Generating text-only Data-Juicer benchmark data "
            f"({rows} records); skipping multimodal archive",
            flush=True,
        )
        generate_text_benchmark_data(text_file, rows=rows)
    return data_dir


def _download_url(url: str, target: Path, timeout: int) -> None:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        with target.open("wb") as fh:
            shutil.copyfileobj(response, fh)


def generate_text_benchmark_data(path: Path, *, rows: int) -> None:
    if rows < 1:
        raise ValueError("rows must be >= 1")

    topics = [
        "normalization",
        "token counting",
        "document deduplication",
        "field selection",
        "metadata cleanup",
        "quality filtering",
        "parallel workers",
        "CPU profiling",
    ]
    with path.open("w", encoding="utf-8") as fh:
        for index in range(rows):
            topic = topics[index % len(topics)]
            text = (
                f"Data-Juicer performance benchmark sample {index:05d}. "
                f"This text-only record exercises {topic}, mapper stages, "
                "token filters, deduplication behavior, and export ordering. "
                f"The stable row id {index:05d} keeps the document distinct "
                "while preserving a compact CPU benchmark input."
            )
            row = {
                "id": f"wiki-{index:05d}",
                "text": text,
                "__dj__stats__": {"num_token": max(1, len(text.split()))},
            }
            fh.write(json.dumps(row) + "\n")


def write_cpu_only_runtime_patch(work_dir: Path) -> Path:
    patch_path = work_dir / "sitecustomize.py"
    patch_path.write_text(
        """\
from __future__ import annotations

import sys
import types
from contextlib import contextmanager

from data_juicer.utils.lazy_loader import LazyLoader

_original_check_packages = LazyLoader.check_packages.__func__
_original_load = LazyLoader._load


def _package_base(spec):
    name = str(spec).strip()
    if "@" in name:
        name = name.split("@", 1)[0]
    if "[" in name:
        name = name.split("[", 1)[0]
    for marker in ("==", ">=", "<=", ">", "<"):
        if marker in name:
            name = name.split(marker, 1)[0]
            break
    return name.strip().lower()


@classmethod
def _cpu_only_check_packages(cls, package_specs, pip_args=None):
    if isinstance(package_specs, str):
        specs = [package_specs]
    else:
        specs = list(package_specs)
    filtered = [spec for spec in specs if _package_base(spec) != "torch"]
    if len(filtered) != len(specs):
        sys.stderr.write(
            "[pyframework] skipping Data-Juicer torch auto-install "
            "for CPU-only benchmark\\n"
        )
    if filtered:
        return _original_check_packages(cls, filtered, pip_args)
    return None


class _TorchCudaStub:
    @staticmethod
    def is_available():
        return False

    @staticmethod
    def device_count():
        return 0

    @staticmethod
    def empty_cache():
        return None


class _TorchStub(types.ModuleType):
    cuda = _TorchCudaStub()
    Tensor = type("Tensor", (), {})
    FloatTensor = Tensor
    LongTensor = Tensor
    float32 = "float32"
    float16 = "float16"
    bfloat16 = "bfloat16"

    @staticmethod
    @contextmanager
    def no_grad():
        yield


def _install_stub(self, module):
    self._module = module
    self._parent_module_globals[self._module_name] = module
    self.__dict__.update(module.__dict__)
    return module


def _cpu_only_load(self):
    module_name = getattr(self, "_module_name", "")
    if module_name == "torch":
        return _install_stub(self, _TorchStub("torch"))
    if module_name == "torch.nn":
        return _install_stub(self, types.ModuleType("torch.nn"))
    return _original_load(self)


LazyLoader.check_packages = _cpu_only_check_packages
LazyLoader._load = _cpu_only_load
""",
        encoding="utf-8",
    )
    return patch_path


def _prepend_pythonpath(path: str, existing: str) -> str:
    if not existing:
        return path
    return f"{path}{os.pathsep}{existing}"


def write_text_config(
    work_dir: Path,
    data_dir: Path,
    *,
    np: int,
    tokenizer: str,
) -> Path:
    config_dir = work_dir / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "text.yaml"
    config_path.write_text(
        "\n".join([
            "project_name: 'performance-benchmark-text'",
            f"dataset_path: '{data_dir / 'text' / 'wiki-10k.jsonl'}'",
            "export_path: 'outputs/performance_benchmark_text/res.jsonl'",
            f"np: {np}",
            "use_cache: false",
            "process:",
            "  - whitespace_normalization_mapper:",
            "  - token_num_filter:",
            f"      hf_tokenizer: '{tokenizer}'",
            "      min_num: 0",
            "  - document_deduplicator:",
            "      lowercase: false",
            "      ignore_non_character: false",
            "  - topk_specified_field_selector:",
            "      field_key: '__dj__stats__.num_token'",
            "      topk: 1000",
        ])
        + "\n",
        encoding="utf-8",
    )
    return config_path


def parse_datajuicer_logs(work_dir: Path) -> dict:
    ops = []
    log_dir = work_dir / "log"
    log_files = sorted(log_dir.glob("export_*_time_*.txt")) if log_dir.exists() else []
    op_re = re.compile(r"OP \[(.*?)\] Done in (.*?)s")
    total_re = re.compile(r"All OPs are done in (.*?)s")
    total_seconds = 0.0
    for log_file in log_files:
        text = log_file.read_text(encoding="utf-8", errors="replace")
        for name, seconds in op_re.findall(text):
            ops.append({"name": name, "seconds": float(seconds)})
        totals = total_re.findall(text)
        if totals:
            total_seconds = float(totals[-1])
    return {
        "ops": ops,
        "totalOpSeconds": sum(op["seconds"] for op in ops),
        "reportedTotalSeconds": total_seconds,
    }


def count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(encoding="utf-8", errors="replace") as fh:
        return sum(1 for line in fh if line.strip())


def build_timing_case(
    case_id: str,
    wall_seconds: float,
    parsed: dict,
    record_count: int,
) -> dict:
    op_seconds = float(parsed.get("totalOpSeconds", 0.0))
    reported_total = float(parsed.get("reportedTotalSeconds", 0.0) or 0.0)
    total_seconds = reported_total or wall_seconds
    framework_seconds = max(total_seconds - op_seconds, 0.0)
    wall_ns = int(wall_seconds * 1_000_000_000)
    op_ns = int(op_seconds * 1_000_000_000)
    framework_ns = int(framework_seconds * 1_000_000_000)
    return {
        "caseId": case_id,
        "platform": "",
        "recordCount": record_count,
        "timingSource": "datajuicer_performance_benchmark",
        "metrics": {
            "wallClockTime": {"wall_clock_ns": wall_ns},
            "tmE2eTime": {"wall_clock_ns": wall_ns},
            "frameworkCallTime": {"total_ns": framework_ns},
            "businessOperatorTime": {"total_ns": op_ns},
        },
        "ops": parsed.get("ops", []),
    }


if __name__ == "__main__":
    raise SystemExit(main())
