# UDF Benchmarking Adapter Design

## Goal

Add first-class support for `stone31415/UDF_Benchmarking` as a single-container framework target, modeled after the Data-Juicer adapter, using Python 3.11 and enabling Python flamegraphs by default for the reference project.

## Source Repository

The upstream repository is `https://gitcode.com/stone31415/UDF_Benchmarking`. Its entrypoint is:

```bash
python main.py --config-file config.yaml --output ./log
```

The benchmark writes per-case CSV files, `benchmark.log`, and for `MockVideoE2EUDF` a summary JSON/CSV. The default upstream config filters to `MockVideoE2EUDF`, which makes it a good first reference case for the pipeline.

## Architecture

Introduce a new framework id, `udfbenchmarking`, with the same broad execution model as `datajuicer`:

- build a Python 3.11 Docker image containing runtime dependencies, perf tools, and py-spy;
- run one long-lived privileged container per platform;
- deploy the workload directory into `/workspace/benchmark`;
- run `perf record` around `python main.py`;
- convert benchmark CSV/JSON output into the pipeline's `timing/timing-normalized.json`;
- optionally run a second `py-spy record --format flamegraph` pass and fetch SVG artifacts.

This keeps Daft/Ray-specific details out of the first implementation. The default reference project uses native Daft runner with a small deterministic E2E smoke workload so real-machine validation is stable on ARM and x86.

## Configuration

Add `software.udfBenchmarkingImages` and related fields:

- `udfBenchmarkingImages.{platform}`: per-platform image tag.
- `udfBenchmarkingContainer`: default `udf-benchmarking-bench`.
- `udfBenchmarkingRepo`: default GitCode URL.
- `udfBenchmarkingRevision`: optional git ref or commit.
- `benchmarkName`: default `MockVideoE2EUDF`.
- `benchmarkConfigFile`: default `config.yaml`.
- `pythonFlamegraph`: same schema as Data-Juicer, enabled in the reference environment example.

The reference workload includes a pinned smoke `config.yaml` rather than relying on the upstream default million-row settings.

## Artifacts

For each platform the run directory will contain:

- `timing/timing-raw.json`
- `timing/timing-normalized.json`
- `perf/data/perf-{platform}.data`
- `perf/data/perf_records.csv`
- `python/flamegraphs/<benchmark>.svg`
- `python/manifest.json`

The benchmark raw output is fetched under a framework-specific output directory inside the platform run directory for debugging.

## Error Handling

The adapter fails fast when the image lacks required tools, when `main.py` exits non-zero, or when timing conversion cannot find a benchmark CSV/summary. Missing optional flamegraph artifacts are fatal only when `pythonFlamegraph.enabled` is true.

## Test Strategy

Tests cover:

- environment plan generation and py-spy readiness checks;
- config validation for `udfbenchmarking`;
- workload deploy targeting the UDF Benchmarking container;
- benchmark command generation and timing conversion;
- optional Python flamegraph command and manifest generation.

Real-machine validation should run the reference project on `blue-98` and `zen5` with a fresh run dir, stopping before bridge publish.
