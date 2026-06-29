"""Data-Juicer environment adapter.

Creates a single CPU-only Docker container for running Data-Juicer's upstream
performance benchmark text modality.  Image/video/audio modalities are
intentionally excluded because they can require GPU/model-heavy operators.
"""

from __future__ import annotations

import shlex
from typing import Any

from pyframework_pipeline.environment.planning import PlanStep

DEFAULT_BASE_IMAGE = "python:3.11-slim"
DEFAULT_CONTAINER = "data-juicer-bench"
DEFAULT_VERSION = "1.5.2"
DEFAULT_DATA_URL = (
    "http://dail-wlcb.oss-cn-wulanchabu.aliyuncs.com/"
    "data_juicer/perf_bench_data/perf_bench_data.tar.gz"
)
CPU_MODALITIES = {"text"}


class DataJuicerEnvironmentAdapter:
    """Generates Data-Juicer-specific environment plan steps."""

    framework_id = "datajuicer"

    def get_plan_steps(
        self,
        platform: str,
        platform_config: dict[str, Any],
        software: dict[str, Any],
        host_refs: dict[str, Any],
    ) -> list[PlanStep]:
        steps: list[PlanStep] = []

        hosts_by_role = {
            entry["role"]: entry["hostRef"]
            for entry in platform_config.get("hosts", [])
            if "role" in entry and "hostRef" in entry
        }
        host = hosts_by_role.get("client") or next(iter(hosts_by_role.values()), "")
        host_alias = host_refs.get(host, {}).get("alias", host)
        host_env = host_refs.get(host, {}).get("env", {})
        arch = platform_config.get("arch", "x86_64")

        image = software.get("dataJuicerImages", {}).get(
            platform,
            software.get("dataJuicerImage", f"data-juicer-bench:{DEFAULT_VERSION}"),
        )
        registry = software.get("dockerRegistry", "")
        base_image = software.get("pythonImage", DEFAULT_BASE_IMAGE)
        if registry:
            base_image = f"{registry}/{base_image}"

        container = software.get("dataJuicerContainer", DEFAULT_CONTAINER)
        version = software.get("dataJuicerVersion", DEFAULT_VERSION)
        benchmark_url = software.get("benchmarkDataUrl", DEFAULT_DATA_URL)
        benchmark_mirror = software.get("benchmarkDataMirrorUrl", "")
        benchmark_text_url = software.get("benchmarkTextDataUrl", "")
        benchmark_use_full_archive = _bool_env(
            software.get("benchmarkUseFullArchive", False)
        )
        benchmark_text_rows = software.get("benchmarkTextRows", "")
        modalities = _cpu_modalities(software.get("benchmarkModalities", ["text"]))
        modalities_csv = ",".join(modalities)
        hf_endpoint = software.get("hfEndpoint", "")
        cache_home = software.get("dataJuicerCacheHome", "/root/.cache/data_juicer")
        model_cache = software.get(
            "dataJuicerModelsCache", "/root/.cache/data_juicer/models"
        )
        asset_cache = software.get(
            "dataJuicerAssetsCache", "/root/.cache/data_juicer/assets"
        )

        pip_trusted_hosts = software.get("pipTrustedHosts", "")
        if isinstance(pip_trusted_hosts, list):
            pip_trusted_hosts = " ".join(str(host) for host in pip_trusted_hosts)

        build_env = _env_assignments({
            "IMAGE_NAME": image,
            "BASE_IMAGE": base_image,
            "DATA_JUICER_VERSION": version,
            "PY_SPY_VERSION": software.get("pySpyVersion", ""),
            "APT_MIRROR": software.get("aptMirror", ""),
            "APT_SECURITY_MIRROR": software.get("aptSecurityMirror", ""),
            "PIP_INDEX_URL": software.get("pipIndexUrl", ""),
            "PIP_EXTRA_INDEX_URL": software.get("pipExtraIndexUrl", ""),
            "PIP_TRUSTED_HOST": pip_trusted_hosts,
            "PIP_TIMEOUT": software.get("pipTimeout", ""),
            "PIP_RETRIES": software.get("pipRetries", ""),
            "HF_ENDPOINT": hf_endpoint,
            **_proxy_env(host_env),
        })
        steps.append(PlanStep(
            id="build-datajuicer-image",
            kind="build",
            hostRef=host,
            command=(
                f"docker image inspect {image} >/dev/null 2>&1 "
                f"|| {build_env} bash /tmp/build-datajuicer-image.sh {arch}"
            ),
            description=f"Build Data-Juicer CPU benchmark image on {host_alias}",
            mutatesHost=True,
            requiresApproval=True,
            rollbackHint=f"docker rmi {image}",
            scriptPath="adapters/datajuicer/scripts/build-datajuicer-image.sh",
            timeout=3600,
        ))

        steps.append(PlanStep(
            id="create-datajuicer-cache-volume",
            kind="prepare",
            hostRef=host,
            command="docker volume create data-juicer-cache >/dev/null",
            description=f"Create persistent Data-Juicer cache volume on {host_alias}",
            mutatesHost=True,
            rollbackHint="docker volume rm data-juicer-cache",
        ))

        run_env = _docker_env_flags({
            **_proxy_env(host_env),
            "DATA_JUICER_BENCH_MODALITIES": modalities_csv,
            "DATA_JUICER_BENCH_DATA_URL": benchmark_url,
            "DATA_JUICER_BENCH_DATA_MIRROR_URL": benchmark_mirror,
            "DATA_JUICER_BENCH_TEXT_URL": benchmark_text_url,
            "DATA_JUICER_BENCH_USE_FULL_ARCHIVE": benchmark_use_full_archive,
            "DATA_JUICER_BENCH_TEXT_ROWS": benchmark_text_rows,
            "DATA_JUICER_CACHE_HOME": cache_home,
            "DATA_JUICER_MODELS_CACHE": model_cache,
            "DATA_JUICER_ASSETS_CACHE": asset_cache,
            "HF_ENDPOINT": hf_endpoint,
        })
        run_args = (
            f"docker run -d --name {container} --privileged "
            f"-v data-juicer-cache:/root/.cache {run_env} "
            f"{image} sleep infinity"
        )
        steps.append(PlanStep(
            id="start-datajuicer",
            kind="framework-start",
            hostRef=host,
            command=_docker_reconcile_container(
                name=container,
                image=image,
                run_args=run_args,
                require_privileged=True,
            ),
            description=f"Start Data-Juicer benchmark container on {host_alias}",
            mutatesHost=True,
            requiresApproval=True,
            rollbackHint=f"docker rm -f {container}",
        ))

        steps.append(PlanStep(
            id="readiness-datajuicer",
            kind="framework-readiness",
            hostRef=host,
            command=(
                f"docker exec {container} python -c "
                f"'import data_juicer; print(data_juicer.__version__)' "
                f"&& docker exec {container} dj-process --help >/dev/null"
            ),
            description=f"Verify Data-Juicer import and CLI on {host_alias}",
            timeout=120,
        ))

        profiling_tools = software.get("profilingTools", [])
        verify_tools = ["command -v perf", "command -v objdump"]
        if _python_flamegraph_enabled(software) or "py-spy" in profiling_tools:
            verify_tools.append("command -v py-spy")
        verify_tools.append("python --version")

        steps.append(PlanStep(
            id="verify-datajuicer-perf-tools",
            kind="framework-readiness",
            hostRef=host,
            command=(
                f"docker exec {container} bash -c "
                f"'{_join_shell_checks(verify_tools)}'"
            ),
            description=f"Verify perf and objdump in {container} on {host_alias}",
        ))

        if "perf" in profiling_tools:
            steps.append(PlanStep(
                id="enable-perf-paranoid",
                kind="prepare",
                hostRef=host,
                command="sudo sysctl -w kernel.perf_event_paranoid=0",
                description=f"Set kernel.perf_event_paranoid=0 on {host_alias}",
                requiresPrivilege=True,
                requiresApproval=True,
                rollbackHint="sudo sysctl -w kernel.perf_event_paranoid=2",
            ))

        return steps


def _cpu_modalities(raw: Any) -> list[str]:
    if raw in ("", None):
        values = ["text"]
    elif isinstance(raw, list):
        values = [str(item).strip() for item in raw]
    else:
        values = [item.strip() for item in str(raw).replace(",", " ").split()]
    selected = [item for item in values if item in CPU_MODALITIES]
    return selected or ["text"]


def _python_flamegraph_enabled(software: dict[str, Any]) -> bool:
    config = software.get("pythonFlamegraph", {})
    if isinstance(config, dict):
        return _bool_env(config.get("enabled", False)) == "true"
    return _bool_env(config) == "true"


def _join_shell_checks(checks: list[str]) -> str:
    return " && ".join(checks)


def _proxy_env(host_env: dict[str, Any]) -> dict[str, Any]:
    names = [
        "http_proxy",
        "https_proxy",
        "no_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
    ]
    return {name: host_env.get(name, "") for name in names}


def _bool_env(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    raw = str(value).strip().lower()
    return "true" if raw in {"1", "true", "yes", "on"} else "false"


def _env_assignments(values: dict[str, Any]) -> str:
    return " ".join(
        f"{name}={shlex.quote(str(value))}"
        for name, value in values.items()
        if value not in ("", None)
    )


def _docker_env_flags(values: dict[str, Any]) -> str:
    return " ".join(
        f"-e {name}={shlex.quote(str(value))}"
        for name, value in values.items()
        if value not in ("", None)
    )


def _docker_reconcile_container(
    name: str,
    image: str,
    run_args: str,
    require_privileged: bool = False,
) -> str:
    checks = "recreate=0; "
    if require_privileged:
        checks += (
            f"priv=$(docker inspect -f '{{{{.HostConfig.Privileged}}}}' {name}); "
            f"if [ \"$priv\" != \"true\" ]; then "
            f"echo Recreating {name} with --privileged; "
            f"recreate=1; fi; "
        )
    checks += f"if [ \"$recreate\" = \"1\" ]; then docker rm -f {name}; fi; "
    return (
        f"if docker inspect {name} >/dev/null 2>&1; then "
        f"{checks}"
        f"fi; "
        f"if docker inspect {name} >/dev/null 2>&1; then "
        f"current=$(docker inspect -f '{{{{.Config.Image}}}}' {name}); "
        f"if [ \"$current\" = \"{image}\" ]; then "
        f"state=$(docker inspect -f '{{{{.State.Running}}}}' {name}); "
        f"if [ \"$state\" = \"true\" ]; then "
        f"echo {name} already running with {image}; "
        f"else docker start {name}; fi; "
        f"else docker rm -f {name} && {run_args}; fi; "
        f"else {run_args}; fi"
    )
