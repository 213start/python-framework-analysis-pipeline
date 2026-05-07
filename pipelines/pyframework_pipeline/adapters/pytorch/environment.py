"""PyTorch environment adapter.

Declares framework-specific Docker setup steps for PyTorch Inductor
profiling.  Unlike the PyFlink adapter this is a single-container runtime:
Python is compiled in the container image, PyTorch CPU wheels are installed,
and the container stays alive for workload deployment and benchmark runs.
"""

from __future__ import annotations

from typing import Any

from pyframework_pipeline.environment.planning import PlanStep

DEFAULT_BASE_IMAGE = "openeuler/openeuler:24.03-lts-sp3"
DEFAULT_NETWORK = "pytorch-network"
DEFAULT_CONTAINER = "pytorch-runner"


class PyTorchEnvironmentAdapter:
    """Generate PyTorch-specific environment plan steps."""

    framework_id = "pytorch"

    def get_plan_steps(
        self,
        platform: str,
        platform_config: dict[str, Any],
        software: dict[str, Any],
        host_refs: dict[str, Any],
    ) -> list[PlanStep]:
        steps: list[PlanStep] = []

        image = software.get("pytorchImages", {}).get(
            platform,
            software.get("pytorchImage", _default_image(platform_config.get("arch", "x86_64"))),
        )
        registry = software.get("dockerRegistry", "")
        if registry:
            image = f"{registry}/{image}"

        base_image = software.get("baseImage", DEFAULT_BASE_IMAGE)
        if registry and "/" not in base_image.split(":", 1)[0]:
            base_image = f"{registry}/{base_image}"

        network = software.get("containerNetwork", DEFAULT_NETWORK)
        container = software.get("containerName", DEFAULT_CONTAINER)
        python_version = software.get("pythonVersion", "3.14.3")
        torch_version = software.get("torchVersion", software.get("pytorchVersion", "2.10.0+cpu"))
        torchaudio_version = software.get("torchaudioVersion", "2.10.0+cpu")
        torchvision_version = software.get("torchvisionVersion", "0.25.0+cpu")
        pytorch_index_url = software.get("pytorchIndexUrl", "https://download.pytorch.org/whl/cpu")
        python_source_url = software.get(
            "pythonSourceUrl",
            f"https://www.python.org/ftp/python/{python_version}/Python-{python_version}.tar.xz",
        )
        pip_bootstrap_index_url = software.get("pipBootstrapIndexUrl", "https://pypi.org/simple")
        pytorch_wheel_base_url = software.get("pytorchWheelBaseUrl", "")

        hosts_by_role = {}
        for host_entry in platform_config.get("hosts", []):
            hosts_by_role[host_entry["role"]] = host_entry["hostRef"]
        host = hosts_by_role.get("client", hosts_by_role.get("jobmanager", ""))
        host_alias = host_refs.get(host, {}).get("alias", host)
        arch = platform_config.get("arch", "x86_64")

        build_script = "adapters/pytorch/scripts/build-pytorch-image.sh"
        build_env = (
            f"IMAGE_NAME={image} BASE_IMAGE={base_image} "
            f"PYTHON_VERSION={python_version} "
            f"TORCH_VERSION={torch_version} "
            f"TORCHAUDIO_VERSION={torchaudio_version} "
            f"TORCHVISION_VERSION={torchvision_version} "
            f"PYTORCH_INDEX_URL={pytorch_index_url} "
            f"PYTORCH_WHEEL_BASE_URL={pytorch_wheel_base_url} "
            f"PYTHON_SOURCE_URL={python_source_url} "
            f"PIP_BOOTSTRAP_INDEX_URL={pip_bootstrap_index_url}"
        )
        steps.append(PlanStep(
            id="build-pytorch-image",
            kind="build",
            hostRef=host,
            command=(
                f"docker image inspect {image} >/dev/null 2>&1 "
                f"|| {build_env} bash /tmp/build-pytorch-image.sh {arch}"
            ),
            description=f"Build openEuler+Python+PyTorch image on {host_alias} (~80 min first run)",
            mutatesHost=True,
            requiresApproval=True,
            rollbackHint=f"docker rmi {image}",
            scriptPath=build_script,
            timeout=6000,
        ))

        steps.append(PlanStep(
            id="create-network",
            kind="prepare",
            hostRef=host,
            command=f"docker network create {network} 2>/dev/null || true",
            description=f"Create Docker network '{network}' on {host_alias}",
        ))

        run_args = (
            f"docker run -d --name {container} --hostname {container} --network {network} "
            "--privileged "
            "-e PYTHONPERFSUPPORT=1 "
            "-e OMP_WAIT_POLICY=PASSIVE "
            "-e GOMP_SPINCOUNT=0 "
            "-e OMP_NUM_THREADS=1 "
            "-e OPENBLAS_NUM_THREADS=1 "
            f"{image} sleep infinity"
        )
        steps.append(PlanStep(
            id="start-pytorch-runner",
            kind="framework-start",
            hostRef=host,
            command=_docker_reconcile_container(container, image, run_args),
            description=f"Start PyTorch runner container on {host_alias}",
            mutatesHost=True,
            requiresApproval=True,
            rollbackHint=f"docker rm -f {container}",
        ))

        steps.append(PlanStep(
            id="readiness-pytorch-import",
            kind="framework-smoke-test",
            hostRef=host,
            command=(
                f"docker exec {container} bash -lc "
                "'python3 - <<\"PY\"\n"
                "import torch, torchaudio, torchvision\n"
                "print(\"torch\", torch.__version__)\n"
                "print(\"torchaudio\", torchaudio.__version__)\n"
                "print(\"torchvision\", torchvision.__version__)\n"
                "assert torch.__version__ == \"" + torch_version + "\"\n"
                "assert torchaudio.__version__ == \"" + torchaudio_version + "\"\n"
                "assert torchvision.__version__ == \"" + torchvision_version + "\"\n"
                "PY'"
            ),
            description=f"Verify PyTorch CPU packages in {container} on {host_alias}",
            timeout=120,
        ))

        steps.append(PlanStep(
            id="prepare-pytorch-perf-output-dir",
            kind="framework-readiness",
            hostRef=host,
            command=f"docker exec -u root {container} bash -lc 'mkdir -p /home/w30063991 /opt/pytorch-results && chmod 777 /home/w30063991 /opt/pytorch-results'",
            description=f"Create fixed perf output directory in {container}",
        ))

        steps.append(PlanStep(
            id="verify-pytorch-build-tools",
            kind="framework-readiness",
            hostRef=host,
            command=f"docker exec {container} bash -lc 'gcc --version && g++ --version'",
            description=f"Verify gcc/g++ available for PyTorch Inductor demo in {container} on {host_alias}",
        ))

        profiling_tools = software.get("profilingTools", [])
        if profiling_tools:
            verify_cmds = {
                "perf": "perf --version",
                "strace": "strace --version",
                "objdump": "objdump --version",
                "gdb": "gdb --version",
                "readelf": "readelf --version",
            }
            verifications = " && ".join(
                verify_cmds[t] for t in profiling_tools if t in verify_cmds
            )
            if verifications:
                steps.append(PlanStep(
                    id="verify-profiling-tools",
                    kind="framework-readiness",
                    hostRef=host,
                    command=f"docker exec {container} bash -lc '{verifications}'",
                    description=f"Verify profiling tools available in {container} on {host_alias}",
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


def _default_image(arch: str) -> str:
    arch_tag = "arm" if arch == "aarch64" else "x86"
    return f"pytorch-inductor:2.10.0-py314-openeuler2403sp3-{arch_tag}-final"


def _docker_reconcile_container(name: str, image: str, run_args: str) -> str:
    return (
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
