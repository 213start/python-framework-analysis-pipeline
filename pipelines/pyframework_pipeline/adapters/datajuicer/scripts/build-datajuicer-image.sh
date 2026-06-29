#!/usr/bin/env bash
set -euo pipefail

ARCH_TAG="${1:-$(uname -m)}"
IMAGE_NAME="${IMAGE_NAME:-data-juicer-bench:1.5.2-py311-${ARCH_TAG}}"
BASE_IMAGE="${BASE_IMAGE:-python:3.11-slim}"
DATA_JUICER_VERSION="${DATA_JUICER_VERSION:-1.5.2}"
PY_SPY_VERSION="${PY_SPY_VERSION:-}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

cat >"$tmpdir/Dockerfile" <<'DOCKERFILE'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG DATA_JUICER_VERSION
ARG PY_SPY_VERSION
ARG HF_ENDPOINT
ARG APT_MIRROR
ARG APT_SECURITY_MIRROR
ARG PIP_INDEX_URL
ARG PIP_EXTRA_INDEX_URL
ARG PIP_TRUSTED_HOST
ARG PIP_TIMEOUT
ARG PIP_RETRIES
ARG http_proxy
ARG https_proxy
ARG no_proxy
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY

ENV PYTHONUNBUFFERED=1 \
    HF_ENDPOINT=${HF_ENDPOINT} \
    PIP_INDEX_URL=${PIP_INDEX_URL} \
    PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL} \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} \
    PIP_TIMEOUT=${PIP_TIMEOUT} \
    PIP_RETRIES=${PIP_RETRIES} \
    DATA_JUICER_CACHE_HOME=/root/.cache/data_juicer \
    DATA_JUICER_MODELS_CACHE=/root/.cache/data_juicer/models \
    DATA_JUICER_ASSETS_CACHE=/root/.cache/data_juicer/assets

RUN set -eux; \
    write_debian_sources() { \
        mirror="$1"; \
        security_mirror="$2"; \
        codename="$(. /etc/os-release && printf '%s' "${VERSION_CODENAME:-bookworm}")"; \
        printf 'Types: deb\nURIs: %s\nSuites: %s %s-updates\nComponents: main\nSigned-By: /usr/share/keyrings/debian-archive-keyring.gpg\n\nTypes: deb\nURIs: %s\nSuites: %s-security\nComponents: main\nSigned-By: /usr/share/keyrings/debian-archive-keyring.gpg\n' "$mirror" "$codename" "$codename" "$security_mirror" "$codename" > /etc/apt/sources.list.d/debian.sources; \
        rm -f /etc/apt/sources.list; \
    }; \
    if [ -n "${APT_MIRROR}" ]; then \
        security_mirror="${APT_SECURITY_MIRROR:-${APT_MIRROR%/}-security}"; \
        write_debian_sources "${APT_MIRROR}" "${security_mirror}"; \
    fi; \
    printf '%s\n' \
        'Acquire::http::Timeout "30";' \
        'Acquire::https::Timeout "30";' \
        'Acquire::https::Verify-Peer "false";' \
        'Acquire::https::Verify-Host "false";' \
        'Acquire::Retries "5";' \
        > /etc/apt/apt.conf.d/99pyframework-timeouts; \
    if ! apt-get update; then \
        echo "Configured apt mirror failed; retrying default Debian mirror"; \
        write_debian_sources "https://deb.debian.org/debian" "https://deb.debian.org/debian-security"; \
        apt-get update; \
    fi; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        wget \
        git \
        binutils \
        procps \
        linux-perf; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    pip_trusted_hosts="${PIP_TRUSTED_HOST:-} pypi.org files.pythonhosted.org"; \
    pip_trusted_args=""; \
    for host in $pip_trusted_hosts; do \
        pip_trusted_args="$pip_trusted_args --trusted-host $host"; \
    done; \
    python -m pip install $pip_trusted_args --upgrade pip setuptools wheel; \
    python -m pip install $pip_trusted_args \
        "py-data-juicer==${DATA_JUICER_VERSION}" \
        "transformers==4.57.1" \
        sentencepiece; \
    if [ -n "${PY_SPY_VERSION}" ]; then \
        python -m pip install $pip_trusted_args "py-spy==${PY_SPY_VERSION}"; \
    else \
        python -m pip install $pip_trusted_args py-spy; \
    fi; \
    python -c "import data_juicer, transformers; print('data_juicer', data_juicer.__version__); print('transformers', transformers.__version__)"; \
    py-spy --version

WORKDIR /workspace/benchmark
CMD ["sleep", "infinity"]
DOCKERFILE

build_args=(
  --build-arg "BASE_IMAGE=${BASE_IMAGE}"
  --build-arg "DATA_JUICER_VERSION=${DATA_JUICER_VERSION}"
  --build-arg "PY_SPY_VERSION=${PY_SPY_VERSION:-}"
  --build-arg "HF_ENDPOINT=${HF_ENDPOINT:-}"
  --build-arg "APT_MIRROR=${APT_MIRROR:-}"
  --build-arg "APT_SECURITY_MIRROR=${APT_SECURITY_MIRROR:-}"
  --build-arg "PIP_INDEX_URL=${PIP_INDEX_URL:-}"
  --build-arg "PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL:-}"
  --build-arg "PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST:-}"
  --build-arg "PIP_TIMEOUT=${PIP_TIMEOUT:-}"
  --build-arg "PIP_RETRIES=${PIP_RETRIES:-}"
  --build-arg "http_proxy=${http_proxy:-}"
  --build-arg "https_proxy=${https_proxy:-}"
  --build-arg "no_proxy=${no_proxy:-}"
  --build-arg "HTTP_PROXY=${HTTP_PROXY:-}"
  --build-arg "HTTPS_PROXY=${HTTPS_PROXY:-}"
  --build-arg "NO_PROXY=${NO_PROXY:-}"
)

echo "Building ${IMAGE_NAME} from ${BASE_IMAGE} for ${ARCH_TAG}"
docker build "${build_args[@]}" -t "${IMAGE_NAME}" "$tmpdir"

echo "Built ${IMAGE_NAME}"
