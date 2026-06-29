#!/usr/bin/env bash
set -euo pipefail

ARCH_TAG="${1:-$(uname -m)}"
IMAGE_NAME="${IMAGE_NAME:-udf-benchmarking-bench:py311-${ARCH_TAG}}"
BASE_IMAGE="${BASE_IMAGE:-python:3.11-slim}"
UDF_BENCHMARKING_REPO="${UDF_BENCHMARKING_REPO:-https://gitcode.com/stone31415/UDF_Benchmarking.git}"
UDF_BENCHMARKING_REVISION="${UDF_BENCHMARKING_REVISION:-}"
PY_SPY_VERSION="${PY_SPY_VERSION:-}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

cat >"$tmpdir/Dockerfile" <<'DOCKERFILE'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG UDF_BENCHMARKING_REPO
ARG UDF_BENCHMARKING_REVISION
ARG PY_SPY_VERSION
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
    PIP_INDEX_URL=${PIP_INDEX_URL} \
    PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL} \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} \
    PIP_TIMEOUT=${PIP_TIMEOUT} \
    PIP_RETRIES=${PIP_RETRIES}

RUN set -eux; \
    if [ -n "${APT_MIRROR}" ]; then \
        security_mirror="${APT_SECURITY_MIRROR:-${APT_MIRROR%/}-security}"; \
        if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
            sed -i \
                -e "s|http://deb.debian.org/debian-security|${security_mirror}|g" \
                -e "s|http://deb.debian.org/debian|${APT_MIRROR}|g" \
                /etc/apt/sources.list.d/debian.sources; \
        fi; \
        if [ -f /etc/apt/sources.list ]; then \
            sed -i \
                -e "s|http://deb.debian.org/debian-security|${security_mirror}|g" \
                -e "s|http://deb.debian.org/debian|${APT_MIRROR}|g" \
                /etc/apt/sources.list; \
        fi; \
    fi; \
    printf '%s\n' \
        'Acquire::http::Timeout "30";' \
        'Acquire::https::Timeout "30";' \
        'Acquire::Retries "5";' \
        > /etc/apt/apt.conf.d/99pyframework-timeouts; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        bash \
        binutils \
        ca-certificates \
        curl \
        git \
        linux-perf \
        numactl \
        procps \
        libglib2.0-0 \
        libgl1 \
        libgomp1; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    python -m pip install --upgrade pip setuptools wheel; \
    python -m pip install \
        getdaft \
        numpy \
        opencv-python-headless \
        psutil \
        pyyaml \
        scikit-image; \
    if [ -n "${PY_SPY_VERSION}" ]; then \
        python -m pip install "py-spy==${PY_SPY_VERSION}"; \
    else \
        python -m pip install py-spy; \
    fi; \
    python -c "import daft, cv2, skimage, psutil, yaml; print('udfbenchmarking dependencies ready')"; \
    py-spy --version

RUN set -eux; \
    git clone --depth 1 "${UDF_BENCHMARKING_REPO}" /opt/UDF_Benchmarking; \
    if [ -n "${UDF_BENCHMARKING_REVISION}" ]; then \
        cd /opt/UDF_Benchmarking; \
        git checkout "${UDF_BENCHMARKING_REVISION}" || \
            (git fetch --depth 1 origin "${UDF_BENCHMARKING_REVISION}" && git checkout "${UDF_BENCHMARKING_REVISION}"); \
    fi; \
    cd /opt/UDF_Benchmarking; \
    python -c "import pathlib; assert pathlib.Path('main.py').is_file(); print('UDF_Benchmarking source ready')"

WORKDIR /workspace/benchmark
CMD ["sleep", "infinity"]
DOCKERFILE

build_args=(
  --build-arg "BASE_IMAGE=${BASE_IMAGE}"
  --build-arg "UDF_BENCHMARKING_REPO=${UDF_BENCHMARKING_REPO}"
  --build-arg "UDF_BENCHMARKING_REVISION=${UDF_BENCHMARKING_REVISION:-}"
  --build-arg "PY_SPY_VERSION=${PY_SPY_VERSION:-}"
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
