#!/usr/bin/env bash
set -euo pipefail

ARCH_TAG="${1:-$(uname -m)}"
IMAGE_NAME="${IMAGE_NAME:-data-juicer-bench:1.5.2-py311-${ARCH_TAG}}"
BASE_IMAGE="${BASE_IMAGE:-python:3.11-slim}"
DATA_JUICER_VERSION="${DATA_JUICER_VERSION:-1.5.2}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

cat >"$tmpdir/Dockerfile" <<'DOCKERFILE'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG DATA_JUICER_VERSION
ARG HF_ENDPOINT
ARG APT_MIRROR
ARG APT_SECURITY_MIRROR
ARG PIP_INDEX_URL
ARG PIP_EXTRA_INDEX_URL
ARG PIP_TRUSTED_HOST
ARG PIP_TIMEOUT
ARG PIP_RETRIES

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
        ca-certificates \
        curl \
        wget \
        git \
        binutils \
        procps \
        linux-perf; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    python -m pip install --upgrade pip setuptools wheel; \
    python -m pip install \
        "py-data-juicer==${DATA_JUICER_VERSION}" \
        "transformers==4.57.1" \
        sentencepiece; \
    python -c "import data_juicer, transformers; print('data_juicer', data_juicer.__version__); print('transformers', transformers.__version__)"

WORKDIR /workspace/benchmark
CMD ["sleep", "infinity"]
DOCKERFILE

build_args=(
  --build-arg "BASE_IMAGE=${BASE_IMAGE}"
  --build-arg "DATA_JUICER_VERSION=${DATA_JUICER_VERSION}"
  --build-arg "HF_ENDPOINT=${HF_ENDPOINT:-}"
  --build-arg "APT_MIRROR=${APT_MIRROR:-}"
  --build-arg "APT_SECURITY_MIRROR=${APT_SECURITY_MIRROR:-}"
  --build-arg "PIP_INDEX_URL=${PIP_INDEX_URL:-}"
  --build-arg "PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL:-}"
  --build-arg "PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST:-}"
  --build-arg "PIP_TIMEOUT=${PIP_TIMEOUT:-}"
  --build-arg "PIP_RETRIES=${PIP_RETRIES:-}"
)

echo "Building ${IMAGE_NAME} from ${BASE_IMAGE} for ${ARCH_TAG}"
docker build "${build_args[@]}" -t "${IMAGE_NAME}" "$tmpdir"

echo "Built ${IMAGE_NAME}"
