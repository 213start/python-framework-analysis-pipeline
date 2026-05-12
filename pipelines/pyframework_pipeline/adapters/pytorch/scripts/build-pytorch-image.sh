#!/usr/bin/env bash
# build-pytorch-image.sh — Build openEuler + Python + PyTorch CPU Docker image
#
# Usage: ./build-pytorch-image.sh [ARCH]
#   ARCH = x86_64 (default) or aarch64
#
# Runs on the target host.  Values are supplied through environment variables
# by the pipeline environment adapter.

set -euo pipefail

ARCH="${1:-x86_64}"
if [ "$ARCH" = "aarch64" ]; then
    ARCH_TAG="arm"
else
    ARCH_TAG="x86"
fi

BASE_IMAGE="${BASE_IMAGE:-openeuler/openeuler:24.03-lts-sp3}"
IMAGE_NAME="${IMAGE_NAME:-pytorch-inductor:2.10.0-py314-openeuler2403sp3-${ARCH_TAG}-final}"
PYTHON_VERSION="${PYTHON_VERSION:-3.14.3}"
TORCH_VERSION="${TORCH_VERSION:-2.10.0+cpu}"
TORCHAUDIO_VERSION="${TORCHAUDIO_VERSION:-2.10.0+cpu}"
TORCHVISION_VERSION="${TORCHVISION_VERSION:-0.25.0+cpu}"
PYTORCH_INDEX_URL="${PYTORCH_INDEX_URL:-https://download.pytorch.org/whl/cpu}"
PYTORCH_WHEEL_BASE_URL="${PYTORCH_WHEEL_BASE_URL:-}"
PYTORCH_WHEEL_PLATFORM_TAG="${PYTORCH_WHEEL_PLATFORM_TAG:-}"
PYTHON_SOURCE_URL="${PYTHON_SOURCE_URL:-https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tar.xz}"
PIP_BOOTSTRAP_INDEX_URL="${PIP_BOOTSTRAP_INDEX_URL:-https://pypi.org/simple}"
MAKEOPTS="${MAKEOPTS:--j$(($(nproc 2>/dev/null || echo 4) / 2))}"
PYENV_ROOT="/root/.pyenv"
PYTHON="$PYENV_ROOT/versions/$PYTHON_VERSION/bin/python3"
PIP="$PYENV_ROOT/versions/$PYTHON_VERSION/bin/pip --trusted-host pypi.org --trusted-host files.pythonhosted.org --trusted-host pypi.python.org --trusted-host download.pytorch.org --trusted-host mirrors.aliyun.com --trusted-host mirrors.huaweicloud.com --trusted-host pypi.tuna.tsinghua.edu.cn"
BUILD_CONTAINER="pytorch-build"

if [ "$ARCH" = "aarch64" ]; then
    PYTORCH_DIRECT_WHEEL_BASE_URL="$PYTORCH_WHEEL_BASE_URL"
    PYTORCH_DIRECT_WHEEL_PLATFORM_TAG="${PYTORCH_WHEEL_PLATFORM_TAG:-manylinux_2_28_aarch64}"
else
    PYTORCH_DIRECT_WHEEL_BASE_URL="$PYTORCH_WHEEL_BASE_URL"
    PYTORCH_DIRECT_WHEEL_PLATFORM_TAG="${PYTORCH_WHEEL_PLATFORM_TAG:-manylinux_2_28_x86_64}"
fi

# Forward proxy env vars into docker exec calls (container does not inherit host env).
DOCKER_PROXY_FLAGS=""
for var in http_proxy https_proxy no_proxy HTTP_PROXY HTTPS_PROXY NO_PROXY; do
    val="${!var:-}"
    if [ -n "$val" ]; then
        DOCKER_PROXY_FLAGS="$DOCKER_PROXY_FLAGS -e $var=$val"
    fi
done

echo "=== Building $IMAGE_NAME on $ARCH ==="
echo "  BASE_IMAGE=$BASE_IMAGE"
echo "  PYTHON_VERSION=$PYTHON_VERSION"
echo "  TORCH_VERSION=$TORCH_VERSION"
echo "  TORCHAUDIO_VERSION=$TORCHAUDIO_VERSION"
echo "  TORCHVISION_VERSION=$TORCHVISION_VERSION"
echo "  PYTORCH_INDEX_URL=$PYTORCH_INDEX_URL"
echo "  PYTORCH_WHEEL_BASE_URL=$PYTORCH_WHEEL_BASE_URL"
echo "  PYTORCH_DIRECT_WHEEL_BASE_URL=$PYTORCH_DIRECT_WHEEL_BASE_URL"
echo "  PYTORCH_DIRECT_WHEEL_PLATFORM_TAG=$PYTORCH_DIRECT_WHEEL_PLATFORM_TAG"
echo "  PYTHON_SOURCE_URL=$PYTHON_SOURCE_URL"
echo "  PIP_BOOTSTRAP_INDEX_URL=$PIP_BOOTSTRAP_INDEX_URL"
echo "  BUILD_CONTAINER=$BUILD_CONTAINER"

# ---------------------------------------------------------------------------
# Phase 1: Start or reuse build container
# ---------------------------------------------------------------------------
echo ""
if docker inspect "$BUILD_CONTAINER" >/dev/null 2>&1; then
    echo "[Phase 1/6] Reusing existing build container '$BUILD_CONTAINER'..."
    docker start "$BUILD_CONTAINER" 2>/dev/null || true
else
    echo "[Phase 1/6] Creating build container '$BUILD_CONTAINER'..."
    docker run -d --name "$BUILD_CONTAINER" --hostname pytorch-build \
        "$BASE_IMAGE" sleep infinity
fi
sleep 3

# ---------------------------------------------------------------------------
# Phase 2: Install system deps + compile Python
# ---------------------------------------------------------------------------
echo ""
echo "[Phase 2/6] Installing build deps and compiling Python $PYTHON_VERSION..."
echo "  (This takes ~40-60 min with LTO+PGO on 4 cores)"

docker exec $DOCKER_PROXY_FLAGS -u root "$BUILD_CONTAINER" bash -c "
set -e

retry() {
    local attempts=5 delay=10 cmd=\"\$@\"
    for i in \$(seq 1 \$attempts); do
        echo \"    [retry \$i/\$attempts] \$cmd\"
        if eval \$cmd; then return 0; fi
        echo '    Failed, waiting '\$delay's...'
        sleep \$delay
    done
    echo '    All retries exhausted.'
    return 1
}

# openEuler 24.03 uses dnf.  Package names intentionally stay broad so the
# script works on both x86_64 and aarch64 images.
# Some lab networks terminate TLS with an internal/self-signed CA.  The base
# image does not know that CA yet, so disable dnf SSL verification before the
# first metadata download.  curl/wget/pip are relaxed below for the same reason.
grep -q '^sslverify=False' /etc/dnf/dnf.conf 2>/dev/null || echo 'sslverify=False' >> /etc/dnf/dnf.conf
for repo_file in /etc/yum.repos.d/*.repo; do
    [ -f "\$repo_file" ] && sed -i 's/^sslverify=.*/sslverify=False/' "\$repo_file" || true
done
dnf makecache -y || true
dnf install -y \
    gcc gcc-c++ make patch bzip2 bzip2-devel zlib-devel \
    openssl-devel readline-devel sqlite-devel libffi-devel xz-devel \
    git curl wget ca-certificates findutils tar gzip shadow-utils \
    perf strace binutils gdb || exit 1
echo '  System deps installed'

# Make common tooling more tolerant of corporate TLS interception.
git config --global http.sslVerify false
git config --global http.version HTTP/1.1
echo 'insecure' >> ~/.curlrc
echo 'check_certificate = off' >> ~/.wgetrc

if [ ! -d $PYENV_ROOT ]; then
    echo '  Installing pyenv...'
    retry 'git clone https://github.com/pyenv/pyenv.git $PYENV_ROOT'
else
    echo '  pyenv already exists'
fi
export PYENV_ROOT=$PYENV_ROOT
export PATH=\$PYENV_ROOT/bin:\$PATH
eval \"\$(pyenv init -)\"

if [ ! -x $PYTHON ]; then
    echo '  Pre-caching Python $PYTHON_VERSION source...'
    mkdir -p \$PYENV_ROOT/cache
    if [ -s \$PYENV_ROOT/cache/Python-$PYTHON_VERSION.tar.xz ] && tar -tf \$PYENV_ROOT/cache/Python-$PYTHON_VERSION.tar.xz >/dev/null 2>&1; then
        echo '  Python source cache already exists'
    else
        rm -f \$PYENV_ROOT/cache/Python-$PYTHON_VERSION.tar.xz
        retry 'curl -k -o \$PYENV_ROOT/cache/Python-$PYTHON_VERSION.tar.xz $PYTHON_SOURCE_URL'
    fi

    echo '  Compiling Python (LTO+PGO)...'
    CFLAGS='-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer' \
    PYTHON_CONFIGURE_OPTS='--enable-optimizations --with-lto' \
    PYTHON_BUILD_CURL_OPTS='-k' \
    MAKEOPTS='$MAKEOPTS' \
    pyenv install $PYTHON_VERSION
else
    echo '  Python already compiled'
fi

pyenv global $PYTHON_VERSION
echo '  Python:' \$(python3 --version)
"

echo "[Phase 2/6] Done."

# ---------------------------------------------------------------------------
# Phase 3: Fix pip truststore + install build helpers
# ---------------------------------------------------------------------------
echo ""
echo "[Phase 3/6] Fixing pip and installing build helpers..."

docker exec $DOCKER_PROXY_FLAGS -u root "$BUILD_CONTAINER" bash -c "
set -e
export PYENV_ROOT=$PYENV_ROOT
export PATH=\$PYENV_ROOT/bin:\$PYENV_ROOT/versions/$PYTHON_VERSION/bin:\$PATH
eval \"\$(pyenv init -)\"
pyenv global $PYTHON_VERSION

$PYTHON << 'PYEOF'
import os, shutil
try:
    import pip._internal.cli.index_command as m
    path = m.__file__
    with open(path) as f:
        lines = f.readlines()
    new_lines = []
    skip = False
    for line in lines:
        if line.startswith('def _create_truststore_ssl_context'):
            new_lines.append('def _create_truststore_ssl_context() -> None:\n')
            new_lines.append('    return None\n')
            skip = True
            continue
        if skip:
            if line and not line[0].isspace() and line.strip():
                skip = False
                new_lines.append(line)
        else:
            new_lines.append(line)
    with open(path, 'w') as f:
        f.writelines(new_lines)
    print('  Patched pip truststore')
except Exception as exc:
    print('  WARNING: could not patch pip truststore:', exc)

try:
    import pip._vendor.certifi as pc
    ca = '/etc/pki/tls/certs/ca-bundle.crt'
    if not os.path.exists(ca):
        ca = '/etc/ssl/certs/ca-certificates.crt'
    shutil.copy2(ca, os.path.join(os.path.dirname(pc.__file__), 'cacert.pem'))
    print('  Fixed pip certifi cacert.pem')
except Exception as exc:
    print('  WARNING: could not fix pip certifi:', exc)
PYEOF

$PIP install --index-url $PIP_BOOTSTRAP_INDEX_URL --upgrade pip setuptools wheel
"

echo "[Phase 3/6] Done."

# ---------------------------------------------------------------------------
# Phase 4: Install PyTorch CPU packages
# ---------------------------------------------------------------------------
echo ""
echo "[Phase 4/6] Installing PyTorch CPU packages..."

docker exec $DOCKER_PROXY_FLAGS -u root "$BUILD_CONTAINER" bash -c "
set -e
export PYENV_ROOT=$PYENV_ROOT
export PATH=\$PYENV_ROOT/bin:\$PYENV_ROOT/versions/$PYTHON_VERSION/bin:\$PATH

if [ -n "$PYTORCH_DIRECT_WHEEL_BASE_URL" ]; then
    $PIP install \
        "$PYTORCH_DIRECT_WHEEL_BASE_URL/torch-${TORCH_VERSION/+/%2B}-cp314-cp314-$PYTORCH_DIRECT_WHEEL_PLATFORM_TAG.whl" \
        "$PYTORCH_DIRECT_WHEEL_BASE_URL/torchaudio-${TORCHAUDIO_VERSION/+/%2B}-cp314-cp314-$PYTORCH_DIRECT_WHEEL_PLATFORM_TAG.whl" \
        "$PYTORCH_DIRECT_WHEEL_BASE_URL/torchvision-${TORCHVISION_VERSION/+/%2B}-cp314-cp314-$PYTORCH_DIRECT_WHEEL_PLATFORM_TAG.whl" \
        --index-url $PIP_BOOTSTRAP_INDEX_URL
else
    $PIP install \
        torch==$TORCH_VERSION \
        torchaudio==$TORCHAUDIO_VERSION \
        torchvision==$TORCHVISION_VERSION \
        --index-url $PYTORCH_INDEX_URL \
        --extra-index-url $PIP_BOOTSTRAP_INDEX_URL
fi
"

echo "[Phase 4/6] Done."

# ---------------------------------------------------------------------------
# Phase 5: Verify and prepare runtime image
# ---------------------------------------------------------------------------
echo ""
echo "[Phase 5/6] Verifying PyTorch and runtime tools..."

docker exec $DOCKER_PROXY_FLAGS -u root "$BUILD_CONTAINER" bash -c "
set -e
export PYENV_ROOT=$PYENV_ROOT
export PATH=\$PYENV_ROOT/versions/$PYTHON_VERSION/bin:\$PATH

$PYTHON << PYEOF
import torch, torchaudio, torchvision
print('  torch:', torch.__version__)
print('  torchaudio:', torchaudio.__version__)
print('  torchvision:', torchvision.__version__)
assert torch.__version__ == '$TORCH_VERSION'
assert torchaudio.__version__ == '$TORCHAUDIO_VERSION'
assert torchvision.__version__ == '$TORCHVISION_VERSION'
print('  PyTorch CPU packages OK')
PYEOF

mkdir -p /home/w30063991 /opt/pytorch-workload /opt/pytorch-results
chmod 777 /home/w30063991 /opt/pytorch-workload /opt/pytorch-results

ln -sf $PYENV_ROOT/versions/$PYTHON_VERSION/bin/python3 /usr/local/bin/python3
ln -sf $PYENV_ROOT/versions/$PYTHON_VERSION/bin/pip /usr/local/bin/pip

# Keep tools on PATH. openEuler's perf package normally installs /usr/bin/perf.
perf --version
strace --version | head -1
objdump --version | head -1
gdb --version | head -1
readelf --version | head -1
gcc --version | head -1
g++ --version | head -1

$PYENV_ROOT/versions/$PYTHON_VERSION/bin/pip cache purge || true
rm -rf /tmp/pip-* /tmp/python-build.* /tmp/*.whl
"

echo "[Phase 5/6] Done."

# ---------------------------------------------------------------------------
# Phase 6: Commit image
# ---------------------------------------------------------------------------
echo ""
echo "[Phase 6/6] Committing image..."
docker commit "$BUILD_CONTAINER" "$IMAGE_NAME"
echo "  Committed image: $IMAGE_NAME"

echo ""
echo "=== BUILD COMPLETE ==="
echo "Image: $IMAGE_NAME"
echo "Python: $PYTHON_VERSION"
echo "PyTorch: $TORCH_VERSION"
echo "Ready for PyTorch Inductor benchmark."
