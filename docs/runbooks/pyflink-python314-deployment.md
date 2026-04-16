# PyFlink 2.2.0 + Python 3.14.3 环境部署手册

## 1. 目标

在远程物理机上部署 Flink 2.2.0 集群（1 JM + N TM），容器内编译 Python 3.14.3（LTO+PGO），安装 PyFlink 2.2.0 及其全部依赖。

## 2. 环境假设

| 项目 | 要求 |
|------|------|
| 宿主机 | Docker 已安装，可拉取公网镜像，SSH 可达 |
| Flink 镜像 | `flink:2.2.0-java17`（官方镜像） |
| Python | 3.14.3（从源码编译） |
| 架构 | aarch64（ARM）或 x86_64 均支持 |

## 3. 总体流程

```
1. 启动 JobManager 容器
2. 在 JM 容器内编译 Python 3.14.3
3. 修复 pip SSL 问题
4. 安装 Cython 3.2.4
5. 安装 apache-beam（从源码编译 C 扩展）
6. 安装 beam 依赖链
7. 安装 apache-flink（从源码编译 C 扩展）
8. 验证 PyFlink SQL
9. docker commit 保存镜像
10. 从保存的镜像启动 TaskManager
```

## 4. 详细步骤

### 4.1 创建 Docker 网络并启动 JobManager

```bash
docker network create flink-network 2>/dev/null || true
docker pull flink:2.2.0-java17
docker run -d --name flink-jm \
  --network flink-network \
  -p 8081:8081 \
  flink:2.2.0-java17 jobmanager
```

### 4.2 在容器内安装 pyenv 并编译 Python 3.14.3

```bash
# 进入容器（以 root 身份安装编译依赖）
docker exec -it -u root flink-jm bash

# 安装编译依赖（Debian/Ubuntu 基础镜像）
apt-get update && apt-get install -y \
  build-essential libssl-dev zlib1g-dev libbz2-dev \
  libreadline-dev libsqlite3-dev libffi-dev \
  liblzma-dev git curl

# 安装 pyenv
curl https://pyenv.run | bash
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

# 编译 Python 3.14.3（LTO+PGO 优化）
# 重要：使用 -j2（物理核数的一半），LTO 链接阶段单线程需 3-4GB 内存
CFLAGS="-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer" \
PYTHON_CONFIGURE_OPTS="--enable-optimizations --with-lto" \
MAKEOPTS="-j2" \
pyenv install 3.14.3

pyenv global 3.14.3
```

**编译时间参考**：
- ARM 4C/7.2G：约 60 分钟（`-j2`）
- x86 4C/15G：约 40 分钟（`-j2`）

**离线安装**（网络慢时）：先在本地下载 Python 源码包，scp 到远程机器，放入 `$PYENV_ROOT/cache/` 目录后执行 `pyenv install`。

### 4.3 修复 pip SSL 问题

Python 3.14 的 pip truststore 模块存在兼容性问题，需要两步修复：

**步骤 1**：禁用 truststore

```python
# 创建修复脚本
cat > /tmp/fix_pip_truststore.py << 'EOF'
path = f"{__import__('os').environ.get('PYENV_ROOT', __import__('os').path.expanduser('~/.pyenv'))}/versions/3.14.3/lib/python3.14/site-packages/pip/_internal/cli/index_command.py"
with open(path) as f:
    lines = f.readlines()

new_lines = []
skip = False
for line in lines:
    if line.startswith("def _create_truststore_ssl_context"):
        new_lines.append("def _create_truststore_ssl_context() -> SSLContext | None:\n")
        new_lines.append("    return None\n")
        skip = True
        continue
    if skip:
        if line and not line[0].isspace() and line.strip():
            skip = False
            new_lines.append(line)
    else:
        new_lines.append(line)

with open(path, "w") as f:
    f.writelines(new_lines)
print("Patched")
EOF

python3 /tmp/fix_pip_truststore.py
```

**步骤 2**：补全 certifi CA 证书

```bash
cp $(python3 -c "import certifi; print(certifi.where())") \
   $(python3 -c "import pip._vendor.certifi; print(pip._vendor.certifi.__file__)")./../cacert.pem
```

### 4.4 安装 Cython 3.2.4

```bash
pip install "Cython>=3.2" setuptools==78.1.0
```

**必须使用 Cython 3.2+**。旧版 Cython（0.29.x）生成的 C 代码不兼容 Python 3.14 的 C API 变更。

### 4.5 安装 apache-beam 2.61.0

beam 包含 C 扩展（Cython），需要从源码编译：

```bash
pip install apache-beam==2.61.0 --no-build-isolation
```

如果编译 C 扩展失败，可以尝试：

```bash
pip install apache-beam==2.61.0 --no-build-isolation --no-deps
```

然后手动安装 beam 的纯 Python 依赖：

```bash
pip install dill sortedcontainers zstandard crcmod PyYAML regex \
  proto-plus objsize jsonpickle packaging
```

### 4.6 安装 apache-flink 2.2.0

**x86_64**：有预编译 wheel（仅支持 cp39-cp312），Python 3.14 需从源码编译。

**aarch64**：无预编译 wheel，必须从源码编译。

```bash
pip install py4j==0.10.9.7
pip install apache-flink==2.2.0 --no-build-isolation --no-deps
```

### 4.7 验证

```bash
python3 -c '
from pyflink.table import TableEnvironment, EnvironmentSettings
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)
result = t_env.sql_query("SELECT 1 + 1")
print(result.execute().collect().next())
'
# 预期输出: <Row(2)>
```

### 4.8 保存镜像并启动 TaskManager

```bash
# 清理容器内临时文件，避免 /tmp 占满
rm -f /tmp/*.whl /tmp/*.tar.gz
rm -rf /tmp/pip-* /tmp/python-build.*

# 在宿主机上提交镜像
docker commit flink-jm flink-pyflink:2.2.0-py314-<arch>

# 启动 TaskManager（必须使用 --tmpfs，见 FAQ）
docker run -d --name flink-tm1 --network flink-network \
  -e FLINK_PROPERTIES='jobmanager.rpc.address: flink-jm' \
  --tmpfs /tmp:rw,exec \
  flink-pyflink:2.2.0-py314-<arch> taskmanager

docker run -d --name flink-tm2 --network flink-network \
  -e FLINK_PROPERTIES='jobmanager.rpc.address: flink-jm' \
  --tmpfs /tmp:rw,exec \
  flink-pyflink:2.2.0-py314-<arch> taskmanager
```

### 4.9 验证集群健康

```bash
# 检查 TaskManager 注册数量
docker exec flink-jm curl -sf http://localhost:8081/taskmanagers | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('taskmanagers',[])))"
# 预期输出: 2

# 检查集群概览
docker exec flink-jm curl -sf http://localhost:8081/overview
```

## 5. 完整依赖清单

以下为 Python 3.14.3 环境中验证通过的依赖版本：

| 包 | 版本 | 说明 |
|----|------|------|
| Python | 3.14.3 | LTO+PGO 编译 |
| Cython | 3.2.4 | 必须 >= 3.2 |
| setuptools | 78.1.0 | 不能 >= 82（pkg_resources 被移除） |
| pip | 25.3 | 需修复 truststore |
| apache-beam | 2.61.0 | 从源码编译 |
| apache-flink | 2.2.0 | 从源码编译 |
| py4j | 0.10.9.7 | flink 指定版本 |
| numpy | 2.4.4 | |
| pyarrow | 23.0.1 | 需魔改源码适配 3.14 C API |
| protobuf | 6.33.6 | |
| dill | 0.4.1 | |
| sortedcontainers | 2.4.0 | |
| zstandard | 0.25.0 | |
| crcmod | 1.7 | |
| PyYAML | 6.0.3 | |
| regex | 2026.4.4 | |
| proto-plus | 1.27.2 | |
| objsize | 0.8.0 | |
| jsonpickle | 4.1.1 | |
| packaging | 26.0 | beam 运行时依赖 |

### 5.1 依赖版本偏离清单

beam 2.61.0 和 flink 2.2.0 发布时 Python 3.14 尚不存在，它们的版本上限锁死在旧版本。Python 3.14 要求这些包必须升到新版才能工作，因此以下依赖的实际安装版本**超出了上游声明的版本范围**：

| 包 | 上游声明 | 实际安装 | 偏离原因 |
|----|----------|----------|----------|
| **numpy** | beam: `>=1.14.3,<2.2.0` / flink: `>=1.22.4` | **2.4.4** | Python 3.14 要求 numpy >= 2.4 |
| **pyarrow** | beam: `>=3.0.0` / flink: `>=5.0.0,<21.0.0` | **23.0.1** | Python 3.14 要求 pyarrow >= 23 |
| **protobuf** | beam: `>=3.20.3,<6.0.0` / flink: `>=3.19.0` | **6.33.6** | Python 3.14 需要 protobuf 6.x |
| **dill** | beam: `>=0.3.1.1,<0.3.2` | **0.4.1** | 旧版 dill 不兼容 Python 3.14 |
| **httplib2** | beam: `>=0.8,<0.23.0` | **0.31.2** | 新版兼容，旧版在 3.14 有问题 |
| **objsize** | beam: `>=0.6.1,<0.8.0` | **0.8.0** | 新版兼容 |
| **jsonpickle** | beam: `>=3.0.0,<4.0.0` | **4.1.1** | 新版兼容 |

以下依赖**降级**安装：

| 包 | 上游声明 | 实际安装 | 偏离原因 |
|----|----------|----------|----------|
| **setuptools** | （非直接依赖） | **78.1.0** | 降级：setuptools 82+ 移除了 `pkg_resources`，beam 构建需要 |

pip 安装时会打印大量版本冲突警告，这些都是预期内的，不影响实际运行。PyFlink 场景下 beam 只使用部分功能，不依赖这些包的版本上限。

### 5.2 源码修改清单

只有 **pyarrow** 进行了源码级修改：

| 包 | 版本 | 修改内容 | 原因 |
|----|------|----------|------|
| **pyarrow** | 23.0.1 | 魔改 Cython 生成的 C 源码，适配 Python 3.14 C API | pyarrow 23 的 C 扩展使用了旧版 API（`_PyLong_AsByteArray` 缺第 5 参数等），无法在 3.14 上编译通过 |

**其他包没有修改源码**。apache-beam 和 apache-flink 的 C 扩展兼容性问题通过升级 Cython 到 3.2.4 后从源码重新编译解决——Cython 3.2.4 自己知道 Python 3.14 的新 C API，重新生成的 `.c` 文件天然兼容。

另外 **pip** 也做了运行时 patch（禁用 truststore + 补 certifi cacert.pem），但这属于安装工具环境修复，不属于依赖包的源码修改。

## 6. 磁盘空间要求

| 项目 | 大小 |
|------|------|
| `flink:2.2.0-java17` 基础镜像 | ~900MB |
| pyenv + Python 3.14.3 编译 | ~1.5GB |
| pip 包（beam + flink + 依赖） | ~1.5GB |
| **docker commit 后镜像** | **~3GB** |
| **总计最少需要** | **~8GB 可用空间** |

如果磁盘紧张，可清理：
- 旧 Docker 镜像：`docker rmi <old-image>`
- pip 缓存：`pip cache purge`
- pyenv 编译缓存：`rm -rf /tmp/python-build.*`
- 容器内临时文件：`rm -f /tmp/*.whl /tmp/*.tar.gz`
