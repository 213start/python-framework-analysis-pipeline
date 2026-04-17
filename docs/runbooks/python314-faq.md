# Python 3.14 兼容性 FAQ 与故障排除

本文汇总在 Flink/Beam/PyArrow 等 Python 框架中适配 Python 3.14 时遇到的常见问题及解决方案。

---

## 目录

1. [编译相关](#编译相关)
2. [pip 安装相关](#pip-安装相关)
3. [C 扩展兼容性](#c-扩展兼容性)
4. [Docker 容器相关](#docker-容器相关)
5. [依赖版本冲突](#依赖版本冲突)
6. [磁盘空间](#磁盘空间)
7. [远程集群提交](#远程集群提交)
8. [Benchmark Runner](#benchmark-runner)

---

## 编译相关

### Q: Python 3.14 LTO+PGO 编译 OOM（Exit 137）怎么办？

**现象**：`pyenv install 3.14.3` 在 `profile-run-stamp` 或链接阶段被 kill，退出码 137。

**原因**：LTO 链接阶段内存消耗极大，每个线程需要 3-4GB。`-j4` 在 16GB 机器上可能 OOM。

**解决**：使用物理核数的一半作为并行度。

```bash
MAKEOPTS="-j2" pyenv install 3.14.3
```

经验值：
- 4C/7.2G → `-j2`（编译约 60 分钟）
- 4C/15G → `-j2`（编译约 40 分钟）
- 8C/32G → 可以尝试 `-j4`

### Q: pyenv 下载 Python 源码太慢怎么办？

**现象**：`pyenv install` 下载 Python-X.X.X.tar.xz 速度只有几十 KB/s。

**解决**：在本地下载后 scp 到远程机器，放入 pyenv 缓存目录：

```bash
# 本地
scp Python-3.14.3.tar.xz remote:/tmp/

# 远程
mkdir -p $PYENV_ROOT/cache
cp /tmp/Python-3.14.3.tar.xz $PYENV_ROOT/cache/
pyenv install 3.14.3  # 会优先使用缓存
```

### Q: 编译时报 `undefined reference to '_PyLong_AsByteArray'`？

**现象**：链接阶段报 `_PyLong_AsByteArray` 参数数量不匹配。

**原因**：Python 3.14 给 `_PyLong_AsByteArray` 增加了 `with_exceptions` 参数（第 5 个参数）。

**解决**：使用 Cython 3.2+ 重新生成 C 代码。旧版 Cython（0.29.x）不知道这个新参数。如果无法升级 Cython，需要手动 patch 生成的 `.c` 文件：

```python
content = content.replace(
    "is_little, !is_unsigned)",
    "is_little, !is_unsigned, 1)"
)
```

---

## pip 安装相关

### Q: pip 报 `FileNotFoundError` in `truststore._api.py`？

**现象**：
```
FileNotFoundError: [Errno 2] No such file or directory
  pip/_vendor/truststore/_api.py:160 in load_verify_locations
```

**原因**：Python 3.14 的 pip 使用 truststore 模块加载系统 CA 证书，但该模块在某些环境下会找不到证书文件。`--trusted-host` 参数无法绕过此问题。

**解决**：两步修复。

步骤 1 — 禁用 truststore，让 pip 回退到标准 SSL：

```python
# 找到并编辑 pip/_internal/cli/index_command.py
# 将 _create_truststore_ssl_context 函数体替换为 return None
```

可以用脚本自动化：

```python
path = "path/to/pip/_internal/cli/index_command.py"
with open(path) as f:
    lines = f.readlines()
new_lines, skip = [], False
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
```

步骤 2 — 补全 pip vendor certifi 的 CA 证书（如果仍然报 `invalid path`）：

```bash
cp $(python3 -c "import certifi; print(certifi.where())") \
   $(python3 -c "import pip._vendor.certifi; import os; print(os.path.dirname(pip._vendor.certifi.__file__))")/cacert.pem
```

### Q: pip 报 `Could not find a suitable TLS CA certificate bundle`？

**现象**：
```
ERROR: Could not install packages due to an OSError:
Could not find a suitable TLS CA certificate bundle, invalid path:
  .../pip/_vendor/certifi/cacert.pem
```

**原因**：pip 内置的 certifi 包没有 `cacert.pem` 文件（可能安装时损坏或磁盘满导致写入失败）。

**解决**：从独立 certifi 包复制 CA 证书到 pip vendor 目录：

```bash
SITE=$(python3 -c "import site; print(site.getsitepackages()[0])")
cp $SITE/certifi/cacert.pem $SITE/pip/_vendor/certifi/cacert.pem
```

---

## C 扩展兼容性

### Q: import 时报 `undefined symbol: _PyUnicode_FastCopyCharacters`？

**现象**：`import apache_beam` 或 `import pyflink` 时动态链接失败。

**原因**：该符号在 Python 3.14 中已被移除。说明 `.so` 文件是用旧版 Cython（0.29.x）编译的。

**解决**：
1. 升级 Cython 到 3.2+：`pip install "Cython>=3.2"`
2. 卸载有问题的包：`pip uninstall apache-beam -y`
3. 重新从源码编译安装：`pip install apache-beam --no-build-isolation`

### Q: 报 `cframe->use_tracing` 扠除了或 `PyThreadState` 没有 `cframe` 成员？

**现象**：编译 Cython 生成的 C 代码时，访问 `tstate->cframe->use_tracing` 失败。

**原因**：Python 3.14 移除了 `PyThreadState.cframe` 字段，改用 `tstate->c_profilefunc` / `tstate->c_tracefunc` 判断是否启用了 tracing/profiling。

**解决**：使用 Cython 3.2+ 会自动处理。手动 patch 方式：

```python
content = content.replace(
    "(unlikely((tstate)->cframe->use_tracing) &&",
    "(unlikely(((tstate)->c_profilefunc || (tstate)->c_tracefunc)) &&"
)
```

### Q: 报 `_PyObject_NextNotImplemented` 找不到？

**现象**：编译或链接时找不到 `_PyObject_NextNotImplemented` 符号。

**原因**：Python 3.14 将该符号从公共头文件移到了内部头文件。

**解决**：使用 Cython 3.2+ 会自动添加版本守卫。手动 patch：

```python
content = content.replace(
    "#if PY_VERSION_HEX >= 0x02070000 && CYTHON_COMPILING_IN_CPYTHON",
    "#if PY_VERSION_HEX >= 0x02070000 && PY_VERSION_HEX < 0x030e0000 && CYTHON_COMPILING_IN_CPYTHON"
)
```

### Q: apache-flink 没有 ARM (aarch64) 的预编译 wheel？

**现象**：`pip install apache-flink` 在 ARM 机器上报 `No matching distribution found`。

**原因**：apache-flink 官方只发布 x86_64 的 wheel，且仅支持 cp39-cp312。

**解决**：从源码编译。需要先安装 Cython 3.2+ 和所有依赖：

```bash
pip install "Cython>=3.2" setuptools==78.1.0
pip install py4j==0.10.9.7
pip install apache-flink==2.2.0 --no-build-isolation --no-deps
```

### Q: Cython 编译的 C 文件有哪些 Python 3.14 不兼容？

Python 3.14 C API 变更导致 4 类不兼容：

| # | 变更 | 影响 | 自动修复 |
|---|------|------|---------|
| 1 | `_PyLong_AsByteArray` 增加第 5 参数 `with_exceptions` | 链接失败 | Cython 3.2+ |
| 2 | `_PyObject_NextNotImplemented` 移至内部头文件 | 编译失败 | Cython 3.2+ |
| 3 | `PyThreadState.cframe` 字段删除 | 编译失败 | Cython 3.2+ |
| 4 | `_PyInterpreterState_GetConfig` 断言变更 | 运行时崩溃 | Cython 3.2+ |

**结论**：统一使用 Cython >= 3.2 即可全部解决，无需手动 patch。

---

## Docker 容器相关

### Q: TaskManager 启动后立即退出，报 "Could not create working directory"？

**现象**：TM 日志显示：
```
java.io.IOException: Could not create the working directory /tmp/tm_xxx
```

**原因**：从 `docker commit` 的镜像启动 TM 时，容器内 `/tmp` 的权限或内容可能与 Flink 预期不符（JM 的临时文件、权限变化等被一并提交）。

**解决**：启动 TM 时使用 `--tmpfs` 挂载干净的 tmpfs：

```bash
docker run -d --name flink-tm1 --network flink-network \
  -e FLINK_PROPERTIES='jobmanager.rpc.address: flink-jm' \
  --tmpfs /tmp:rw,exec \
  flink-pyflink:2.2.0-py314-<arch> taskmanager
```

**注意**：JobManager 不能使用 `--tmpfs`，否则重启后状态丢失。TM 是无状态的，可以安全使用。

### Q: TaskManager 报 JAAS 配置文件创建失败？

**现象**：
```
java.nio.file.AccessDeniedException: /tmp/jaas-xxx.conf
RuntimeError: unable to generate a JAAS configuration file
```

**原因**：同上，`/tmp` 权限问题。

**解决**：同上，使用 `--tmpfs /tmp:rw,exec`。

### Q: docker commit 后镜像太大？

**现象**：commit 后镜像超过 3GB。

**原因**：commit 会保存容器文件系统的所有变更，包括：
- pyenv 编译中间文件（~1GB）
- pip 缓存（~500MB）
- /tmp 中的下载文件

**解决**：commit 前在容器内清理：

```bash
docker exec -u root flink-jm bash -c "
  rm -rf /tmp/pip-* /tmp/python-build.* /tmp/*.whl /tmp/*.tar.gz
  pip cache purge
  find /opt/flink/.pyenv/versions/3.14.3 -name '__pycache__' -exec rm -rf {} + 2>/dev/null
"
docker commit flink-jm flink-pyflink:2.2.0-py314-<arch>
```

---

## 依赖版本冲突

### Q: beam/flink 声明的依赖版本在 Python 3.14 上无法满足，怎么办？

**背景**：beam 2.61.0 和 flink 2.2.0 发布时 Python 3.14 尚不存在，它们的 `install_requires` 锁定了旧版本上限。Python 3.14 要求这些包必须升到新版。

**实际偏离清单**（超出上游声明范围）：

| 包 | 上游声明 | 实际安装 | 偏离原因 |
|----|----------|----------|----------|
| **numpy** | beam: `>=1.14.3,<2.2.0` / flink: `>=1.22.4` | **2.4.4** | Python 3.14 要求 numpy >= 2.4 |
| **pyarrow** | beam: `>=3.0.0` / flink: `>=5.0.0,<21.0.0` | **23.0.1** | Python 3.14 要求 pyarrow >= 23 |
| **protobuf** | beam: `>=3.20.3,<6.0.0` / flink: `>=3.19.0` | **6.33.6** | Python 3.14 需要 protobuf 6.x |
| **dill** | beam: `>=0.3.1.1,<0.3.2` | **0.4.1** | 旧版 dill 不兼容 Python 3.14 |
| **httplib2** | beam: `>=0.8,<0.23.0` | **0.31.2** | 新版兼容，旧版在 3.14 有问题 |
| **objsize** | beam: `>=0.6.1,<0.8.0` | **0.8.0** | 新版兼容 |
| **jsonpickle** | beam: `>=3.0.0,<4.0.0` | **4.1.1** | 新版兼容 |

降级安装：

| 包 | 上游声明 | 实际安装 | 偏离原因 |
|----|----------|----------|----------|
| **setuptools** | （非直接依赖） | **78.1.0** | setuptools 82+ 移除了 `pkg_resources`，beam 构建需要 |

**结论**：pip 安装时会打印大量版本冲突警告，这都是预期内的。PyFlink 场景下 beam 只使用部分功能，不依赖这些包的版本上限。安装方式为 `--no-deps` 加手动安装依赖，避免 pip 因版本约束拒绝安装。

### Q: 哪些依赖包修改了源码？

**只有 pyarrow 修改了源码**：

| 包 | 修改内容 | 原因 |
|----|----------|------|
| **pyarrow 23.0.1** | 魔改 Cython 生成的 C 源码，适配 Python 3.14 C API（`_PyLong_AsByteArray` 增加第 5 参数等） | pyarrow 23 的 C 扩展使用了旧版 API，无法在 3.14 上编译 |

**其他包没有修改源码**：
- **apache-beam** 和 **apache-flink** 的 C 扩展通过升级到 Cython 3.2.4 后从源码重新编译解决。Cython 3.2.4 自己知道 Python 3.14 的新 C API，重新生成的 `.c` 文件天然兼容。
- **pip** 做了运行时 patch（禁用 truststore + 补 certifi cacert.pem），但这属于安装工具环境修复，不属于依赖包源码修改。

### Q: setuptools 82+ 报 `No module named 'pkg_resources'`？

**现象**：`pip install apache-beam` 时构建依赖阶段失败：
```
ModuleNotFoundError: No module named 'pkg_resources'
```

**原因**：setuptools 82.0 移除了 `pkg_resources` 模块，而 beam 的 `setup.py` 依赖它。

**解决**：降级 setuptools：

```bash
pip install "setuptools<82"
```

验证通过的版本：`setuptools==78.1.0`。

### Q: beam 的依赖版本警告可以忽略吗？

**现象**：安装完 beam 后 pip 报大量版本冲突警告：
```
apache-beam 2.61.0 requires numpy<2.2.0,>=1.14.3, but you have numpy 2.4.4
apache-beam 2.61.0 requires pyarrow<17.0.0,>=3.0.0, but you have pyarrow 23.0.1
```

**回答**：可以忽略。具体偏离原因见上方「beam/flink 声明的依赖版本在 Python 3.14 上无法满足」条目。PyFlink 场景下 beam 只使用部分功能，实际运行不受版本上限影响。

### Q: `import apache_beam` 报 `ModuleNotFoundError: No module named 'packaging'`？

**原因**：beam 的运行时代码依赖 `packaging` 模块，但它不在 beam 的 `install_requires` 中。

**解决**：

```bash
pip install packaging
```

### Q: `import apache_beam` 报 `Could not find a compatible bson package`？

**现象**：导入时打印警告 `Could not find a compatible bson package`。

**回答**：这是一个警告，不影响功能。bson 是 beam 的可选依赖，PyFlink 不使用它。

---

## 磁盘空间

### Q: kunpeng 磁盘满导致各种奇怪错误？

**现象**：pip 安装失败、Python import 失败、容器内文件消失。

**原因**：kunpeng 只有 38GB 磁盘，Docker 镜像和 Python 环境占用大量空间。

**排查**：

```bash
df -h /
docker system df
du -sh /var/lib/docker/overlay2/
du -sh /opt/flink/.pyenv/
```

**清理优先级**：
1. 旧 Docker 镜像：`docker images` 查看，`docker rmi <old>` 清理
2. Docker 卷：`docker volume prune -f`
3. pip 缓存：`pip cache purge`
4. pyenv 编译缓存：`rm -rf /tmp/python-build.*`
5. 容器内临时文件：`rm -f /tmp/*.whl`

**最低空间要求**：保持至少 2GB 可用空间，否则 pip 和 Flink 运行时可能失败。

---

## 远程集群提交

### Q: PyFlink 2.2.0 报 `StreamExecutionEnvironment has no attribute 'create_remote_environment'`？

**原因**：PyFlink 2.x 移除了 `create_remote_environment()` Python API。Java 端的 `RemoteStreamEnvironment` 类仍然存在，但 Python 封装层不再暴露它。

**解决**：通过 Py4J 直接调用 Java 构造函数：

```python
from pyflink.java_gateway import get_gateway
from pyflink.datastream import StreamExecutionEnvironment

gateway = get_gateway()
jvm = gateway.jvm
jars = gateway.new_array(jvm.java.lang.String, 1)
jars[0] = "/opt/flink/FlinkDemo-1.0-SNAPSHOT.jar"
j_env = jvm.org.apache.flink.streaming.api.environment\
    .RemoteStreamEnvironment(host, port, jars)
env = StreamExecutionEnvironment(j_env)
```

**注意**：
- JAR 路径用绝对路径，不要加 `file://` 前缀
- `gateway.new_array` 创建 `String[]`，不能用 Python list

### Q: 远程提交报 `ClassNotFoundException: PythonTableFunctionOperator`？

**现象**：Job 提交到远程 Flink 集群后，JM 反序列化失败。

```
java.lang.ClassNotFoundException:
  org.apache.flink.table.runtime.operators.python.table.PythonTableFunctionOperator
```

**原因**：`flink-python-2.2.0.jar` 在 Flink 基础镜像中位于 `opt/` 目录而非 `lib/`。Flink 只自动加载 `lib/` 下的 JAR。

**解决**：在所有容器（JM + TM）中复制 JAR 到 `lib/`，然后重启集群：

```bash
docker exec <container> cp /opt/flink/opt/flink-python-2.2.0.jar /opt/flink/lib/
docker restart flink-jm flink-tm1 flink-tm2
```

### Q: 远程提交报 TM 上 `undefined symbol: _PyUnicode_FastCopyCharacters`？

**现象**：TM 日志显示 Python worker 进程启动后立即退出，beam C 扩展加载失败。

```
ImportError: .../apache_beam/coders/coder_impl.cpython-314-x86_64-linux-gnu.so:
  undefined symbol: _PyUnicode_FastCopyCharacters
```

**原因**：TM 容器的 beam `.so` 文件是用旧版 Cython (0.29.x) 编译的。这说明 TM 镜像是在 Cython 升级和重编译之前从 JM commit 的。

**解决**：从当前正确的 JM 重新 commit 镜像，重新创建 TM：

```bash
# 清理 JM 临时文件
docker exec -u root flink-jm rm -rf /tmp/pip-* /tmp/python-build.* /tmp/*.whl
# commit 新镜像
docker commit flink-jm flink-pyflink:2.2.0-py314-<arch>-v2
# 重建 TM
docker stop flink-tm1 flink-tm2 && docker rm flink-tm1 flink-tm2
docker run -d --name flink-tm1 --network flink-network \
  -e FLINK_PROPERTIES='jobmanager.rpc.address: flink-jm' \
  --tmpfs /tmp:rw,exec \
  flink-pyflink:2.2.0-py314-<arch>-v2 taskmanager
```

### Q: 远程提交报 TM 上 `ModuleNotFoundError: No module named 'dill'`？

**原因**：同上，TM 镜像 commit 时缺失部分 beam 运行时依赖。

**临时修复**：在 TM 中安装缺失依赖：

```bash
docker exec -u root flink-tm1 bash -c '... pip install dill sortedcontainers ...'
```

**根本修复**：从完整的 JM 重新 commit 镜像（见上一条）。

### Q: SQL 解析报 `Encountered "result" ... Was expecting identifier`？

**现象**：

```
SQL parse failed. Encountered "result" at line 16, column 16.
```

**原因**：`result` 是 Flink SQL 保留字。在 `LATERAL TABLE(...) AS TPY(...)` 的别名列表中不能直接使用。

**解决**：用反引号引用所有别名：

```sql
AS TPY(`result`, `java_start_time`, `py_duration`)
```

`benchmark_runner.py` 已自动处理。

---

## Benchmark Runner

### Q: benchmark_runner.py 的 `--dry-run` 模式需要 PyFlink 吗？

**回答**：不需要。`--dry-run` 只生成和打印 SQL，不执行任何 Flink 操作。PyFlink 的 import 只在实际执行路径中触发。

### Q: 本地 mini-cluster 和远程集群的性能差异？

**实测数据**（zen5 x86_64, q06, 10K rows, parallelism=1）：

| 模式 | Throughput |
|------|-----------|
| 本地 mini-cluster | 404 rows/s |
| 远程集群 (1JM+2TM) | 2431 rows/s |

远程集群约 6 倍吞吐，原因是 TM 在独立容器中运行，Python UDF 进程不受 JM JVM 资源争用影响。
