# PyFlink TPC-H E2E Pipeline Lessons Learned

本文档汇总 PyFlink TPC-H 端到端性能分析流水线开发过程中遇到的所有技术问题、根因分析和解决方案。

---

## 目录

- [一、perf 数据采集](#一perf-数据采集)
- [二、perf 数据处理与回填](#二perf-数据处理与回填)
- [三、容器与文件传输](#三容器与文件传输)
- [四、SSH 与远程执行](#四ssh-与远程执行)
- [五、Flink 运行时](#五flink-运行时)
- [六、流水线编排](#六流水线编排)
- [七、operator/framework 计时](#七operatorframework-计时)

---

## 一、perf 数据采集

### 1.1 宿主机 perf 无法解析容器内符号

**现象**: `perf_records.csv` 中 94% 的记录 `shared_object=[unknown]`, `symbol=[unknown]`，所有 category 为 `Library`，component 全为 `unknown`。

**根因**: `perf record -a` 在容器内采集 perf.data，但 `perf report`（由 python-performance-kits 调用）在**宿主机**上执行。宿主机的 perf 二进制找不到容器内的 `libpython3.14.so`、Python `.so` 模块等文件，无法解析符号。

**修复**: 将 perf-kits 管线改为在 TM 容器内部执行。容器内有完整的 Python 运行时和所有依赖库，perf report 可以正确解析符号。

关键改动 (`orchestrator.py`):
- `_run_perf_kits_on_remote` 改为通过 `docker cp` 部署脚本到容器，用容器内的 Python + perf 运行
- 注意 `docker cp host_dir/ container:/path` 会创建 `/path/host_dir/`，必须用 `host_dir/.` 才能复制内容
- perf-kits 的 `render_platform_report.py` 等脚本也需要复制，否则管线失败（即使有 `--skip-annotate`）

**验证**: 修复后 `libpython3.14.so` 的符号正确解析为 `_PyEval_EvalFrameDefault`, `Py_BytesMain` 等，category 正确分类为 `CPython.Interpreter`, `CPython.Runtime` 等。

### 1.2 perf -a 系统级采集导致非 Python 样本占 99.5%

**现象**: 即使符号解析正确，744K 行中只有 2144 行（0.3%）来自 Python worker 进程。

**根因**: `perf record -a` 是系统级采集（`-a` = all CPUs），捕获了 JVM 线程（`C2 CompilerThread`, `GC Thread`, `pool-1-thread` 等）、bash、perf 自身等。Java Flink 的大部分 CPU 时间在 JVM 线程中，Python worker 只占很少。

按进程分布（ARM perf_records.csv，744K 行）:
| 进程 | 行数 |
|------|------|
| Thread-4 (_serv) | 35,196 |
| C2 CompilerThread | 32,185 |
| G1 Refine#1 | 24,526 |
| bash | 17,384 |
| ...（数十种 JVM 线程） | ... |
| **python3** | **1,991** |
| **pyflink-udf-run** | **153** |

**修复**: 在 `_filter_python_rows` 中按 `pid_command` 过滤，只保留 `python3` 和 `pyflink-udf-run` 进程的采样。当 CSV 无 `pid_command` 列时（如测试数据），回退到 category/SO 过滤。

```python
# 按 pid_command 过滤（生产环境）
cmd = row.get("pid_command", "").lower()
if "python" in cmd or "pyflink" in cmd:
    keep(row)
```

**替代方案（未实现）**: 使用 `perf record -p <python_pid>` 精确采集 Python worker 进程。需要先发现 worker PID，增加了编排复杂度。

### 1.3 Kernel 占比 99.8%（按 category 过滤的缺陷）

**现象**: 按(category_top == "Kernel" or "glibc")过滤后，kernel 组件占 99.8%（35,197 行），CPython 只占 0.1%。

**根因**: 按 category 保留 Kernel 行时，JVM 线程、bash 等所有进程的内核调用都被保留，而 Python worker 的 kernel 调用只有 333 行。

对比：
| 过滤方式 | Kernel 行数 | CPython 行数 |
|----------|------------|-------------|
| 按 category | 35,197 | ~1,100 |
| 按 pid_command | 333 | ~1,100 |

**修复**: 改用 `pid_command` 过滤（见 1.2）。过滤后 Python worker 的 2144 行中：
- CPython: ~1,100 行（10.9% self share）
- Kernel: 333 行（0.49% self share，主要是 `default_idle_call`）
- glibc: 113 行

### 1.4 Kernel idle 符号误归属（perf -a PID namespace 混淆）

**现象**: 即使按 `pid_command=python3` 过滤后，kernel 组件仍占 89.1%（`default_idle_call` 占全部 self%）。

**根因**: `perf record -a` 在容器内以系统级采集。当 CPU 空闲时，perf 采样到 `default_idle_call`（CPU idle loop），但由于 PID namespace 隔离，perf 将 idle 采样错误地归属给容器内的 `python3` 进程。一条 `default_idle_call` 行就占了 0.49 self%，等于 Python worker 全部 CPU 时间的 89%。

**修复**: 在 `_filter_python_rows` 中排除已知 idle 相关 kernel 符号：

```python
_KERNEL_IDLE_SYMBOLS = frozenset({
    "default_idle_call", "cpu_startup_entry", "do_idle",
    "cpuidle_idle_call", "schedule_idle", ...
})
if sym in _KERNEL_IDLE_SYMBOLS:
    continue
```

过滤后 kernel 从 89.1% 降到 0%（kernel 的非 idle 符号 self% 本身就是 0）。CPython 占 100%。

**长期方案**: 将 `perf record -a` 改为 `perf record -p <python_worker_pid>`，从采集端就避免问题。

### 1.4 未解析符号（0x 地址）污染函数列表

**现象**: 函数列表中出现 `0x0000ffffb015595c` 等十六进制地址，component 为 unknown，self time 为 0。

**根因**: perf 采样中部分符号无法解析（缺少 debug symbols），以原始地址形式出现在 CSV 中。928 个 Python worker 符号中有 ~1,000 个是未解析的十六进制地址。

**修复**: `_filter_python_rows` 中跳过 `0x` 开头和 `[unknown]` 符号。过滤后从 2144 行降到 1051 行（全是已解析符号）。

```python
sym = row.get("symbol", "").strip()
if sym.startswith("0x") or sym == "[unknown]":
    continue
```

---

## 二、perf 数据处理与回填

### 2.1 CSV 中 None 值导致 AttributeError

**现象**: `AttributeError: 'NoneType' object has no attribute 'strip'` 在 `_aggregate_symbols` 中。

**根因**: `perf_records.csv` 的 `symbol` 列包含 Python `None` 字面量（CSV 中写作 `None`），不是空字符串或缺失值。`row.get("symbol", "").strip()` 中 `dict.get` 返回了字符串 `"None"` 而非缺失 key，后续 `.strip()` 虽然不会报错，但更危险的是某些行实际返回了 Python None 对象（CSV reader 的行为差异）。

**修复**: 所有 `.strip()` 调用改为 `(row.get("field") or "").strip()`，同时处理 `None` 和空字符串：

```python
# Before (crash on None)
symbol = row.get("symbol", "").strip()

# After (safe)
symbol = (row.get("symbol") or "").strip()
```

### 2.2 旧运行脏函数残留

**现象**: 重新运行 perf backfill 后，函数列表中仍包含上一次运行产生的 `0x...` 地址函数和 `termios_exec` 等无关条目。

**根因**: `_build_functions` 的合并逻辑是"按 ID 合并、追加新的"——旧运行的函数 ID 不在新结果中，但仍被保留。

```python
# Before (merge, preserves stale entries)
existing_funcs = {f["id"]: f for f in dataset.get("functions", [])}
for func in functions:
    if func["id"] in existing_funcs:
        existing_funcs[func["id"]].update(...)
    else:
        existing_funcs[func["id"]] = func

# After (replace entirely)
dataset["functions"] = functions
```

**修复**: 将合并逻辑改为直接替换：`dataset["functions"] = functions`。每次 backfill 从 perf 数据完整重建函数列表。

### 2.3 topFunctionId 悬空引用

**现象**: 数据集中分类的 `topFunctionId` 指向不存在的函数 ID（如 `func_9702dd64`）。

**根因**: `top_n=20` 截断函数列表后，分类的 `topFunctionId` 仍引用被截断的函数。

**修复**: 截断后遍历所有分类，将指向不存在函数 ID 的 `topFunctionId` 置为 `None`：

```python
func_ids = {f["id"] for f in functions}
for cat in categories:
    tfid = cat.get("topFunctionId")
    if tfid and tfid not in func_ids:
        cat["topFunctionId"] = None
```

### 2.4 四层数据集验证失败导致 CLI 静默退出

**现象**: `pyframework_pipeline.cli run` 命令执行后无输出、无报错、不修改状态文件。

**根因**: `validate_pipeline_config` 检测到数据集为空模板（0 cases, 0 functions）返回 `status=error`，CLI 打印到 stderr 后返回 1。但 stderr 被 `2>&1` 重定向后未显示。

**修复**: 直接调用 `orchestrator.run_pipeline` 绕过 CLI 验证。后续应改进 CLI 的错误输出。

---

## 三、容器与文件传输

### 3.1 容器 /tmp 是 tmpfs，docker cp 行为异常

**现象**: `docker cp /tmp/file flink-tm1:/tmp/` 在某些情况下失败，或者 `docker cp host_dir/ container:/path` 创建嵌套目录。

**根因**: Flink TM 容器配置了 `--tmpfs /tmp:rw,exec`，文件系统行为与常规 overlay 不同。大文件写入 /tmp 可能有大小限制。

**修复**:
- 使用 `/opt/flink/` 作为中转路径（非 tmpfs）
- `docker cp src/. container:/dest/` 用 `/.` 复制目录内容而非目录本身
- 大文件（160MB perf.data）的 `scp` 传输需注意超时设置

### 3.2 docker cp 目录嵌套问题

**现象**: `docker cp host_dir/ container:/path` 创建 `/path/host_dir/` 而非 `/path/contents`。

**根因**: docker cp 的行为是复制目录本身到目标路径。要复制目录内容，必须用 `host_dir/.` 语法。

```bash
# Wrong: creates /path/host_dir/...
docker cp host_dir/ container:/path/

# Correct: copies contents to /path/
docker cp host_dir/. container:/path/
```

### 3.3 perf.data 大文件传输耗时极长

**现象**: x86 perf.data 160MB，通过 `docker cp → scp` 传输到本地耗时约 1 小时。ARM 137MB 类似。

**根因**: 两层传输——先 `docker cp` 到宿主机，再 `scp` 到本地。网络带宽有限，加上 SSH 开销。

**优化**:
- 添加 `perf.data` 存在性检查，跳过重复下载
- `scp` 没有断点续传能力，必须一次完成
- 容器内先 copy 到 `/opt/flink/`（非 tmpfs），再 docker cp 出来

---

## 四、SSH 与远程执行

### 4.1 SSH 连接延迟导致超时

**现象**: zen5（x86）主机的 SSH 连接需要 ~5 秒建立，导致 `timeout=5` 的命令全部超时失败。

**根因**: SSH 握手 + 认证 + MOTD（"Authorized users only..."）耗时较长。

**修复**: 所有 SSH 命令的 timeout 从 5s 提高到 15s。涉及：
- `_collect_binary_from_container` 中的清理 `rm -f` 命令
- `_run_perf_kits_on_remote` 中的 `mkdir`、清理命令
- `_find_task_tm` 已有 10s 超时

**经验值**: 所有 SSH 远程命令 timeout 最小设为 15s。

---

## 五、Flink 运行时

### 5.1 Flink REST API /jobs 返回格式不一致

**现象**: `_find_task_tm` 中解析 `/jobs` API 报错。

**根因**: Flink REST API `/jobs` 在无任务时返回 `{"jobs": []}`（列表），有任务时返回 `{"jobs": {"running": [...], "finished": [...]}}`（字典）。代码只处理了字典格式。

**修复**: 添加格式判断 `isinstance(jobs_data, list)` 分别处理。

### 5.2 TM /tmp 权限问题

**现象**: TMs started from committed JM image fail with "Could not create working directory" or JAAS config errors.

**根因**: TM 容器的 /tmp 需要 `rw,exec` 权限。

**修复**: 启动 TM 时加 `--tmpfs /tmp:rw,exec` 标志。已更新在 environment adapter 和 environment.yaml 中。

---

## 六、流水线编排

### 6.1 perf 容器内执行的完整流程

在 TM 容器内执行 perf-kits 的步骤：

1. **安装 linux-tools**: 容器内默认无 perf，需 `apt-get install linux-tools-common linux-tools-generic`
2. **部署 perf-kits 脚本**: 通过 host staging → `docker cp` 到容器的 `/opt/flink/perf-kits-scripts/`
3. **执行 perf report**: 用容器内的 Python + perf 解析 perf.data
4. **收集输出**: 从容器内 `docker cp` 输出到宿主机，再 `scp` 到本地

关键路径:
```
host staging dir → docker cp → container /opt/flink/perf-kits-scripts/
                                   ↓
                        container: python3 render_platform_report.py
                                   ↓
                        container: /opt/flink/perf-kits-output/
                                   ↓
                        docker cp → host → scp → local
```

### 6.2 perf.data 文件传输优化

- 容器内 copy 到 `/opt/flink/`（非 tmpfs）→ `docker cp` 到宿主机 → `scp` 到本地
- 添加存在性检查避免重复下载：`if perf_data_local.exists() and perf_data_local.stat().st_size > 0: skip`

---

## 七、operator/framework 计时

### 7.1 Python worker 无法写 stats 文件

**现象**: `_timed_eval` 中写入 `/tmp/_pyflink_timing_stats.json` 的代码在 Python worker 进程中不生效，文件从未创建。

**根因**: PyFlink `process` 模式下，UDTF 函数在独立的 worker 进程中运行。函数通过 cloudpickle 序列化后发送给 worker。闭包中引用的模块级变量（`json`, `os`, `tempfile`）在 worker 进程中可能无法正确解析（benchmark_runner 模块不在 worker 的 Python 路径中）。

**替代方案（均未验证）**:
- 使用 Flink 的 Accumulator 机制传递统计信息
- 将统计信息通过 SQL 管道写回到 sink（修改 SQL schema）
- 使用共享内存或 TCP socket 传递

### 7.2 从 perf 数据估算 operator/framework 时间

**现状**: 由于 7.1 的限制，per-invocation 计时不可用。采用 perf 采样数估算：

```
Python worker CPU time = total_samples / sample_rate (999 Hz)
CPU utilization ratio = CPU time / sum(all case wall-clock times)
Per case: operator = case_wallclock × ratio
          framework = case_wallclock - operator
```

ARM 实测数据（CPU time = 1.04s, wall-clock total = 126.6s, ratio = 0.8%）:

| Case | 总耗时 | Operator (Python执行) | Framework (框架开销) |
|------|--------|----------------------|---------------------|
| q01 | 20.68 s | 2256.0 ms (10.9%) | 18424.0 ms (89.1%) |
| q06 | 14.28 s | 1557.8 ms (10.9%) | 12722.2 ms (89.1%) |
| q19 | 17.85 s | 1947.3 ms (10.9%) | 15902.7 ms (89.1%) |

**注意**: 这里的比例是 CPU time / wall-clock 比例按各 case wall-clock 等比分配，所有 case 的 operator/framework 百分比相同。当有真实的 per-invocation 数据时应替换此估算。

**演进历史**:
1. 最初用 CPython share（CPython self% / 总 self%）估算，但过滤 idle 后 CPython = 100%
2. 改用 CPU utilization（采样数 / 采样率 / 总 wall-clock），更准确反映实际 CPU 占比

### 7.3 frameworkCallTime 从未生成

**现象**: timing-normalized.json 中没有 `frameworkCallTime` 指标。

**根因**: orchestrator 的 `_merge_wall_clock_times` 只写入了 `wallClockTime` 和 `tmE2eTime`，从未计算或写入 `frameworkCallTime`。

**修复**: 在 `perf_backfill` 中通过估算补充（见 7.2），而非修改 orchestrator。

---

## 附录 A：关键数据流

```
perf record -a (容器内)
    ↓ perf.data
perf report (容器内) → perf_records.csv (744K 行)
    ↓ _filter_python_rows (按 pid_command)
    ↓ 1051 行 (已解析 Python worker 符号)
    ↓ _aggregate_symbols
    ↓ {symbol: {self_share, component, category...}}
    ↓ _build_components / _build_categories / _build_functions
    ↓ stackOverview + functions + componentDetails
    ↓ _estimate_case_operator_framework
    ↓ cases[].metrics.operator / framework
    ↓
dataset.json → web/public/
```

## 附录 B：perf CSV 关键字段

| 字段 | 说明 | 示例 |
|------|------|------|
| `symbol` | 函数符号名 | `_PyEval_EvalFrameDefault` |
| `shared_object` | 所属 .so / 二进制 | `libpython3.14.so.1.0` |
| `category_top` | python-performance-kits L1 分类 | `CPython.Interpreter` |
| `category_sub` | L2 子分类 | `CPython.Objects.Dict` |
| `self` | 自身 CPU 占比 % | `0.02` |
| `children` | 含子调用 CPU 占比 % | `0.05` |
| `period` | 采样周期数 | `1000` |
| `pid_command` | 进程命令 | `python3`, `C2 CompilerThre` |

## 附录 C：组件映射

| category_top | component | categoryL1 |
|-------------|-----------|------------|
| CPython.Interpreter | cpython | interpreter |
| CPython.Runtime | cpython | runtime |
| CPython.Memory | cpython | memory |
| CPython.Objects | cpython | object_model |
| CPython.Calls | cpython | calls_dispatch |
| CPython.Lookup | cpython | lookup |
| CPython.Import | cpython | import_loading |
| CPython.Compiler | cpython | compiler |
| CPython.Concurrency | cpython | concurrency |
| CPython.Exceptions | cpython | exceptions |
| CPython.GC | cpython | gc |
| Kernel | kernel | kernel |
| glibc | glibc | glibc |
| Library | unknown | library |
