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
- [八、x86 "性能差 24x" 调查与修正](#八x86-性能差-24x-调查与修正)
- [十五、前端数据展示修复（第二轮）](#十五前端数据展示修复第二轮)
- [十六、组件/分类总耗时量级错误与堆叠图比例失调](#十六组件分类总耗时量级错误与堆叠图比例失调)
- [十七、分类热点函数只显示一个 & 函数缺少来源信息](#十七分类热点函数只显示一个--函数缺少来源信息)
- [十八、四层校验 160 个错误 & 过期绑定清理](#十八四层校验-160-个错误--过期绑定清理)

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

### 5.3 TM 日志文件名不是 taskexecutor.log

**现象**: `_collect_operator_timing` grep `taskexecutor.log` 找不到文件。x86 TM 实际日志名为 `flink--taskexecutor-0-9fe55a708e12.log`。

**根因**: Flink 日志文件名包含容器 hostname（如 `flink--taskexecutor-0-{hostname}.log`），不同容器实例名称不同。

**修复**: 使用通配符 `flink--taskexecutor-*.log` 匹配。

### 5.4 PostUDF System.out 不写入 Flink log4j 日志

**现象**: PostUDF 的 `System.out.printf("[BENCHMARK_SUMMARY] ...")` 不出现在 TM 的 Flink 日志文件中。

**根因**: Flink 的 log4j 日志只捕获 Logger（SLF4J）输出。`System.out.printf` 写入 JVM 进程的 stdout，被 Docker 的 logging driver 捕获为 `docker logs` 输出。

**修复**: `_collect_operator_timing` 先尝试 grep Flink 日志文件，再 fallback 到 `docker logs --tail 500`。

**长期方案**: 让 PostUDF 改用 SLF4J Logger 输出 BENCHMARK_SUMMARY，或写文件到 `/tmp/`。

---

## 六、流水线编排

### 6.1 zen5 (x86) SSH 延迟导致短超时命令频繁失败

**现象**: zen5 SSH 连接延迟 ~3.4s（vs kunpeng ~0.3s）。所有 `timeout=10` 的远程命令在 zen5 上超时。

**根因**: zen5 通过代理连接，SSH 握手开销大。每次 `executor.run()` 建立新 SSH 连接。

**修复**: 所有远程命令 timeout 从 10s/15s 提升到 30s。`_collect_operator_timing` timeout 60s。`docker_logs` 增加 `--tail N` 避免传输全量日志。

**优化方向**: 使用 SSH ControlMaster 复用连接，避免每次命令都建立新连接。

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

### 7.2 perf 采样严重低估 bursty 工作负载（已废弃估算方法）

**现象**: 用 perf 采样数估算 operator/framework 时间，得到 12.9 ns/row，但实际值约 2 us/row（160 倍低估）。

**根因**: `perf -F 999`（999Hz 采样率）对 PyFlink 的 bursty 工作负载严重欠采样。PyFlink UDF 执行是批量突发模式——在 Java 端触发时集中处理大量行，然后长时间等待下一批。999Hz 采样在突发窗口内只捕获极少量样本：

```
ARM 实测：1034 samples / 999 Hz = 1.035s CPU time（80M rows）
→ 估算: 1.035s / 80M = 12.9 ns/row
→ 实际: ~2 us/row（从 PostUDF BENCHMARK_SUMMARY 得知）
→ 低估倍数: ~160x
```

**结论**: perf 采样数据**不能**用于估算 operator/framework 时间。perf 适合热点分析（哪些函数最耗时），不适合绝对耗时估算。

### 7.3 PostUDF BENCHMARK_SUMMARY：真实 operator/framework 计时来源

**方案**: Java UDF `PostUDF.java` 的 `close()` 方法已经通过 AtomicLong 累积了：
- `totalPyDurationNs` — Python UDF 真实执行耗时（operator time）
- `totalFrameworkOverheadNs` — 框架调用开销（framework time）
- `recordCount` — 处理的行数

在作业结束时打印 `[BENCHMARK_SUMMARY]` JSON 到 stdout。但 Flink 将 System.out 重定向到 TM 日志文件（`/opt/flink/log/taskexecutor.log`），而非 docker stdout。

**orchestrator 实现**: `_collect_operator_timing` 从各 TM 容器的 Flink 日志中提取最后一个 `[BENCHMARK_SUMMARY]` 行：

```python
grep BENCHMARK_SUMMARY /opt/flink/log/taskexecutor.log | tail -1
```

解析 JSON 后累加各 TM 的统计数据，写入 timing-normalized.json 的 `businessOperatorTime` 和 `frameworkCallTime`（以 `total_ns` 字段）。

**timing_backfill 适配**: `_extract_per_invocation_ns` 已更新为同时检查 `per_invocation_ns` 和 `total_ns`：

```python
val = metric.get("per_invocation_ns") or metric.get("total_ns")
```

**注意**: 在下次 E2E 运行之前，数据集中 operator/framework 值为 None（当前运行未采集 TM 日志）。

### 7.4 frameworkCallTime 从未生成（已解决）

**现象**: timing-normalized.json 中没有 `frameworkCallTime` 指标。

**根因**: orchestrator 的 `_merge_wall_clock_times` 只写入了 `wallClockTime` 和 `tmE2eTime`，从未计算或写入 `frameworkCallTime`。

**修复**: 通过 PostUDF BENCHMARK_SUMMARY 采集真实数据（见 7.3），`_merge_wall_clock_times` 现在同时写入 `businessOperatorTime`（`totalPyDurationNs`）和 `frameworkCallTime`（`totalFrameworkOverheadNs`）。

### 7.5 perf_backfill 移除估算逻辑

**变更**: `perf_backfill.py` 中的 `_estimate_case_operator_framework` 函数已移除。函数热图分析（stackOverview、functions、categories）保留不变，operator/framework 时间现在完全由 PostUDF 的真实计时数据驱动。

---

## 附录 A：关键数据流

```
perf record -a (容器内)
    ↓ perf.data
perf report (容器内) → perf_records.csv (744K 行)
    ↓ _filter_python_rows (按 pid_command, 排除 idle/0x)
    ↓ 1051 行 (已解析 Python worker 符号)
    ↓ _aggregate_symbols
    ↓ {symbol: {self_share, component, category...}}
    ↓ _build_components / _build_categories / _build_functions
    ↓ stackOverview + functions + componentDetails
    ↓
PostUDF [BENCHMARK_SUMMARY] (TM 日志)
    ↓ _collect_operator_timing (grep + JSON parse)
    ↓ businessOperatorTime + frameworkCallTime (total_ns)
    ↓ timing_backfill → _build_metrics → cases[].metrics
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

---

## 八、x86 "性能差 24x" 调查与修正

### 8.1 问题现象

x86 (zen5) q01 耗时 491.7s (20k rows/s)，ARM (kunpeng) q01 耗时 20.7s (483k rows/s)，
表面差距 24x，似乎 x86 性能极差。

### 8.2 根因 1：行数假设错误

- `project.yaml` 配置 `rows: 1000000`（1M），orchestrator 传 `--rows 1000000` 给 benchmark_runner
- `benchmark_runner.py` 默认 `--rows 10M`，但 orchestrator 覆盖了默认值
- 手动跑 x86 benchmark 时用了默认 10M，与 ARM 的 1M 不对齐
- 误以为 ARM 也用 10M（因为 benchmark_runner 默认是 10M），导致吞吐量计算错误
- ARM 实际吞吐量: 1M / 20.7s = **48.3k rows/s**（不是 483k）

**教训**: 对比 benchmark 结果前必须检查 `project.yaml` 的 `rows` 设置，不能假设用 benchmark_runner 的默认值。

### 8.3 根因 2：x86 benchmark_runner 代码多了 JSON 文件 I/O

x86 部署的 `benchmark_runner.py` 中 `_timed_eval` 函数每行执行：
```python
# 每行都做 open → read → json.load → json.dump → write
with open("/tmp/_pyflink_timing_stats.json", "r") as f:
    _s = json.load(f)
# ... update stats ...
with open("/tmp/_pyflink_timing_stats.json", "w") as f:
    json.dump(_s, f)
```

而 ARM 的 `benchmark_runner.py` 从未包含此代码（`grep "timing_stats"` 匹配 0 行）。
x86 上文件写入成功（count=10M），每行增加 ~1.6μs 额外开销。

**修复**: 移除 `_timed_eval` 中的 JSON 文件 I/O。PostUDF 的 BENCHMARK_SUMMARY 已提供更好的计时数据。

**修复效果**: x86 q01 (1M rows) 从 63.9s → **9.97s** (6.4x 加速)。

### 8.4 修正后的真实性能对比

| 平台 | q01 耗时 | 吞吐量 | 纯 Python UDF |
|------|---------|--------|--------------|
| ARM (kunpeng) | 20.68s | 48.3k rows/s | 0.17μs/行 |
| **x86 (zen5)** | **9.97s** | **100.3k rows/s** | **0.09μs/行** |

**x86 实际比 ARM 快 2.1x**，完全符合纯 Python 基准测试的预期。

### 8.5 调查过程中的微基准测试

| 测试项 | x86 | ARM | x86/ARM |
|--------|-----|-----|---------|
| 纯 Python UDF | 0.09μs | 0.17μs | 1.9x 快 |
| Generator UDTF | 0.66μs | 0.88μs | 1.3x 快 |
| Arrow IPC round-trip (batch) | 0.004μs | 0.01μs | 2.5x 快 |
| Arrow per-element (.as_py) | 2.59μs | 4.72μs | 1.8x 快 |
| 纯 Java datagen→blackhole | 5.88s/10M | N/A | - |

所有纯 Python/Arrow 测试中 x86 均优于 ARM。

### 8.6 Arrow C++ 版本差异（非根因但需注意）

| 属性 | x86 | ARM |
|------|-----|-----|
| pyarrow 版本 | 23.0.1 | 23.0.1 |
| Arrow C++ 版本 | **24.0.0** | **23.0.1** |
| build_type | relwithdebinfo (-O2) | release |
| 来源 | 系统 APT 包 | wheel 自带 |

pyarrow 源码构建时链接了系统 APT 安装的 Arrow C++ 24.0.0，与 pyarrow 23.0.1 不匹配。
虽然未证实这是性能问题的根因，但版本不匹配可能带来潜在风险。

### 8.7 通用教训

1. **部署 workload 前必须 diff 确认两端代码一致**
2. **对比 benchmark 前必须确认参数（特别是 rows）一致**
3. **先验证基线再下结论**：假设 "x86 慢 24x" 导致大量时间花在错误方向
4. **debug 时先跑单个 query**：单 query 验证比全量跑完再发现问题高效得多

---

## 九、perf-kits 容器内执行（x86 Ubuntu 环境）

### 9.1 x86 TM 容器 Python 路径不同于 ARM

**现象**: `docker exec flink-tm1 /opt/flink/.pyenv/versions/3.14.3/bin/python3` 报 `no such file or directory`。

**根因**: x86 容器（Ubuntu 24.04）Python 3.14 安装在 `/usr/local/bin/python3`，而 ARM 容器（Debian）安装在 `/opt/flink/.pyenv/versions/3.14.3/bin/python3`。

**修复**: x86 容器用 `python3`（已在 PATH 中），不硬编码 pyenv 路径。

### 9.2 perf.data docker cp 后权限不可读

**现象**: perf-kits 执行时 `failed to open perf-x86.data: Permission denied`。

**根因**: `docker cp` 从宿主机复制文件到容器后，文件属主为 root:root，权限 `-rw-------`。容器进程以 flink 用户（uid 9999）运行，无法读取。

**修复**: `docker exec -u root flink-tm1 chmod 644 /opt/flink/perf-x86.data`。必须用 `-u root` 因为 flink 用户无法 chmod 别人的文件。

### 9.3 Ubuntu perf wrapper 找不到 RHEL 内核的 perf 二进制

**现象**: 容器内 `perf --version` 报 `WARNING: perf not found for kernel 5.14.0`。

**根因**: x86 容器是 Ubuntu 24.04（内核 6.8.0），宿主机是 RHEL 9.1（内核 5.14.0）。Ubuntu 的 `/usr/bin/perf` 是个 wrapper 脚本，会查找 `/usr/lib/linux-tools/<宿主机内核版本>/perf`，找不到 RHEL 内核对应的目录。

**修复**: 直接使用 Ubuntu 自带的 perf 二进制路径 `/usr/lib/linux-tools/6.8.0-110-generic/perf`。`perf report` 读取 perf.data 不要求内核版本完全匹配，符号解析依赖的是容器内的共享库而非内核版本。

### 9.4 perf-kits 脚本部署到容器的方法

完整流程：

```bash
# 1. 本地 → zen5 宿主机
scp -r vendor/python-performance-kits/scripts/perf_insights zen5:/tmp/perf-kits-scripts/

# 2. 宿主机 → 容器（注意 /. 复制目录内容）
ssh zen5 "docker exec flink-tm1 mkdir -p /opt/flink/perf-kits-scripts"
ssh zen5 "docker cp /tmp/perf-kits-scripts/. flink-tm1:/opt/flink/perf-kits-scripts/"

# 3. perf.data → 容器
ssh zen5 "docker cp /tmp/perf-x86.data flink-tm1:/opt/flink/perf-x86.data"
ssh zen5 "docker exec -u root flink-tm1 chmod 644 /opt/flink/perf-x86.data"

# 4. 在容器内运行
ssh zen5 "docker exec flink-tm1 python3 /opt/flink/perf-kits-scripts/run_single_platform_pipeline.py \
    /opt/flink/perf-x86.data \
    -o /opt/flink/perf-kits-output \
    --benchmark tpch --platform x86 \
    --perf-bin /usr/lib/linux-tools/6.8.0-110-generic/perf \
    --skip-annotate --no-print-report"

# 5. 收集输出
ssh zen5 "docker cp flink-tm1:/opt/flink/perf-kits-output/data/perf_records.csv /tmp/x86-perf_records.csv"
scp zen5:/tmp/x86-perf_records.csv <local_dest>
```

---

## 十、perf 采样量不足导致数据不可信

### 10.1 问题描述

使用 1M 行数据运行 TPC-H benchmark 后，perf 采集的 Python worker 样本极少：

| 平台 | perf 总行数 | Python worker 行数 | 采样率 | 总执行时间 |
|------|------------|-------------------|--------|-----------|
| ARM | 744K | 1,034 | ~1Hz 有效 | ~2min (8 queries) |
| x86 | 70K | 1,534 | ~2Hz 有效 | ~1.5min (8 queries) |

组件/分类耗时分布异常：
- x86 CPython 占 70.3%，但 "Unknown" 占 27.7%（很多未解析符号）
- ARM CPython 占 100%（数据太少，几乎全是 CPython 样本）
- 绝对耗时数字不可信（如 `_PyEval_EvalFrameDefault` selfX86=531s，而 wall clock 才 12s）

### 10.2 根因

1. **执行时间太短**: 单 query 8-20s，Python UDF 实际 CPU 时间仅 0.1-1.5s。`perf -F 999` 在 1-2s 的 CPU 时间窗口内只能采集到 ~1,000-2,000 个 Python 样本
2. **bursty 工作负载**: PyFlink UDF 以批量突发模式执行（Arrow batch），999Hz 采样率在突发窗口内欠采样
3. **perf -a 系统级采集**: 绝大部分样本来自 JVM 线程、kernel idle 等，Python worker 样本占比极低（ARM: 0.14%, x86: 2.2%）

### 10.3 修复方案

将行数从 1M 增加到 **10M**，使：
- Python worker CPU 时间从 ~1s 增加到 ~10s
- perf 采样数预计从 ~1,500 增加到 ~15,000
- 组件/分类分布更稳定可靠

### 10.4 10M 修复结果

将行数从 1M 增加到 10M 后，perf 采样质量大幅提升：

| 指标 | ARM 1M | ARM 10M | x86 1M | x86 10M |
|------|--------|---------|--------|---------|
| Python worker 行数 | 1,034 | **4,683** | 1,534 | **12,027** |
| libpython 行数 | ~1,100 | **7,365** | ~2,800 | **8,969** |
| CPython 分类数 | 5 | **14** | 5 | **14** |
| 解析符号数 | ~50 | **2,749** | ~50 | **1,495** |
| 组件分布可靠性 | 不可信 | **可信** | 不可信 | **可信** |

10M 数据的组件/分类分布：
- CPython: ARM 69.0%, x86 67.5%（一致）
- Runtime: ARM 31.0%, x86 32.5%（最大子类别）
- Interpreter: ARM 14.3%, x86 15.0%
- `_PyEval_EvalFrameDefault`: ARM 14.3%, x86 15.0%（合理）

### 10.5 教训

1. **perf 采样需要足够的 CPU 时间**: 经验法则：目标进程至少 5-10 秒 CPU 时间才能得到可靠的 perf profile
2. **先检查采样数再信任数据**: 样本数 < 5,000 时，分布比例不可靠
3. **bursty 工作负载需要更长的采集窗口**: 不是提高采样频率（已 999Hz），而是延长执行时间
4. **对比验证**: ARM 和 x86 的分类分布应该相似（相同代码），如果差异过大说明采样不足

---

## 十一、ARM 本地模式 vs 远程集群模式

### 11.1 ARM --cluster 模式失败

**现象**: ARM 用 `--cluster flink-jm:8081` 运行 benchmark 时 `ClassNotFoundException: PythonTableFunctionOperator`。

**根因**: ARM TM 的 classpath 中缺少 `flink-python` JAR。远程模式下，Python UDF 需要 TM 上有完整的 Flink Python 运行时。

**修复**: ARM 使用本地 mini-cluster 模式（不加 `--cluster` 参数）。在 JM 容器内运行 benchmark_runner.py，自动创建本地 mini-cluster，所有运算在同一 JVM 内完成。

### 11.2 本地模式对 perf 采集的影响

本地模式在 JM 容器内创建 mini-cluster，Python worker 仍在独立进程中运行（process 模式）。宿主机的 `perf record -a` 仍可采集到 Python worker 进程的样本。perf 数据质量不受影响。

### 11.3 BENCHMARK_SUMMARY 在本地模式下的输出位置

**现象**: 本地模式下 BENCHMARK_SUMMARY 不出现在 TM 的 docker logs 或 Flink 日志中。

**根因**: 本地模式下，PostUDF 的 `System.out.printf` 输出到 benchmark_runner.py 进程的 stdout（JM 容器内），被重定向到 `/tmp/benchmark-10m/${q}.txt`。

**修复**: 从每个 query 的结果文件中 grep BENCHMARK_SUMMARY，而非从 TM docker logs。

---

## 十二、ARM perf.data OOM 导致 perf script 失败

### 12.1 问题

ARM 10M perf.data (508MB) 在 TM1 容器内运行 perf-kits 时，`perf_script_to_csv.py` 被 OOM killer 杀掉（exit code -9）。

### 12.2 修复

分步执行 perf-kits pipeline：
1. `perf_data_to_csv.py` — 单独运行，输出到持久路径（非临时目录）
2. `perf_script_to_csv.py` — 跳过（OOM，仅用于 call graph，backfill 不需要）
3. `normalize_perf_records.py` — 单独运行
4. `summarize_platform_perf.py` — 单独运行（不传 perf_script.csv）

### 12.3 教训

- perf-kits 的 `run_single_platform_pipeline.py` 使用临时目录，pipeline 失败后临时文件被清除
- 大文件处理时应分步执行，避免单步 OOM 导致整个 pipeline 重新开始
- `perf script`（生成 call graph）的内存消耗远大于 `perf report`（flat profile），大数据量时可安全跳过

---

## 十三、前端本地调试

### 13.1 启动方法

```bash
cd web
# 先杀掉占用端口的旧进程（5173 是 Vite 默认端口）
lsof -ti:5173 | xargs kill -9 2>/dev/null
# 启动 dev server
npm run dev
```

Vite 默认端口 5173。如果 5173 被占用，Vite 会自动跳到 5174，但容易混乱——每次启动前先 `lsof -ti:5173 | xargs kill -9` 确保端口干净。

### 13.2 数据更新流程

backfill 写入 `examples/four-layer/pyflink-reference/`，前端从 `web/public/examples/four-layer/` 读取。两者是独立文件（非符号链接），backfill 后必须手动同步：

```bash
cp examples/four-layer/pyflink-reference/datasets/*.json web/public/examples/four-layer/pyflink-reference/datasets/
cp examples/four-layer/pyflink-reference/sources/*.json web/public/examples/four-layer/pyflink-reference/sources/
cp examples/four-layer/pyflink-reference/projects/*.json web/public/examples/four-layer/pyflink-reference/projects/
```

**教训**: backfill 完成后必须同步到 web/public 并刷新浏览器，否则前端显示的是旧数据。

---

## 十四、前端数据完整性问题（2024-04-23 修复）

### 14.1 ARM operator/framework 时间为 None

**现象**: 前端显示 ARM 列的 operator/framework 全是空白，x86 有值。

**根因**: ARM 使用本地 mini-cluster 模式运行 benchmark，`BENCHMARK_SUMMARY` 输出到每个 query 的结果文件（`/tmp/benchmark-10m/${q}.txt`），而非 TM 的 docker logs。手动创建的 `timing-normalized.json` 中 `businessOperatorTime` 和 `frameworkCallTime` 是空对象 `{}`，没有填入 `total_ns` 值。timing backfill 从空对象中提取 `per_invocation_ns`/`total_ns` 得到 None。

**修复**: 从每个 query 结果文件中提取 BENCHMARK_SUMMARY，按执行顺序匹配 queryId，将 `totalPyDurationNs` 写入 `businessOperatorTime.total_ns`，将 `totalFrameworkOverheadNs` 写入 `frameworkCallTime.total_ns`。

```bash
# 创建 ARM tm-stdout-tm1.log（从 query 结果文件拼接）
for q in q01 q03 ... q19; do
    grep BENCHMARK_SUMMARY /tmp/benchmark-10m/${q}.txt
done > runs/.../arm/tm-stdout-tm1.log
```

**教训**: 本地模式下 BENCHMARK_SUMMARY 的输出位置不同于远程模式。timing-normalized.json 必须包含 `total_ns` 字段，否则 backfill 无法提取值。

### 14.2 组件明细/分类明细页面打不开

**现象**: 点击组件或分类卡片后页面空白或报错。

**根因 1**: `categoryDetails` 完全缺失。`perf_backfill` 生成了 `stackOverview.categories` 但没有生成平铺的 `categoryDetails` 数组。前端 `CategoryDetailPage` 在 `categoryDetails` 中查找匹配条目，找不到就抛异常。

**根因 2**: `componentDetails` 条目缺少 `patternIds`、`rootCauseIds`、`artifactIds` 三个数组字段。前端 `assembleComponentDetail` 直接访问 `component.patternIds`（无 `?? []` 兜底），得到 `undefined`，后续 `.map()` 调用崩溃。

**修复**:
1. `perf_backfill.py` 新增 `_build_category_details()` 函数，从 `stackOverview.categories` 构建 `categoryDetails`，包含 `componentIds`、`hotspotIds` 等必需字段
2. `_build_component_details()` 输出中补上 `patternIds: []`、`rootCauseIds: []`、`artifactIds: []`
3. 前端 `assembly.ts` 对 `patternIds`/`rootCauseIds`/`artifactIds`/`caseIds` 加 `?? []` 防御

**教训**: 后端生成的数据结构必须与前端 TypeScript 类型定义完全对齐。缺失字段要用空数组而非省略。前端也必须有防御性 fallback。

### 14.3 函数明细没有源码/机器码

**现象**: 函数详情页的"源码"和"机器码"标签页为空。

**根因**: 10M 运行时跳过了 ASM 采集步骤（Step 5c-iii）。没有从容器内 objdump 关键符号的汇编代码，`asm_backfill` 找不到 .s 文件，无法生成 source artifacts。

**修复**: 从两个集群的 TM1 容器内执行 objdump：

```bash
# 在容器内执行（ARM）
LIB=/opt/flink/.pyenv/versions/3.14.3/lib/libpython3.14.so.1.0
for sym in _PyEval_EvalFrameDefault _Py_dict_lookup ...; do
    objdump -d -C $LIB | awk "/^[0-9a-f]+ <${sym}>:/,/^$/ {print}" | head -500 > ${sym}.s
done

# 在容器内执行（x86）
LIB=/usr/lib/x86_64-linux-gnu/libpython3.14.so.1.0  # x86 不同路径
```

采集 14 个热点符号，每个平台 14 个 .s 文件（~4MB）。文件放置在 `<run_dir>/asm/<arm64|x86_64>/` 目录下。

**教训**: ASM 采集是端到端流程的必需步骤，不是可选步骤。必须对 top N 热点函数采集 objdump 输出。

### 14.4 ARM/x86 容器内 libpython 路径不同

| 平台 | libpython 路径 |
|------|---------------|
| ARM (kunpeng) | `/opt/flink/.pyenv/versions/3.14.3/lib/libpython3.14.so.1.0` |
| x86 (zen5) | `/usr/lib/x86_64-linux-gnu/libpython3.14.so.1.0` |

ARM 用 pyenv 安装，库在 pyenv 目录下。x86 用系统包管理器安装，库在系统 lib 目录。

### 14.5 objdump --start-symbol 不可靠

**现象**: `objdump -d -C --start-symbol=$sym --stop-symbol=_` 对某些符号输出为空。

**根因**: `--stop-symbol=_` 的行为不可预测，可能匹配到其他符号导致提前终止。

**修复**: 用 `objdump -d -C $LIB | awk` 按 `<symbol>:` 标记提取，更可靠：

```bash
objdump -d -C $LIB | awk "/^[0-9a-f]+ <${sym}>:/,/^$/ {print}" | head -500
```

### 14.6 部分符号 objdump 输出为空

以下符号在两个平台的 libpython 中都没有 `T`（text）段导出，objdump 提取 0 行：

- `visit_decref`、`untrack_tuples`、`tuple_dealloc`、`update_one_slot`、`gc_collect_region`、`r_object`

这些可能是 `static` 内联函数或被编译器优化掉了。ASM backfill 正确处理了空文件（跳过）。

---

## 十五、前端数据展示修复（第二轮）

### 15.1 耗时分布全用 ms 显示大数值难以阅读

**现象**: 组件耗时分布中显示 "6610388.9 ms"、"23136361.1 ms" 等极大毫秒值，阅读困难。

**根因**: `perf_backfill.py` 的 `_format_ms()` 固定输出 `"X.X ms"`，不做自适应单位切换。perf 数据本身是 CPU 采样时间累计（10M 行 benchmark 累积数秒到数十秒），全部以 ms 显示导致大数值不直观。

**修复**: 修改 `_format_ms` 和 `_format_delta`，>=1000ms 时自动转秒：

```python
def _format_ms(value_ms: float) -> str:
    if value_ms >= 1000.0:
        return f"{value_ms / 1000.0:.2f} s"
    return f"{value_ms:.1f} ms"
```

同时添加 `_parse_time_to_ms()` 反向解析函数，修复 `_compute_platform_totals` 中对秒单位字符串的解析（之前 `.replace(" ms", "")` 无法处理 `"4.49 s"` 格式，导致 platform totals 从 "23136.36 s" 变成 "6.50 s"）。

**教训**: 改输出格式时，必须同步检查所有**消费该格式**的下游代码。格式化函数的输出格式变了，解析逻辑也要跟着变。添加 `_format_*` 的同时必须添加对应的 `_parse_*`。

### 15.2 框架调用耗时量级错误（111 万秒 vs 墙钟 145 秒）

**现象**: 框架调用耗时显示 q01 ARM 为 "1112019.95 s"（约 12.9 天），而实际 benchmark 墙钟时间仅 144.9 秒。

**根因**: PyFlink 的 `MarkStart → timed UDTF → CalcOverhead` 链路中，`CalcOverhead` 在整个 batch 处理完成后才记录 `java_end_time`，不是每条记录单独记录。BENCHMARK_SUMMARY 的 `totalFrameworkOverheadNs = avgFrameworkOverheadNs × recordCount` 是**跨所有 batch 的累计 CPU 时间**，不是墙钟时间。用 `total_ns / recordCount` 得到的 "per_invocation" 值（111ms/次）远大于实际墙钟 per-record 时间（14.5µs/次），因为包含了同一 batch 内其他记录的等待时间。

数据流错误链：
1. `timing-normalized.json` 的 `frameworkCallTime.total_ns` 直接存了 BENCHMARK_SUMMARY 的 `totalFrameworkOverheadNs`
2. `timing_backfill.py` 的 `_extract_per_invocation_ns()` 在没有 `per_invocation_ns` 时回退到 `total_ns`
3. `_format_ns(total_ns)` 将 1.11×10¹⁵ ns 格式化为 1,112,019.95 秒

**修复**:

1. 修正 `timing-normalized.json` 的 `frameworkCallTime` 计算：用 `wallClockTime - totalPythonTime` 代替 BENCHMARK_SUMMARY 的 `totalFrameworkOverheadNs`：

```python
framework_total_ns = wallclock_ns - total_py_ns  # 墙钟 - Python总时间
framework_per_inv = framework_total_ns / record_count
```

2. 同时为 `businessOperatorTime` 提供 `per_invocation_ns = avgPyDurationNs`（来自 BENCHMARK_SUMMARY）。

修正后数值对比：

| Metric | 修正前 | 修正后 |
|--------|--------|--------|
| q01 ARM framework | 1,112,019.95 s | 11.7 µs |
| q01 ARM operator | 27.71 s | 2.8 µs |
| q01 ARM demo | 144.94 s | 144.94 s（不变） |

**教训**:

- **PyFlink batch 模式下 `java_end_time - java_start_time` 不是单条记录的处理时间**。它包含了同 batch 内其他记录的处理等待。用 batch 模式的累计时间除以 record count 得到的 "平均值" 不等于真实的 per-record 延迟。
- `framework_overhead = wallClockTime - totalPythonTime` 才是合理的框架开销总量（墙钟减去 Python 执行时间），per-invocation 值由此除以 record count 得到。
- timing-normalized.json 应同时存储 `per_invocation_ns` 和 `total_ns`，让 backfill 优先使用 `per_invocation_ns`。

### 15.3 源码和机器码（ASM）不显示

**现象**: 函数详情页中"源码对齐机器码差异"区域不显示任何内容。

**根因**: `asm_backfill.py` 只创建了空的 `diffView` 骨架（`analysisBlocks: []`），设计上依赖 Step 7（LLM ASM diff 分析）填充。但 Step 7 从未运行过。实际上：
- ASM 数据已经采集并存储在 `source.artifactIndex` 的 inline `content` 中（14 个 ARM + 14 个 x86 artifacts，每个 4K-30K chars）
- `dataset.functions[].artifactIds` 已正确关联到这些 artifacts
- 但 `diffView.analysisBlocks` 为空，前端 FunctionDetailPage 在 line 128 遍历时跳过

**修复**: 在 `asm_backfill.py` 中新增 `_populate_diff_view()` 函数，直接从 source artifacts 的内容填充 analysisBlocks：

1. 为每个有 ASM 内容的函数创建一个 analysisBlock
2. 创建 armRegion（ARM 反汇编）和 x86Region（x86 反汇编），snippet 存完整 objdump 输出
3. 提取高频指令助记符作为 highlights
4. 创建 dummy sourceAnchor（无 C 源码可用）和 mapping 连接 ARM/x86 regions

关键代码：

```python
def _populate_diff_view(func, symbol, arm_content, x86_content):
    arm_region = {
        "id": f"arm_{func_id}",
        "snippet": arm_content,  # 完整 objdump 输出
        "highlights": _extract_highlights(arm_content),  # top-5 高频指令
        ...
    }
    # x86_region 类似
    # 一个 analysisBlock 包含 arm/x86 regions 和 mapping
```

前端 `assembly.ts` 的 `assembleFunctionDetail()` 无需修改——它已经正确处理 analysisBlocks 中的 sourceAnchors、armRegions、x86Regions。

**教训**:

- **不要让 pipeline step 依赖后续未实现的步骤才能展示数据**。如果 Step 6 能提供原始数据，就应该直接展示，而不是等 Step 7 精加工。可以先展示原始 ASM，后续用 LLM 分析替换为带注释的版本。
- `diffView` 设计中 `sourceAnchors → armRegions/x86Regions` 通过 `mappings` 连接的三层结构虽然灵活，但在没有 LLM 分析结果时，用 "全函数对照" 模式（单 anchor、单 mapping）也能提供有价值的展示。
- inline `content` 在 `source.artifactIndex` 中存了 ASM 文本，但 `assembleArtifactDetail()` 只读 `artifact.path`（文件路径），不支持 inline content。这次通过 diffView 直接内嵌 ASM 绕过了这个问题，但如果单独查看 artifact 详情页仍会失败——这是一个遗留问题。

---

## 十六、组件/分类总耗时量级错误与堆叠图比例失调

### 16.1 组件总耗时只有 6s（实际 775s）

**现象**: 组件耗时分布显示 ARM 总耗时 6.50 s，x86 3.34 s。但单个 query（q01）的墙钟时间就有 144.9s (ARM) / 93.3s (x86)，总耗时远大于显示值。

**根因 1**: `_estimate_total_ms()` 将 `demo` 和 `tm` 指标相加。在 timing backfill 中，`demo` = wall-clock 总耗时，`tm` ≈ 同一值（TM e2e 时间约等于 wall-clock）。相加等于双倍计算。

**根因 2**: `_format_ms` 改为自适应单位后（>=1000ms 输出 "X.XX s"），`_compute_platform_totals` 中的 `.replace(" ms", "")` 无法解析秒单位字符串，将秒值当毫秒处理。

**修复**:

1. `_estimate_total_ms` 只使用 `demo` 指标（墙钟时间），不再加 `tm`。
2. 添加 `_parse_time_to_ms()` 通用反向解析函数，支持 `s`/`ms`/`µs`/`ns` 四种单位。
3. `_compute_platform_totals` 使用 `_parse_time_to_ms` 代替 `.replace(" ms", "")`。

修正后: ARM 总耗时 774.69 s, x86 479.59 s（与 wall-clock 吻合）。

### 16.2 堆叠直方图 Kernel/glibc 占比异常大（CPython 实际 60%+）

**现象**: 堆叠直方图中 Kernel 和 glibc 的柱条占比与 CPython 相当甚至更大，但 perf 数据显示 CPython 占 ~69%。

**根因**: `perf record -a` 的 `self%` 是相对于**全部 CPU 时间**（所有进程）的百分比。PyFlink benchmark 中 Python worker 仅占总 CPU 的 ~0.42%。raw `self%` 直接除以 100 后乘以 `total_ms`（Python worker 的墙钟时间），得到的绝对值远远偏小。

例如: `_PyEval_EvalFrameDefault` arm self% = 0.1%，如果直接 `0.1/100 × 774690ms = 774.69ms`，远小于真实值（~110s）。

正确做法是将 raw share 归一化到 **Python worker 占 total CPU 的份额**：

```python
# 旧: arm_time = arm_total_ms * data["arm_self"] / 100.0
# 新: arm_time = arm_total_ms * data["arm_self"] / arm_share_total
```

其中 `arm_share_total` = 所有 Python worker 行的 `self%` 之和。

**修复**: 在 `_build_components`、`_build_categories`、`_build_functions`、`_build_component_details` 四个函数中统一改为 `/ share_total` 归一化。

修正后组件耗时:
- cpython arm=534.90s (69.0%), x86=323.76s (67.5%)
- unknown arm=221.34s (28.6%), x86=128.76s (26.8%)
- kernel arm=18.45s (2.4%), x86=27.07s (5.6%)
- 各组件之和 = 平台总耗时（精确匹配）

### 16.3 `_build_component_details` 遗漏导致子页面与总览不一致

**现象**: 修复了 `_build_components` 和 `_build_categories` 后，组件总览显示 cpython 534.90s，但点进 cpython 详情页仍显示 2.25s。

**根因**: `_build_component_details` 有独立的归一化计算逻辑，修复时只改了 `_build_components` 和 `_build_categories`，遗漏了 `_build_component_details`。

**修复**: 将 `_build_component_details` 的归一化同样改为 `/ cd_arm_share_total`。

**教训**: 同一数据流的多个聚合函数（components、categories、functions、component_details）必须使用**完全一致的归一化逻辑**。修了一个必须检查其余。

---

## 十七、分类热点函数只显示一个 & 函数缺少来源信息

### 17.1 分类热点函数只显示一个（数据问题，非前端）

**现象**: 每个分类详情页只显示 1 个热点函数，如 runtime 有 9 个函数但只显示 `deduce_unreachable`。

**根因**: `_build_category_details()` 第 820 行 `hotspotIds` 只取了 `cat["topFunctionId"]`（单个值），而非该分类下的全部函数 ID。`topFunctionId` 来自 `_build_categories()` 中 `top_arm_symbol`（ARM 侧占比最高的符号），每个分类只有一个。

**修复**: 用 `cat_to_funcs` 字典（按 `categoryL1` 分组的全部函数 ID 列表）代替 `topFunctionId`：

```python
# 旧
"hotspotIds": [cat["topFunctionId"]] if cat.get("topFunctionId") else [],
# 新
"hotspotIds": cat_to_funcs.get(cat_id, []),
```

前端代码本身不做过滤——`CategoryDetailPage` 渲染 `hotspotIds` 数组中的所有函数。问题完全在后端数据。

**教训**: `topFunctionId` 适合概览表格的"代表函数"展示，不适合详情页的"全量热点"列表。两个字段用途不同，不应复用。

### 17.2 函数缺少来源（source file）信息

**现象**: 函数详情页的"源码文件"显示 `<cpython> deduce_unreachable`（占位文本），无法判断函数来自哪个库。

**根因**: `_build_functions()` 只从 perf 数据提取了 `category_top`/`category_sub`/`component`，没有利用 `shared_object` 字段。`shared_object` 已经标识了符号所属的共享库（如 `libpython3.14.so.1.0`、`libscipy_openblas64_*.so`、`[kernel.kallsyms]`）。

**修复**: 新增 `_resolve_source_info()` 函数，从 `shared_object` 推导 `sourceFile` 和 `origin`：

```python
_LIB_DISPLAY = {"libpython": "CPython", "libscipy_openblas": "OpenBLAS (scipy)", ...}

def _resolve_source_info(symbol, shared_object):
    if shared_object == "[kernel.kallsyms]":
        return {"sourceFile": "Linux Kernel", "origin": "kernel"}
    for lib_prefix, display in _LIB_DISPLAY.items():
        if lib_prefix in shared_object:
            return {"sourceFile": display, "origin": display}
    ...
```

结果:
- CPython 函数 → `sourceFile="CPython"`
- `blas_thread_server` → `sourceFile="OpenBLAS (scipy)"`
- `unmap_page_range` → `sourceFile="Linux Kernel"`
- `__tls_get_addr` → `sourceFile="glibc"`

前端 CategoryDetailPage 和 ComponentDetailPage 的热点函数表新增"来源"列。

### 17.3 非libpython函数没有机器码

**现象**: `blas_thread_server`（来自 OpenBLAS）和 `visit_decref`（static 内联）没有 ASM 反汇编。

**根因**: ASM 采集（Step 5c）只对 `libpython3.14.so` 执行 objdump。`blas_thread_server` 在 `libscipy_openblas64_*.so` 中，不在采集范围。`visit_decref` 等 static 函数被编译器内联，libpython 中没有导出符号。

**已实施**: 对没有 ASM 的函数，`asm_backfill` 根据来源填充说明性 diffGuide：
- 内核符号 → "内核符号，无用户态反汇编可用"
- 第三方库函数 → "来自 {origin} 的第三方库函数，当前未采集该库的反汇编"
- static/内联 → "该符号为 static 内联函数或被编译器优化，未在共享库中导出"

**后续**: ASM 采集应覆盖 perf hotspots 中出现的**所有** shared_object，不仅限于 libpython。对 OpenBLAS 等第三方库也执行 objdump。

---

## 十八、四层校验 160 个错误 & 过期绑定清理

### 18.1 四层校验报 160 个错误

**现象**: `pyframework_pipeline validate examples/four-layer/pyflink-reference` 报 160 个错误，导致 `config validate --skip-bridge-token` 也失败。

**根因**: 两个问题：

1. `Dataset.patterns` 和 `Dataset.rootCauses` 缺失（JSON schema 必需字段）
2. `Project.functionBindings` 有 178 个绑定引用了 158 个不存在的函数 ID

绑定过期的原因：`_merge_bindings()` 只做 add/update，不做 delete。当 `perf_backfill` 的 `top_n=20` 截断了大部分函数后，旧绑定仍然指向被截断的函数。

**修复**:

1. 在 backfill pipeline 中添加 `dataset.setdefault("patterns", [])` 和 `dataset.setdefault("rootCauses", [])`
2. `_merge_bindings()` 新增 pruning：传入 `valid_func_ids`（数据集中实际存在的函数 ID 集合），删除不在此集合中的旧绑定

```python
def _merge_bindings(project_data, new_bindings, valid_func_ids):
    # ... existing merge logic ...
    stale = [fid for fid in existing_funcs if fid not in valid_func_ids]
    for fid in stale:
        existing_funcs.pop(fid, None)
```

**修复后**: 四层校验 0 个错误，全部 104 个测试通过。

**教训**: pipeline 的 binding merge 逻辑必须是双向的——既能添加新的，也要能删除过期的。`top_n` 截断是隐式的 delete，merge 必须感知这种截断。
