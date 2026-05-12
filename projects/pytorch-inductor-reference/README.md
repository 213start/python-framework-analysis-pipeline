# PyTorch Inductor Pipeline 运行说明

本项目使用统一的 `pyframework_pipeline` 执行环境部署、workload 部署、benchmark、perf 数据处理、双平台对比等流程。

一键运行命令：

```powershell
python -m pyframework_pipeline run projects/pytorch-inductor-reference/project.yaml --yes
```

如果某一步失败，或者只想从某个阶段重新执行，可以使用：

```powershell
--resume-from <步骤号>
```

`--resume-from` 会清理指定步骤及其后续步骤的状态，然后从该步骤继续执行。

## Resume 步骤表

| 参数 | 重新开始的阶段 | 功能说明 |
|---|---|---|
| `--resume-from 3` | Step 3 环境部署，environment deploy | 重新执行环境搭建，包括执行环境计划、检查或构建镜像、启动容器、执行 readiness 校验。 |
| `--resume-from 4` | Step 4 workload 部署，workload deploy | 重新上传 workload 到远程主机，并复制到 runner 容器内。 |
| `--resume-from 5a` | Step 5a benchmark 执行，benchmark run | 重新运行 PyTorch benchmark，拉回 `pytorch-results`，并生成 timing JSON。 |
| `--resume-from 5b.1` | Step 5b.1 收集 perf data，collect perf data | 扫描已拉回的 PyTorch `.data` 文件，并生成 PyTorch perf manifest。 |
| `--resume-from 5b.2` | Step 5b.2 运行 perf-kits，run perf-kits | 对每个 PyTorch region 分别运行 perf-kits，并生成各自的 `perf_records.csv`。 |
| `--resume-from 5b.2b` | Step 5b.2b 提取 CPython 源码，extract CPython source | 基于每个 region 的 perf CSV 提取源码映射，生成 `symbol_source_map.json`。 |
| `--resume-from 5b.3` | Step 5b.3 收集反汇编，collect objdump ASM | 基于每个 region 的 perf CSV 收集热点符号的 objdump 反汇编结果。 |
| `--resume-from 5c` | Step 5c 本地解析，acquire all | 执行本地采集/解析检查，并复用前面已经生成的 PyTorch 产物。 |
| `--resume-from 6` | Step 6 数据回填，backfill run | 将 timing、perf、ASM 等数据回填到四层数据模型。 |
| `--resume-from 6b` | Step 6b 双平台对比，platform compare | 重新执行 ARM/x86 双平台对比。PyTorch 会按 region 分别对比。 |
| `--resume-from 7` | Step 7 结果发布，bridge publish | 将结果发布到配置的外部平台，例如 GitHub/GitCode。 |

补充：

```text
--resume-from 5b
```

是旧别名，等价于：

```text
--resume-from 5b.1
```

## 常用命令

运行前先设置本地 Python 环境变量：

```powershell
$env:PYTHONPATH='pipelines'
$env:PYTHONIOENCODING='utf-8'
```

只重新执行环境部署：

```powershell
python -m pyframework_pipeline run projects/pytorch-inductor-reference/project.yaml --yes --resume-from 3 --stop-before 4
```

从 workload 部署开始，执行到 benchmark 完成后停止：

```powershell
python -m pyframework_pipeline run projects/pytorch-inductor-reference/project.yaml --yes --resume-from 4 --stop-before 5b.1
```

benchmark 已完成，只补齐 PyTorch perf CSV、源码映射、ASM 和双平台对比：

```powershell
python -m pyframework_pipeline run projects/pytorch-inductor-reference/project.yaml --yes --resume-from 5b.1 --stop-before 7
```

只重新执行双平台对比：

```powershell
python -m pyframework_pipeline run projects/pytorch-inductor-reference/project.yaml --yes --resume-from 6b --stop-before 7
```

## 输出目录

如果不指定 `--run-dir`，默认本地输出目录是：

```text
projects/pytorch-inductor-reference/runs/<UTC日期>
```

例如：

```text
projects/pytorch-inductor-reference/runs/2026-05-09
```

PyTorch benchmark 原始结果：

```text
<run-dir>/<platform>/pytorch-results/
<run-dir>/<platform>/timing/
```

PyTorch 按 region 独立保存的 perf 输出：

```text
<run-dir>/<platform>/perf/pytorch/aot_trace_joint_graph/
<run-dir>/<platform>/perf/pytorch/fw_compiler_base/
<run-dir>/<platform>/perf/pytorch/bytecode_tracing/
```

PyTorch 双平台对比输出：

```text
<run-dir>/compare/pytorch/
```

其中 `<platform>` 通常是：

```text
arm
x86
```
