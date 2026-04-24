# python-framework-analysis-pipeline

用于构建 Python 框架分析流程的仓库。

当前仓库内置的是一套面向 PyFlink 的参考实现，包括：

- 一组 PyFlink 专用分析规范
- 一套前端 demo
- 一份示例数据包
- 一套自动化分析流程的初始 CLI 骨架

仓库的目标不是长期停留在“单一 PyFlink 报告”，而是演进到一套可复用于不同 Python 框架的软件分析流程。

当前抽象方向已经明确为四层模型：

- `Framework`
- `Dataset`
- `Source`
- `Project`

其中：

- `Framework` 定义框架边界、分类体系和指标口径
- `Dataset` 定义用例、结果、热点、模式和根因
- `Source` 定义源码仓、revision、源码索引和附件索引
- `Project` 通过显式绑定表把前三者装配成一份可展示的具体分析项目

## 当前范围

当前仓库当前主要包含以下内容：

- PyFlink 框架耗时归属规范
- TPC-H SQL 到整体 Python UDF 工作负载与计时规范
- 热点函数、模式、根因与证据建模规范
- 四层抽象方案
- 报告数据 schema 规范
- 报告页面字段规范
- 前端 demo 骨架与示例数据包
- 一套可继续抽象到其他 Python 框架的软件分析流程基础

## 目录结构

- `docs/specs/`
  - PyFlink 专用规范
  - 四层抽象方案
  - 报告 schema 与页面规范
  - 环境搭建架构规范
- `docs/plans/`
  - 前端实现计划、自动化流程路线图与阶段性设计记录
- `docs/runbooks/`
  - 环境搭建执行手册
- `schemas/`
  - 四层输入和校验报告的 JSON Schema
- `pipelines/`
  - 自动化分析流程 CLI、校验器、步骤接口和框架适配器
- `projects/`
  - 真实分析项目的配置、采集产物和运行记录
- `workload/`
  - Benchmark 用例定义和框架专属 UDF 实现
  - `workload/tpch/sql/` — 22 条原始 TPC-H SQL
  - `workload/tpch/pyflink/` — 13 条 PyFlink 纯 Python UDF + 公共 runner
- `web/`
  - 前端 demo
- `web/public/examples/four-layer/`
  - 前端 demo 直接加载的四层示例输入
- `web/public/examples/four-layer/pyflink-reference/artifacts/`
  - 四层 `Source.artifactIndex` 引用的示例证据附件

## 后续方向

- 根据真实实机数据回看并收紧 JSON Schema
- 打通环境搭建、用例生成、采集、回填的最小自动化闭环
- 扩充更多 Python 框架的四层示例输入
- 继续完善正式汇报 demo 的页面表达与证据对比能力

## Pipeline 流程总览

Pipeline 按 Step 1→7 串行执行，每个 Step 有明确的输入、输出和运行位置。运行前务必对照此表确认每步的依赖就绪。

### 流程步骤

| Step | 名称 | 运行位置 | 输入 | 输出 |
|------|------|---------|------|------|
| **1** | 配置校验 | 本地 | `project.yaml`、`environment.yaml`、四层目录、`workload/` | 校验报告（stdout） |
| **3** | 环境部署 | 远程 SSH | `project.yaml`、`environment.yaml` | `<run>/<platform>/environment-plan.json`、`environment-record.json`；远程: Docker 1JM+2TM 集群 |
| **4** | Workload 部署 | 本地→远程→容器 | `workload/tpch/pyflink/` 全目录 | 远程容器内 `/opt/flink/usrlib/` 下的 benchmark_runner、UDF、Java UDF JAR |
| **5a** | Benchmark 执行 | 远程→容器内 | 容器内 workload；`project.yaml` 中 `queries`、`rows` | `<run>/<platform>/timing/timing-normalized.json`；`<run>/<platform>/tm-stdout-tm*.log`；容器内 `/tmp/perf-udf.data` |
| **5b** | 数据采集 | 远程→容器内→本地 | 容器内 `perf-udf.data`；`vendor/python-performance-kits/` 脚本 | `<run>/<platform>/perf/data/perf_records.csv`、`perf-*.data`；`<run>/<platform>/perf/tables/*.csv`；`<run>/<platform>/asm/<arch>/*.s` |
| **5c** | Acquire 汇总 | 本地 | S5a/S5b 的全部产物 | `<run>/<platform>/acquisition-manifest.json`；补充缺失的 timing/perf/asm 产物 |
| **6** | Backfill 回填 | 本地 | ARM+x86 两端的 `timing-normalized.json`、`perf_records.csv`、`asm/*.s`；四层 JSON | 更新后的四层 JSON: `*.dataset.json`、`*.source.json`、`*.project.json` |
| **7** | Bridge 桥接 | 本地→GitHub API | 四层 JSON 中的 functions、artifacts；`PYFRAMEWORK_BRIDGE_TOKEN` | GitHub/GitCode Issues/Discussions；更新后的 `diffView`、`patterns`、`rootCauses` |

### 关键文件路径速查

**四层输入/输出** (Step 6 读写)：

```
examples/four-layer/pyflink-reference/
  datasets/tpch-on-pyflink-2026q2.dataset.json   ← cases, functions, stackOverview, componentDetails, categoryDetails
  sources/pyflink-reference-source.source.json    ← artifactIndex (含 inline ASM content), sourceAnchors
  projects/tpch-pyflink-reference.project.json    ← caseBindings, functionBindings
  frameworks/pyflink.framework.json               ← 分类体系、指标定义 (只读)
```

**运行产物目录** (Step 5→6 产生)：

```
projects/<project>/runs/<run-id>/
  pipeline-run.json                               ← 运行状态追踪
  <platform>/
    timing/timing-normalized.json                  ← Step 5a: wallClock, operator, framework per query
    perf/data/perf_records.csv                     ← Step 5b: 符号/分类/占比的 perf CSV
    perf/tables/category_summary.csv               ← CPython 分类汇总
    perf/tables/symbol_hotspots.csv                ← 热点符号
    asm/<arm64|x86_64>/<symbol>.s                  ← objdump 反汇编
    tm-stdout-tm1.log                              ← BENCHMARK_SUMMARY 原始输出
```

**前端展示数据** (Step 6 输出后需同步)：

```
web/public/examples/four-layer/pyflink-reference/
  datasets/tpch-on-pyflink-2026q2.dataset.json    ← 从四层目录 cp
  sources/pyflink-reference-source.source.json     ← 从四层目录 cp
  projects/tpch-pyflink-reference.project.json     ← 从四层目录 cp
```

### 运行前检查清单

每次运行前按此清单核对，避免重复踩坑：

| 检查项 | 确认方式 | 踩坑记录 |
|--------|---------|---------|
| `project.yaml` 中 `rows` 值 | `grep rows project.yaml` | §八: 默认 10M 但 yaml 里写的 1M，搞错导致结论全错 |
| ARM/x86 workload 代码一致 | `diff` 两端 `benchmark_runner.py` | §八: x86 多了 JSON I/O，慢 6.4x |
| ARM 用本地模式（不加 `--cluster`） | 检查运行脚本无 `--cluster` | 十一: ARM TM classpath 缺 flink-python JAR |
| x86 perf 二进制路径 | 用完整路径 `/usr/lib/linux-tools/.../perf` | 九: `/usr/bin/perf` wrapper 找不到内核对应工具 |
| 容器内 python 路径 | ARM: pyenv 全路径；x86: `/usr/local/bin/python3` | 14.4: 两平台 Python 安装方式不同 |
| `docker cp` 后文件权限 | `docker exec -u root ... chmod 644` | 十二: cp 后 root:root -rw-------，容器用户无法读 |
| perf 采样量 ≥ 5000 | 检查 `perf_records.csv` 行数 | 十: 1M 行仅 ~1000 样本分布不可信，用 10M |
| timing-normalized.json 用 `per_invocation_ns` | 检查 JSON 含该字段 | 15.2: 只有 `total_ns` 时回填会用累计值，框架耗时 111 万秒 |
| Backfill 后同步到 `web/public/` | `cp` 四层 JSON → `web/public/...` | 前端从 `web/public/` 加载，不从 `examples/` 读取 |
| 前端 dev server 端口 5173 被占 | `lsof -ti:5173 \| xargs kill -9` | 默认端口 5173，被占时 Vite 用 5174 导致看旧版本 |
| 改 `_format_*` 后同步改 `_parse_*` | 检查所有消费格式化字符串的代码 | 15.1: 改了输出为秒但解析还假定毫秒，totals 从 23136s 变 6.5s |
| perf self% 归一化用 `/ share_total` | 检查 `_build_*` 函数不做 `/ 100` | 16.2: Python worker 仅 0.42% CPU，`/100` 得到 6s（实际 775s） |
| 平台总耗时只用 `demo` 指标 | 检查 `_estimate_total_ms` 不 sum 重叠指标 | 16.1: demo+tm 双倍计算 |
| 修 `_build_*` 归一化时检查全部四函数 | components/categories/functions/component_details 全检查 | 16.3: 只改了前两个，componentDetails 遗漏导致详情页 2.25s |

## Pipeline CLI

当前 CLI 的第一步是配置获取和完整性校验。真实项目必须先通过 `config validate`，再进入远程环境、workload、benchmark、采集、回填和 Issue 桥接；配置不完整时 `run` 会在本地直接失败，不会连接 SSH 或修改远程 Docker 状态。

```bash
PYTHONPATH=pipelines python3 -m pyframework_pipeline --help
PYTHONPATH=pipelines python3 -m pyframework_pipeline config validate projects/pyflink-tpch-reference/project.yaml
PYTHONPATH=pipelines python3 -m pyframework_pipeline validate examples/four-layer/pyflink-reference
PYTHONPATH=pipelines python3 -m pyframework_pipeline validate projects/pyflink-tpch-reference/project.yaml
```

`config validate` 会检查 `project.yaml`、同目录 `environment.yaml`、四层输入目录、`workload.localDir`、`run.platforms`、平台 `hostRef`、`software.flinkPyflinkImages` 以及 `bridge` 配置。默认还会检查 `bridge.tokenEnvVar` 指向的环境变量是否存在，并拒绝明显占位 token；只验证桥接前流程时可加 `--skip-bridge-token`。

真实一键流程示例：

```bash
export PYFRAMEWORK_BRIDGE_TOKEN=<real-github-or-gitcode-token>
PYTHONPATH=pipelines python3 -m pyframework_pipeline run projects/pyflink-tpch-reference/project.yaml --yes
```

`run` 会按 Step 3→7 串起环境部署、workload 上传、benchmark、远程采集、本地解析、回填和 issue 发布。常用控制参数：

```bash
# 指定运行目录
PYTHONPATH=pipelines python3 -m pyframework_pipeline run projects/pyflink-tpch-reference/project.yaml --run-dir projects/pyflink-tpch-reference/runs/2026-04-20-e2e --yes

# 从失败步骤恢复，例如重新从采集阶段开始
PYTHONPATH=pipelines python3 -m pyframework_pipeline run projects/pyflink-tpch-reference/project.yaml --run-dir projects/pyflink-tpch-reference/runs/2026-04-20-e2e --resume-from 5b --yes

# 只跑到某一步之前，用于本地预检
PYTHONPATH=pipelines python3 -m pyframework_pipeline run projects/pyflink-tpch-reference/project.yaml --stop-before 3
```

如果 `--stop-before 7` 或更早，本次运行不会进入 Issue 桥接，因此 `run` 不要求 `PYFRAMEWORK_BRIDGE_TOKEN`。完整运行到 Step 7 时仍必须提供 token。

也可以逐阶段执行：

```bash
# Step 3: 环境
PYTHONPATH=pipelines python3 -m pyframework_pipeline environment plan projects/pyflink-tpch-reference/project.yaml --platform arm --output projects/pyflink-tpch-reference/runs/arm-env
PYTHONPATH=pipelines python3 -m pyframework_pipeline environment deploy projects/pyflink-tpch-reference/project.yaml --platform arm --plan projects/pyflink-tpch-reference/runs/arm-env/environment-plan.json --yes
PYTHONPATH=pipelines python3 -m pyframework_pipeline environment validate projects/pyflink-tpch-reference/runs/arm-env

# Step 4-5: workload、benchmark、远程采集和本地解析
PYTHONPATH=pipelines python3 -m pyframework_pipeline workload deploy projects/pyflink-tpch-reference/project.yaml --platform arm
PYTHONPATH=pipelines python3 -m pyframework_pipeline benchmark run projects/pyflink-tpch-reference/project.yaml --platform arm --run-dir projects/pyflink-tpch-reference/runs/2026-04-20-e2e
PYTHONPATH=pipelines python3 -m pyframework_pipeline collect run projects/pyflink-tpch-reference/project.yaml --platform arm --run-dir projects/pyflink-tpch-reference/runs/2026-04-20-e2e
PYTHONPATH=pipelines python3 -m pyframework_pipeline acquire all projects/pyflink-tpch-reference/project.yaml --platform arm --run-dir projects/pyflink-tpch-reference/runs/2026-04-20-e2e/arm

# Step 6-7: 回填和 Issue 桥接
PYTHONPATH=pipelines python3 -m pyframework_pipeline backfill run projects/pyflink-tpch-reference/project.yaml --arm-run-dir projects/pyflink-tpch-reference/runs/2026-04-20-e2e/arm --x86-run-dir projects/pyflink-tpch-reference/runs/2026-04-20-e2e/x86
PYTHONPATH=pipelines python3 -m pyframework_pipeline bridge publish projects/pyflink-tpch-reference/project.yaml --dry-run
PYTHONPATH=pipelines python3 -m pyframework_pipeline bridge fetch projects/pyflink-tpch-reference/project.yaml
```

环境计划会优先使用 `environment.yaml` 中的 `software.flinkPyflinkImages.<platform>`，例如 arm 使用 `flink-pyflink:2.2.0-py314-arm-final`，x86 使用 `flink-pyflink:2.2.0-py314-x86-final`。`software.flinkImage` 只作为未配置平台专属镜像时的 fallback。

环境部署命令是幂等的：镜像已存在时跳过 `docker pull`；容器已存在且镜像匹配时复用/启动；容器已存在但镜像不匹配时删除并按当前配置重建。

## 前端应用

前端工程位于 `web/`。

### 本地运行

```bash
cd web
npm install
npm run dev
```

### 构建

```bash
cd web
npm run build
```

### 数据来源

当前前端 demo 默认从 `web/public/examples/four-layer/pyflink-reference/` 读取四层示例输入。加载顺序是：

1. `Project`
2. `Framework`
3. `Dataset`
4. `Source`

`web/src/data/assembly.ts` 负责把四层输入组装成页面 view model。`web/src/data/loaders.ts` 只调用组装层，不再读取旧的 `summary/details` 数据包，也不再使用 mock fallback。

`web/public/examples/four-layer/pyflink-reference/artifacts/` 是示例附件目录；它必须通过 `Source.artifactIndex` 引用，页面不能直接硬编码附件路径。
