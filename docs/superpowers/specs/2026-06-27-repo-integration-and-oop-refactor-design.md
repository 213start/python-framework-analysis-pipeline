# 仓库整合与 OOP 重构设计

## Goal

把 `python-performance-kits` 仓库整合进本仓(`python-framework-analysis-pipeline`),并按 OOP 原则重构项目架构,提升**兼容性、可扩展性、可测试性、可定位性**。

重构不是简单搬移 kits 代码,而是把 kits 里"采集与分析混在一起的一条龙脚本"拆解,填补进重构后的目标架构,使采集与分析成为逻辑上彻底分离的两条流程。

## Background(现状与痛点)

### 两仓关系
- 本仓通过 `vendor/python-performance-kits` git submodule 引用 kits。
- kits 是一个"单脚本可独立运行"的工具集合(17 个脚本,~5800 行),核心是 `scripts/perf_insights/` 下的一组 `perf.data → 分类 CSV` 的分析脚本。
- 本仓 pipeline 通过 **subprocess** 调用 kits 脚本,共 **6 个耦合点**:`acquisition/perf_profile.py`、`acquisition/machine_code.py`、`backfill/perf_backfill.py`、`compare/pipeline.py`、`orchestrator.py`、文档。

### 架构痛点
1. `orchestrator.py` **2577 行**,三种框架(pyflink/datajuicer/udfbenchmarking)的逻辑全揉在一个文件里。
2. `FrameworkAdapter` Protocol 只有 `framework_id` + `describe()` 两个成员,形同虚设,框架特化逻辑散落在 orchestrator。
3. `cli.py` **1005 行**、`backfill/perf_backfill.py` **1265 行**,均职责过载。
4. kits 脚本之间通过 `import` 彼此依赖,本仓无法对其单测,也无法用同一套配置注入。
5. 采集与分析在 kits 的 `run_single_platform_pipeline.py` 里混成一条龙,无法单独验证。

### 约束
- 现有 11 个测试套件(`pipelines/tests/`)必须持续通过。
- 现有 `projects/*/` 四层 JSON 结构有完整 spec 和测试,结构需保持兼容。

## Decisions(已锁定)

| 编号 | 决策点 | 选定方案 |
|---|---|---|
| C | 整合深度 | **C. 深度融合**:吸收 kits 代码 + 把脚本拆成可测模块化组件 |
| C2 | git 方式 | **C2. 直接拷贝、丢弃 kits 历史**(commit message 记录溯源) |
| D3 | 包结构 | **D3. 单包内分层**:`perf_analysis` 作废,kits 代码落到 `pyframework_pipeline/analyze/`,`pyframework_pipeline` 是单一 Python 包,部署/采集/分析/消费是其内部逻辑分层 |
| E3 | kits 重构幅度 | **E3. 领域建模 + 管道抽象**:`Step` 链可组合/可插拔/可回放,分类器规则可热加载,渲染器注册表化 |
| F2 | pipeline 重构幅度 | **F2. adapter 契约化 + Step 注册表 + cli 拆分**;跨层 DTO 固化留后续阶段 |
| G2 | 验收基线 | 现有 11 套件 + 迁移 kits 的 9 套件全绿 |
| H1 | 落地阶段 | 三阶段:机械整合(锁基线)→ analyze 重构 → pyframework_pipeline 重构 |
| I2 | CLI 入口 | 单一入口脚本、子命令分派(`single`/`compare`),薄壳调包内 API |
| 分类规则归属 | CPython 14 类 + L2 sub-category 属 CPython 固有领域知识,固定不变,归 `analyze` 核心;不存在"框架特定 L1 映射" |
| 采集/分析分离 | 采集是必须跑框架的大流程,产出 `perf.data`;分析是框架无关、内部可拔插子流程链;两者只通过契约衔接 |
| 流程层级 | 大流程(部署/采集/分析)+ 子流程两级;渲染/回填/桥接归"消费"二级概念 |
| 指令链对称 | 采集加 B5 指令采集(perf annotate + objdump),C4 注解只接收两类输入(分析链内部产物 + B5 指令数据),C4 指令输入也走契约、可外部喂入 |
| 契约原则 | 契约 = 数据类 + 固定序列化形态(必选)+ 子流程接口(输入输出均为可配置文件路径,带默认值);序列化落盘必选以保证"有证可查",性能非主要矛盾 |
| 2.4 | C2 输出类型 | 方案甲:`PerfRecord` 自带 category 字段(为空=未分类),C1/C2 共享同一套文件列结构 |
| 3.5 | adapter 策略 | 方案甲:adapter 返回描述对象(`PerfAttachSpec` 等),采集执行逻辑在 `acquire/` 写一次 |
| 4.6 | Step 依赖 | 方案甲:`requires`/`produces` 契约名做拓扑排序 |
| 5.7 | backfill 产物 | 方案甲:backfill 输出路径可配置,默认指向 `projects/<id>/`,保持与现有项目结构兼容 |
| 杂项1 | cli 拆分 | 按子命令拆(`cli/config.py`、`cli/environment.py` 等) |
| 杂项2 | 命名 | 去掉 `perf_insights` 痕迹,统一 `analyze/` |
| 杂项3 | 阶段一调用切换 | subprocess→import 一步到位 |
| 杂项4 | contracts 收编 | 现有散落数据结构(`AcquisitionManifest`、裸 dict 等)本次一并收编进 `contracts/` |
| 杂项5 | Step 注册 | `@register_step` 装饰器 |

## Architecture(目标架构)

### 单包内逻辑分层

`pyframework_pipeline` 是单一 Python 包;部署/采集/分析/消费是其内部逻辑分层,共享同一命名空间(`PYTHONPATH=pipelines`)。层与层之间**只通过 `contracts/` 契约通信,不互相 import 实现细节**——这是逻辑边界,不是物理隔离。

```
pipelines/pyframework_pipeline/
├── __init__.py
├── contracts/          分层共享的输入输出契约(数据类)+ 序列化
├── deploy/             大流程 A:部署
├── acquire/            大流程 B:采集(框架无关采集骨架)
├── analyze/            大流程 C:分析(源自 kits,框架无关,可拔插契约链)
├── consume/            下游消费:渲染 / 回填 / 桥接
├── adapters/           框架差异点(pyflink/datajuicer/udfbenchmarking)
├── orchestrator.py     L0 编排:驱动大流程序列 + 状态机(薄壳)
├── cli/                单入口 CLI,按子命令拆分
└── registry.py         Step / 渲染器 / adapter 的注册表
```

### 流程 / 子流程两级

```
大流程 A: 部署 (Deploy)  —— 把"能跑"的环境从无到有准备出来
   A1. 环境部署      起容器/装解释器/装依赖,产出可用的运行环境
   A2. 工作负载部署  把被测的框架代码和用例放进环境(UDF/配方/数据)
   A3. 采集器就位    perf 工具/perf wrapper 注入,确保能采到

大流程 B: 采集 (Acquire)  —— 必须运行框架,产出各类原始测量数据
   B1. 基准执行(驱动)                       跑 benchmark,驱动框架执行被测路径
   B2. 事件采集   → perf.data                perf record 在执行期间采原始 perf.data
   B3. 计时采集   → timing 数据               wall-clock / throughput / operator 计时
   B4. 火焰图采集(可选) → 火焰图数据         Python 采样
   B5. 指令采集   → 指令级热点 + 机器码反汇编  perf annotate + objdump
   (B2/B3/B4/B5 由 B1 驱动,彼此独立/可选)

大流程 C: 分析 (Analyze)  —— 框架无关,可拔插契约链,每段只认输入契约
   C1. 解析     perf.data → 归一化记录
   C2. 分类     归一化记录 → 带类别记录(CPython 14类,固定规则)
   C3. 聚合     带类别记录 → 汇总表(符号表/类别表/对象表)
   C4. 注解     [带类别记录+汇总表] + [B5指令数据] → 关联标注
   (分析到 C4 为止,产出最终分析产物)

下游: 消费 (Consume)  —— 吃分析产物,产出展示物,不产生新分析结论
   • 渲染 (render)    分析产物 → 报告(平台报告/对比报告,渲染器注册表)
   • 回填 (backfill)  分析产物 + 四层 JSON 模板 → 四层 JSON(filled)
   • 桥接 (bridge)    四层 JSON(filled) → Issue/Discussion
```

### 对称性与可拔插性

- **采集 ↔ 分析通过 B2↔C1 的契约衔接**:采集产 `perf.data`,分析从 `perf.data` 起步(或跳过采集直接喂)。
- **指令链对称**:B5 采指令数据 ↔ C4 消费指令数据。C4 不采集,只关联;其指令输入也走契约,可由外部喂入。
- **消费层是纯下游**:渲染/回填/桥接只变换展示形态,都吃 C4 的最终分析产物(或其聚合中间态)。
- **可单独跑/可跳过**:每个子流程输入输出都是可配置文件路径(带默认值),任何人把任意来源的契约文件放到对应路径,子流程就能跑;从中间插入任意一步都行。

## Detailed Design

### 1. contracts/(契约层)

**契约原则**:契约 = 数据类(逻辑形态)+ 固定序列化形态(磁盘文件,必选)+ 子流程接口(输入输出均为可配置文件路径,带默认值)。

序列化落盘是**必选**,以保证"有证可查"——每一步的输入产物和输出产物都落到磁盘,可检查、可单独重放、可跳过/续跑。序列化/反序列化开销对本项目不重要(正确性、可定位性 >> 性能)。

#### 四个数据族

```python
# pyframework_pipeline/contracts/records.py
@dataclass(frozen=True)
class RawSample:                 # C1 解析的输入/perf.data 的内存表示
    ip: int
    pid: int
    tid: int
    period: int
    timestamp: int
    command: str
    shared_object: str
    symbol: str | None
    callchain: tuple[int, ...]

@dataclass(frozen=True)
class PerfRecord(RawSample):     # C2 分类后的记录(category 为空 = 未分类)
    category_top: str            # CPython 14 类之一
    category_sub: str            # L2 sub-category
    category_reason: str         # 匹配到的规则名
```

> **2.4 决策**:`PerfRecord` 自带 category 字段(为空=未分类),C1/C2 共享同一套文件列结构(`classified_records.csv` 只是多了 3 列 category 字段)。

```python
# pyframework_pipeline/contracts/tables.py
@dataclass(frozen=True)
class CategoryRow:               # 类别汇总表的一行
    category_top: str
    category_sub: str
    sample_count: int
    period_total: int
    period_share: float          # self 占比
    children_share: float

@dataclass(frozen=True)
class SymbolRow:                 # 符号热点表的一行
    symbol: str
    shared_object: str
    category_top: str
    sample_count: int
    period_total: int
    period_share: float

@dataclass(frozen=True)
class AggregatedTables:          # C3 的输出契约(多张表打包)
    by_category: tuple[CategoryRow, ...]
    by_symbol: tuple[SymbolRow, ...]
    by_shared_object: tuple[SharedObjectRow, ...]
    total_period: int            # 用于占比换算的基准
```

```python
# pyframework_pipeline/contracts/instruction.py
@dataclass(frozen=True)
class InstructionSample:         # perf annotate 的一条指令热点
    ip: int
    instruction_text: str
    instruction_offset: int
    period: int
    period_share: float

@dataclass(frozen=True)
class DisassemblyBlock:          # objdump 产出的一段机器码
    shared_object: str
    symbol: str
    start_ip: int
    instructions: tuple[tuple[int, str], ...]   # (offset, asm_text)

@dataclass(frozen=True)
class InstructionDataset:        # C4 的指令输入契约(可由 B5 或外部喂入)
    samples: tuple[InstructionSample, ...]
    disassembly: tuple[DisassemblyBlock, ...]
```

```python
# pyframework_pipeline/contracts/timing.py
@dataclass(frozen=True)
class TimingEntry:               # 一项计时观测
    label: str                   # wall_clock / throughput / operator_name
    value_ns: int                # 统一纳秒
    unit_hint: str               # 原始单位(s/ms/ops 等)
    query_id: str | None

@dataclass(frozen=True)
class TimingDataset:             # B3 的输出契约
    entries: tuple[TimingEntry, ...]
    platform_id: str
    benchmark: str
```

#### 现有散落数据结构一并收编

`AcquisitionManifest`、orchestrator 里到处传的裸 dict、`AcquisitionSection` 等,**本次一并收编**进 `contracts/`,统一为契约数据类。不再留临时裸 dict 做跨层传递。

#### 实现要求

- 所有契约用 `@dataclass(frozen=True)`:不可变、值相等、可哈希、可直接构造做断言。
- 每个契约定义其磁盘文件形态(文件名约定 + 列结构/JSON 结构)和 `to_file()`/`from_file()`(或 `to_rows()`/`from_rows()`)读写方法。
- 子流程接口统一形如 `f(input_path=Path("..."), output_path=Path("..."), ...) -> None`,读磁盘、写磁盘,路径可配,默认值保证"不配置时也能跑"。

### 2. adapters/(框架差异点)

#### 注入方式:adapter 作为"采集子流程的策略提供者"

`acquire/` 的子流程实现为**接受 adapter 策略的通用骨架**;`FrameworkAdapter` 是各框架实现这些策略的接口。子流程不 import 任何框架,只依赖 adapter 协议。

```python
# contracts/adapter.py
class FrameworkAdapter(Protocol):
    framework_id: str

    # A2 工作负载部署
    def deploy_workload(self, env_dir: Path, *, workload_output: Path) -> WorkloadHandle: ...
    # B1 基准执行
    def run_benchmark(self, handle: WorkloadHandle, *, timing_output: Path) -> None: ...
    # B2 perf 挂载策略
    def perf_attach_strategy(self, handle: WorkloadHandle) -> PerfAttachSpec: ...
    # B3 计时产物归一化
    def normalize_timing(self, raw_source: Path, *, output: Path) -> None: ...
    # B4 火焰图采集(None 表示该框架不支持)
    def collect_flamegraph(self, handle: WorkloadHandle, *, output: Path) -> None | None: ...
    # B5 指令采集的框架特定部分
    def disassembly_source(self, handle: WorkloadHandle) -> DisassemblySpec: ...
```

#### 3.5 决策:adapter 返回描述对象,不直接执行

adapter 方法返回**描述对象**(`PerfAttachSpec` 含目标 PID/容器 ID/perf 路径等),采集骨架拿描述对象去执行。采集执行逻辑(调 subprocess、处理输出)全在 `acquire/`,adapter 只描述"做什么"。

```python
# acquire/benchmark.py —— 通用骨架,不认框架
def acquire_benchmark(
    adapter: FrameworkAdapter,
    *,
    timing_output: Path = Path("timing_normalized.json"),
    perf_data_output: Path = Path("perf.data"),
    instruction_output: Path = Path("instruction_hotspots.csv"),
    flamegraph_output: Path | None = None,
) -> None:
    handle = adapter.deploy_workload(...)
    adapter.run_benchmark(handle, timing_output=timing_output)
    perf_attach = adapter.perf_attach_strategy(handle)
    record_perf(perf_attach, output=perf_data_output)           # 通用 perf record 调用
    disasm_src = adapter.disassembly_source(handle)
    record_instruction(disasm_src, output=instruction_output)  # 通用 objdump 调用
    if flamegraph_output:
        adapter.collect_flamegraph(handle, output=flamegraph_output)
```

理由:采集核心可测(喂假 adapter)、加新框架成本低(只实现策略方法)、采集逻辑单一来源(不会出现"pyflink 的采集"和"datajuicer 的采集"两套 perf record 实现)。

#### 实现位置

```
adapters/
├── base.py            FrameworkAdapter 协议(或抽象基类)
├── registry.py        按 framework_id 查找 adapter
├── pyflink/adapter.py     PyFlinkAdapter(实现 7 策略)
├── datajuicer/adapter.py  DataJuicerAdapter
└── udfbenchmarking/adapter.py  UdfBenchmarkingAdapter
```

orchestrator 通过 `registry.get(framework_id)` 拿到 adapter,把 adapter 传给采集子流程。**orchestrator 不认框架,只认 adapter 接口**。

### 3. orchestrator.py(L0 编排薄壳)

#### 职责收窄到三件事

1. **驱动大流程序列**:部署 → 采集 → 分析 → 消费,按配置决定跑哪些、跑什么顺序。
2. **状态机管理**:复用现有 `PipelineRunState`,记录每步 done/failed/running,支持 resume。
3. **装配**:从 registry 拿 adapter,把 adapter + 路径传给子流程。

**不做**(全部移出):框架特化逻辑(进 adapter)、子流程实现(进 deploy/acquire/analyze/consume)、step 内部细节。

#### Step 注册表:每个子流程注册成可调度单元

```python
# contracts/step.py
class Step(Protocol):
    name: str                              # 如 "acquire.benchmark"
    requires: tuple[str, ...]              # 依赖的前置 step 的 produces 契约名
    produces: tuple[str, ...]              # 产出的契约名

    def run(self, ctx: RunContext) -> None: ...

# RunContext 携带:adapter、run_dir、已配置的路径映射、RunState
```

```python
# orchestrator 驱动逻辑
def run_pipeline(config):
    adapter = registry.get(config.framework_id)
    ctx = RunContext(adapter=adapter, run_dir=config.run_dir, state=RunState(...))
    for step in resolve_step_plan(config.steps, registry):   # 拓扑排序 + 依赖检查
        if ctx.state.is_completed(step.name):
            continue
        ctx.state.mark_running(step.name)
        try:
            step.run(ctx)
            ctx.state.mark_completed(step.name)
        except Exception as e:
            ctx.state.mark_failed(step.name, e)
            raise StepError(step.name, e)
```

#### 4.6 决策:requires/produces 契约名做拓扑排序

`requires=("analyze.classify",)` / `produces=("aggregated_tables",)`,orchestrator 根据契约依赖做拓扑排序自动决定顺序。加 step 不用改顺序定义,依赖自描述。

#### 注册方式:`@register_step` 装饰器就近注册

```python
# analyze/aggregate.py
@register_step
class AggregateStep:
    name = "analyze.aggregate"
    requires = ("analyze.classify",)
    produces = ("aggregated_tables",)
    def run(self, ctx):
        aggregate(
            input_path=ctx.path("classified_records.csv"),       # 可配置路径
            output_path=ctx.path("category_summary.csv"),         # 默认值由 ctx 提供
        )
```

orchestrator 启动时 import 各层包触发注册,之后只认 registry。

#### 现有 `_run_*` 函数归位

| 现有函数(orchestrator) | 归位 |
|---|---|
| `_run_workload_deploy` / `_run_datajuicer_workload_deploy` / `_run_udfbenchmarking_workload_deploy` | adapter.`deploy_workload` + `deploy/` 骨架 |
| `_run_benchmark` / `_run_datajuicer_benchmark` / `_run_udfbenchmarking_benchmark` | adapter.`run_benchmark` + `acquire/` 骨架 |
| `_run_*_python_flamegraph` | adapter.`collect_flamegraph` + `acquire/flamegraph.py` |
| `_ensure_container_perf` / `_deploy_perf_wrapper` / `_ensure_jar` / `_ensure_pyflink_runner` | adapter 策略方法 / `deploy/` |
| `_run_perf_kits_on_remote` / `_run_compare` / `_parse_benchmark_summary` | `analyze/` + `consume/render/` |
| `_write_udfbenchmarking_timing_artifacts` / `_numeric` / `_seconds_to_ns` | adapter.`normalize_timing` + `contracts/timing.py` |
| `_run_acquire_all` / `_run_backfill` / `_run_compare` / `_run_bridge_publish` | orchestrator 的 step 调度(每段变成 1-2 行) |
| `PipelineRunState` | 保留在 orchestrator(状态机) |
| `_resolve_step_alias` / `_print_resume_hint` | 保留在 orchestrator |

### 4. analyze/(分析层,源自 kits)

#### E3 领域建模 + 管道抽象

把 kits 的"一条龙"拆成可拔插子流程链,每段一个清晰的类,输入输出是显式契约:

```
analyze/
├── parse.py        C1 解析:perf.data → PerfRecord(未分类)
├── classify.py     C2 分类:PerfRecord → PerfRecord(带 CPython 14 类)
├── aggregate.py    C3 聚合:PerfRecord → AggregatedTables
├── annotate.py     C4 注解:[PerfRecord + AggregatedTables] + InstructionDataset → 最终产物
└── rules/
    └── cpython_category_rules.json   CPython 分类规则(领域固有知识,内置资源)
```

- **C2 分类规则**:CPython 14 类 + L2 sub-category 属 CPython 固有领域知识,固定不变,归 `analyze` 核心。规则文件随包内置(`importlib.resources`),`CategoryClassifier` 默认加载,也支持构造时传入自定义规则路径。**不存在框架特定的 L1 映射**——本仓 `acquisition/perf_profile.py` 和 `backfill/perf_backfill.py` 里那两份重复的 `CATEGORY_MAP`,重构时收敛成一份。
- **CLI 入口**:单一入口、子命令分派(`single`/`compare`),薄壳调包内 API,保留"脚本可独立运行"的约定,逻辑单一来源。

### 5. consume/(消费层)

#### 三个子流程(纯下游,吃 C4 最终分析产物)

| 子流程 | 输入(可配置路径) | 输出(可配置路径) | 职责 |
|---|---|---|---|
| **render** | annotated_hotspots.csv + category_summary.csv + symbol_hotspots.csv + timing_normalized.json | report.md / report.html | 渲染成人类可读报告,渲染器注册表选择 |
| **backfill** | 同上 + 四层 JSON 模板 | 四层 JSON(filled) | 把分析产物回填进 Framework/Dataset/Source/Project 四层 JSON |
| **bridge** | 四层 JSON(filled) | Issue body / Discussion | 发布到 GitHub Issue/Discussion |

线性依赖:render 与 backfill 可并行(都直接吃分析产物),bridge 必须在 backfill 之后。

#### 渲染器注册表 + 策略模式

```python
# consume/render/
@register_renderer("platform")
class PlatformReportRenderer:
    def render(self, *, tables_path: Path, output_path: Path) -> None: ...

@register_renderer("compare")
class CompareReportRenderer:
    def render(self, *, tables_path: Path, output_path: Path) -> None: ...
```

调用方指定渲染器名(`render --renderer platform`),新增报告形态只注册一个渲染器,不动 render 骨架。这和 adapter 的策略注入、Step 的注册表一脉相承——全包统一的扩展机制。把 kits 里散落的 `render_platform_*` / `render_compare_*` 收敛成注册式渲染器。

#### backfill 拆分(1265 行 → 聚焦模块)

```
consume/backfill/
├── category_mapping.py    14类 → 四层 JSON 字段的映射逻辑
├── hotspot_backfill.py    符号热点/指令热点的回填
├── instruction_backfill.py 指令级数据回填进 Source 层
├── binding.py             分析产物 ↔ 四层 JSON 实体的绑定
└── step.py                BackfillStep(注册表入口)
```

#### 5.7 决策:backfill 输出路径可配置,默认指向 `projects/<id>/`

backfill 的输出路径**可配置**,默认指向 `projects/<id>/` 对应位置,保持与现有项目结构兼容;四层 JSON 的实体类(Framework/Dataset/Source/Project)定义为 `contracts/` 里的契约数据类,backfill 负责把分析产物映射进这些契约对象并序列化到可配置路径。既有项目目录结构不变,迁移零摩擦。

### 6. cli/(单入口,按子命令拆分)

```
cli/
├── __init__.py       单入口 main(),子命令分派
├── config.py         config 子命令
├── environment.py    environment 子命令
├── benchmark.py      benchmark 子命令
├── acquire.py        acquire 子命令(单独跑某采集子流程)
├── analyze.py        analyze 子命令(单独跑某分析子流程,如 analyze --step classify)
└── ...
```

支持按 step 名单独执行(配合可配置路径),如 `python -m pyframework_pipeline analyze --step aggregate --input classified_records.csv`。

## Phasing(H1 三阶段落地)

E3 + F2 + C2 是大变更,切成三个**可独立验证**的阶段,每个阶段结束时测试全绿、可单独 commit/PR、可回退。阶段一把"机械搬移(低风险)"和"行为重构(高风险)"严格分开。

### 阶段一:机械整合(锁基线)

**目标**:把 kits 代码原样搬进 `pyframework_pipeline/analyze/`(扁平整理,改 import 路径),迁移 kits 的 9 套测试(改 import 路径),本仓改用 import 调用替代 subprocess,删 submodule。**此时所有测试绿,功能等价,算法零改动。**

- 拷贝 kits 17 个脚本到 `analyze/`,去掉 `perf_insights` 命名痕迹,统一 `analyze/`。
- 改 import 路径(kits 内部脚本互相 import 的相对路径调整为包内 import)。
- **subprocess→import 一步到位**:6 个耦合点改用包内 import 调用,不再 subprocess。
- 迁移 kits 的 9 套测试到 `pipelines/tests/`,改 import 路径。
- 删除 `vendor/python-performance-kits` submodule 和 `.gitmodules` 条目。
- commit message 记录溯源("代码源自 vendor/python-performance-kits @ <hash>")。
- 入口脚本收敛为单一薄壳、子命令分派(此时 `single`/`compare` 子命令内部直接调包内函数)。

**验收**:现有 11 套件 + 迁移的 9 套件全绿(G2)。

### 阶段二:analyze 层 E3 领域建模

**目标**:在 `analyze/` 内做 E3 领域建模 + 管道抽象,算法不变。

- 落地 `contracts/` 契约数据族(records/tables/instruction/timing)+ 序列化方法。
- 把 `analyze/` 的扁平脚本重构为 C1/C2/C3/C4 四个有清晰职责的类,输入输出走契约。
- `CategoryClassifier` 规则可热加载,收敛两份重复 `CATEGORY_MAP` 为一份内置规则。
- 子流程接口统一为可配置文件路径(带默认值)。

**验收**:G2 全绿;算法等价性由迁移的 9 套测试守门。

### 阶段三:pyframework_pipeline 的 F2 重构

**目标**:adapter 契约化 + Step 注册表 + cli 拆分。

- `FrameworkAdapter` 扩展为覆盖采集子流程全部策略的协议(见 §2,共 6 个策略方法:`deploy_workload` / `run_benchmark` / `perf_attach_strategy` / `normalize_timing` / `collect_flamegraph` / `disassembly_source`),三框架各自实现 adapter。
- orchestrator 里散落的 `_run_*` 按"现有函数归位表"移入 adapter / deploy / acquire / consume。
- orchestrator 缩成 L0 编排薄壳(驱动 step 序列 + 状态机 + 装配)。
- Step 注册表(`@register_step`)+ requires/produces 拓扑排序。
- `cli.py` 1005 行按子命令拆分。
- `consume/`(渲染/回填/桥接)按 5.x 节落地;backfill 1265 行拆成聚焦模块。
- 现有散落数据结构(`AcquisitionManifest`、裸 dict 等)收编进 `contracts/`。

**验收**:G2 全绿。

## Testing Strategy

- **验收基线(G2)**:现有 11 套件 + 迁移 kits 的 9 套件,每个阶段结束全绿。
- **新增单测**:阶段二为每个契约数据族和 C1-C4 类补单元测试(契约可直接构造、可直接断言)。
- **阶段一守门**:kits 算法等价性由迁移的 9 套测试保证(分类结果与原 kits 一致)。
- **阶段三守门**:adapter 可用假实现单测采集骨架(不依赖真实框架);Step 可单独跑。

## Non-Goals(本次不做)

- 跨层 DTO 固化(F3):本次只收编与新契约直接相关的数据结构,跨层全量 DTO 固化留后续。
- 端到端黄金样本回归(G3):留作后续验证手段。
- 引入 `pyproject.toml`/`console_scripts` 打包机制:与"零第三方依赖、PYTHONPATH 直跑"现有约定冲突。
- kits 上游仓库的双向同步:本仓拥有并演进这套代码,上游只作历史来源。
- 性能优化(频繁序列化/反序列化的开销)。

## Open Items(无)

所有关键决策已在 Decisions 表锁定。无遗留待定项。
