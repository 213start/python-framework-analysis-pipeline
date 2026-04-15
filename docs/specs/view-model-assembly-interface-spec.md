# View Model 组装接口规范 v0.1

## 1. 目标

本规范定义四层模型：

- `Framework`
- `Dataset`
- `Source`
- `Project`

如何组装成当前前端页面直接消费的 view model。

本规范的目标不是重新设计页面字段，而是回答一个更具体的问题：

> 在不推翻现有页面组件的前提下，四层对象最少需要经过哪些组装接口，才能恢复当前 PyFlink 参考实现的页面链路？

因此，本规范默认遵循两个原则：

- 页面组件尽量不直接读取四层原始对象
- 组装层优先输出与 `web/src/types/report.ts` 兼容的现有 view model

## 2. 组装层定位

组装层位于：

- 底层 source adapters 之上
- 页面组件之下

职责边界：

- source adapters
  - 负责读取 `Framework / Dataset / Source / Project`
- assemblers
  - 负责把四层对象装配成页面可读的 summary/detail view model
- pages
  - 只消费组装后的 view model，不关心底层目录结构

组装层不负责：

- 自动推断绑定关系
- 修补缺失的 `Project` 映射
- 修改源码资产
- 推导新的 pattern 或 root cause

## 3. 输入接口

建议先定义四个读取接口：

```ts
type FrameworkDef = unknown;
type DatasetDef = unknown;
type SourceDef = unknown;
type ProjectDef = unknown;

declare function loadFramework(frameworkId: string): Promise<FrameworkDef>;
declare function loadDataset(datasetId: string): Promise<DatasetDef>;
declare function loadSource(sourceId: string): Promise<SourceDef>;
declare function loadProject(projectId: string): Promise<ProjectDef>;
```

说明：

- 这些接口是当前前端的数据输入边界
- `Project` 是组装入口，页面不应直接跳过 `Project` 去读其他对象
- 四层输入缺失时应显式报错，不应静默生成替代数据

## 4. 组装上下文

建议所有 assembler 先接收统一上下文，再派生页面模型。

```ts
type AssemblyContext = {
  project: ProjectDef;
  framework: FrameworkDef;
  dataset: DatasetDef;
  source: SourceDef;
};

declare function loadAssemblyContext(projectId: string): Promise<AssemblyContext>;
```

`loadAssemblyContext` 的顺序应固定为：

1. 读取 `Project`
2. 根据 `frameworkRef` 读取 `Framework`
3. 根据 `datasetRef` 读取 `Dataset`
4. 根据 `sourceRef` 读取 `Source`

## 5. 输出原则

组装层输出遵循以下原则：

- 输出对象优先兼容当前 `web/src/types/report.ts`
- 主链页面必须保留全量分布、全量热点函数和函数级 diff 所需字段
- 所有跨对象关联都通过 `Project` 绑定表解析
- 若绑定缺失，应返回可诊断的空状态，而不是静默伪造数据

## 6. 顶层组装接口

建议至少提供以下组装接口：

```ts
declare function assembleExecutiveSummary(ctx: AssemblyContext): ExecutiveSummary;
declare function assembleScopeSummary(ctx: AssemblyContext): ScopeSummary;
declare function assembleCaseIndex(ctx: AssemblyContext): CaseIndexEntry[];
declare function assembleStackOverview(ctx: AssemblyContext): StackOverview;
declare function assembleOpportunityRanking(ctx: AssemblyContext): OpportunityRankingEntry[];
declare function assemblePatternIndex(ctx: AssemblyContext): PatternIndexEntry[];
declare function assembleRootCauseIndex(ctx: AssemblyContext): RootCauseIndexEntry[];
```

这些接口分别对应当前页面：

- 首页
- Scope
- Cases
- By Case
- By Stack
- Insights

## 7. 明细组装接口

主链页面建议至少提供以下接口：

```ts
declare function assembleCaseDetail(ctx: AssemblyContext, caseId: string): CaseDetail;
declare function assembleComponentDetail(ctx: AssemblyContext, componentId: string): ComponentDetail;
declare function assembleCategoryDetail(ctx: AssemblyContext, categoryId: string): CategoryDetail;
declare function assembleFunctionDetail(ctx: AssemblyContext, functionId: string): FunctionDetail;
declare function assemblePatternDetail(ctx: AssemblyContext, patternId: string): PatternDetail;
declare function assembleRootCauseDetail(ctx: AssemblyContext, rootCauseId: string): RootCauseDetail;
declare function assembleArtifactDetail(ctx: AssemblyContext, artifactId: string): ArtifactDetail;
```

这些接口应覆盖当前最核心的 drill-down 路径：

- `By Stack -> Component / Category -> Function -> Artifact`
- `Case -> Function -> Pattern -> Root Cause -> Artifact`

## 8. 现有类型映射

当前组装输出建议直接兼容：

- `ExecutiveSummary`
- `ScopeSummary`
- `CaseIndexEntry`
- `StackOverview`
- `CaseDetail`
- `ComponentDetail`
- `CategoryDetail`
- `FunctionDetail`
- `PatternDetail`
- `RootCauseDetail`
- `ArtifactDetail`

这些类型当前定义在：

- [report.ts](/opt/Codex/python-framework-analysis-pipeline/web/src/types/report.ts)

第一阶段不要急着改页面类型，先保证四层输入能被组装回现有输出。

## 9. 关键页面的组装规则

### 9.1 首页

`assembleExecutiveSummary(ctx)` 主要依赖：

- `Dataset` 中的总览指标
- `Dataset.patterns`
- `Dataset.rootCauses`

不应直接依赖：

- `Source`
- `Project` 的细粒度绑定

### 9.2 Scope

`assembleScopeSummary(ctx)` 主要依赖：

- `Framework.analysisScope`
- `Framework.excludedScope`
- `Framework.metricDefinitions`
- `Framework.taxonomy`

这是最接近纯 `Framework` 的页面。

### 9.3 Cases

`assembleCaseIndex(ctx)` 主要依赖：

- `Dataset.cases`
- `Project.caseBindings`

这里的关键任务是把：

- case 结果
- SQL 资产
- Python UDF 资产
- 源码绑定入口

装配成当前用例资产页所需字段。

### 9.4 By Stack

`assembleStackOverview(ctx)` 主要依赖：

- `Dataset.stackOverview`

By Stack 页的全景图和全量明细表，不应依赖 `Source`；只有在往下钻到函数时，才进入 `Project` 与 `Source`。

### 9.5 Component / Category

`assembleComponentDetail(ctx, componentId)` 与 `assembleCategoryDetail(ctx, categoryId)` 主要依赖：

- `Dataset` 中的组件/分类统计
- `Dataset.functions`
- `Project.functionBindings`

这里的关键不是“Top N 热点函数”，而是：

- 返回全量热点函数列表
- 每个函数都带稳定 `functionId`
- 后续可继续下钻到 `FunctionDetail`

### 9.6 Function

`assembleFunctionDetail(ctx, functionId)` 是四层组装的核心接口。

它至少要合并：

- `Dataset.functions` 中的函数结果
- `Project.functionBindings` 中的源码锚点与平台 artifact 绑定
- `Source.sourceAnchors`
- `Source.artifactIndex`

它最终必须输出：

- 函数指标
- 调用路径
- pattern 关联
- `diffView`

其中 `diffView` 是最关键的组装结果，因为它需要同时跨：

- `Dataset`
- `Project`
- `Source`

### 9.7 Pattern / Root Cause

这两类接口主要依赖：

- `Dataset.patterns`
- `Dataset.rootCauses`
- `Project.patternBindings`
- `Project.rootCauseBindings`

作用是把“结果层对象”与“证据层对象”重新接回页面。

### 9.8 Artifact

`assembleArtifactDetail(ctx, artifactId)` 主要依赖：

- `Source.artifactIndex`

当前 artifact 页面虽然仍保留，但它不再是主 diff 视图，只是原始证据页。

## 10. FunctionDiffView 的组装约束

`assembleFunctionDetail` 内部最难的是 `diffView`，需要单独列出约束。

### 10.1 结构来源

`FunctionDiffView` 组装时应至少汇总：

- `Dataset.functions[*]`
- `Project.functionBindings[*]`
- `Source.sourceAnchors[*]`
- `Source.artifactIndex[*]`

### 10.2 页面层级

当前函数 diff 页的推荐层级是：

1. 分析块
2. 分析块描述
3. 源码锚点列表
4. 某个源码锚点展开
5. Arm 机器码区块
6. x86 机器码区块

因此 `diffView` 的组装重点，不是简单返回一堆汇编文本，而是要恢复这个层级。

### 10.3 不做自动推断

第一阶段不要求 `assembler` 自己推导：

- 哪个源码锚点应该归到哪个函数
- 哪个汇编区块应该归到哪个平台
- 哪个 pattern 应该落到哪个分析块

这些都必须依赖：

- `Project.functionBindings`
- `Project.patternBindings`
- `Source` 中已有的显式索引

## 11. 错误与空状态

组装层应显式暴露以下错误，而不是静默回退：

- `missing_project_binding`
- `missing_source_anchor`
- `missing_artifact_reference`
- `dataset_entity_not_found`
- `source_entity_not_found`

页面可以决定如何展示这些错误，但 assembler 不应默默伪造一份“看起来完整”的数据。

## 12. 与当前 PyFlink 参考实现的关系

当前仓库中的 PyFlink 参考实现已经切到四层输入：

- `web/public/examples/four-layer/pyflink-reference/`

组装层必须做到：

> 从四层对象重新组装出一份与当前页面输出兼容的 view model。

当前已经有一组对应的四层示例：

- [examples/four-layer/pyflink-reference](/opt/Codex/python-framework-analysis-pipeline/examples/four-layer/pyflink-reference)

这组示例应作为后续 assembler 骨架实现的第一份输入样本。

## 13. 建议的实施顺序

建议按以下顺序推进，而不是直接大改前端：

1. 定义 source adapters 接口
2. 定义 assembler 接口
3. 用 `examples/four-layer/pyflink-reference` 验证 `assembleCaseIndex` 与 `assembleStackOverview`
4. 再验证 `assembleComponentDetail / assembleCategoryDetail`
5. 最后验证 `assembleFunctionDetail`

这样可以优先守住主链：

- 全景分布
- 全量热点函数
- 单函数 diff

## 14. 结论

四层模型接入前端的关键，不是先改页面，而是先建立一层稳定的 view model 组装接口。

一句话概括：

> 页面继续消费当前 `report.ts` 风格的输出；组装层负责把 `Framework + Dataset + Source + Project` 重新拼成这些输出。
