# 报告数据 Schema 规范 v0.1

## 1. 目标

本规范用于定义交互式报告的数据包结构、核心实体、实体关系与加载约束。

设计目标：

- 页面主体与数据包分离
- 支持公开示例包与内网真实数据包替换
- 支持从摘要页一路下钻到函数、模式、根因与附件
- 支持在总览页展示两个平台性能差异的全景分布，而不是只展示摘要结论
- 支持围绕“组件/分类分布 -> 全量热点函数 -> 单函数机器码 diff”这条主链组织数据

## 2. 数据包总结构

建议的数据包目录如下：

```text
report-package/
  report_manifest.json
  summary/
  details/
  artifacts/
```

各目录职责：

- `report_manifest.json`
  - 报告元信息
  - schema 版本
  - 平台定义
  - 数据集信息
- `summary/`
  - 首页与总览页直接消费的数据
- `details/`
  - 详情页直接消费的数据
- `artifacts/`
  - 机器码、源码、SQL、Python UDF、日志等附件

## 3. 加载模型

前端采用三层加载模型：

### 3.1 summary

用于以下页面：

- 首页
- 范围页
- case 总览
- stack 总览
- insights 总览

特点：

- 字段稳定
- 体积小
- 适合首次加载

### 3.2 details

用于以下页面：

- case 详情
- component 详情
- category 详情
- function 详情
- pattern 详情
- root cause 详情
- artifact 元数据页

特点：

- 面向 drill-down
- 结构化程度高
- 可直接渲染详情页

### 3.3 artifacts

用于以下内容：

- 汇编文本
- 源码片段
- SQL 原文
- Python UDF 示例
- profiling 输出
- 日志与原始证据

特点：

- 大文件
- 文本或二进制附件
- 通过 `artifactId` 间接引用

## 4. 核心实体

建议的核心实体如下：

- `Case`
- `ExecutiveSummary`
- `StackOverview`
- `ComponentDetail`
- `CategoryDetail`
- `FunctionDetail`
- `FunctionDiffView`
- `PatternDetail`
- `RootCauseDetail`
- `Opportunity`
- `ArtifactDetail`

## 5. 实体说明

### 5.1 `Case`

表示一个 workload。

字段建议：

- `id`
- `name`
- `semanticNotes`
- `knownDeviations`
- `artifactIds`
- `metrics`
- `hotspots`
- `patterns`
- `rootCauses`

### 5.2 `ExecutiveSummary`

用于首页摘要。

字段建议：

- `title`
- `subtitle`
- `metrics`
- `topPattern`
- `topRootCause`

### 5.3 `StackOverview`

用于 stack 总览。

字段建议：

- `components`
- `categories`

并要求满足以下结构能力：

- 能直接驱动组件维度双平台堆叠柱状图
- 能直接驱动分类维度双平台堆叠柱状图
- 能直接驱动组件全量明细表
- 能直接驱动分类全量明细表

建议补充字段：

- `platformTotals`
- `componentLegend`
- `categoryLegend`

其中 `components` 中每一项至少应包含：

- `id`
- `name`
- `armTime`
- `x86Time`
- `armShare`
- `x86Share`
- `delta`
- `deltaContribution`
- `artifactIds` 或 drill-down 标识

其中 `categories` 中每一项至少应包含：

- `id`
- `name`
- `level`
- `armTime`
- `x86Time`
- `armShare`
- `x86Share`
- `delta`
- `deltaContribution`
- `topFunction`
- `topFunctionId`
- `artifactIds` 或 drill-down 标识

### 5.4 `ComponentDetail`

用于组件级详情。

字段建议：

- `id`
- `name`
- `armTime`
- `x86Time`
- `armShare`
- `x86Share`
- `delta`
- `deltaContribution`
- `hotspots`
- `patternIds`
- `rootCauseIds`
- `artifactIds`

其中 `hotspots` 必须是全量热点函数列表，每一项至少包含：

- `id`
- `symbol`
- `category`
- `selfArm`
- `selfX86`
- `totalArm`
- `totalX86`
- `armShare`
- `x86Share`
- `delta`
- `deltaContribution`

### 5.5 `CategoryDetail`

用于分类级详情。

字段建议：

- `id`
- `name`
- `level`
- `componentIds`
- `caseIds`
- `armTime`
- `x86Time`
- `armShare`
- `x86Share`
- `delta`
- `deltaContribution`
- `hotspots`
- `patternIds`
- `artifactIds`

其中 `hotspots` 必须是全量热点函数列表，每一项至少包含：

- `id`
- `symbol`
- `component`
- `selfArm`
- `selfX86`
- `totalArm`
- `totalX86`
- `armShare`
- `x86Share`
- `delta`
- `deltaContribution`

### 5.4 `FunctionDetail`

用于函数级热点详情。

字段建议：

- `id`
- `symbol`
- `component`
- `categoryL1`
- `categoryL2`
- `caseIds`
- `artifactIds`
- `metrics`
- `callPath`
- `patternIds`
- `diffView`

其中 `diffView` 必须能直接支撑源码视角的机器码差异界面。

### 5.5 `FunctionDiffView`

用于函数级机器码差异展示。

字段建议：

- `functionId`
- `sourceFile`
- `sourceLocation`
- `diffGuide`
- `analysisBlocks`

其中 `diffGuide` 用于明确说明：

- 当前 diff 视图按逻辑分析块对齐，而不是按源码行顺序对齐
- 同一源码语义可能映射到多个离散机器码区块
- 平台之间可能存在重排、拆分、消除和合并

其中 `analysisBlocks` 中每一项至少包含：

- `id`
- `label`
- `summary`
- `patternTag`
- `mappingType`
- `sourceAnchors`
- `armRegions`
- `x86Regions`
- `diffSignals`
- `alignmentNote`
- `performanceNote`
- `defaultExpanded`

其中 `sourceAnchors` 中每一项至少包含：

- `id`
- `label`
- `role`
- `location`
- `snippet`
- `defaultExpanded`

其中 `armRegions` / `x86Regions` 中每一项至少包含：

- `id`
- `label`
- `location`
- `role`
- `snippet`
- `highlights`
- `defaultExpanded`

说明：

- `patternTag` 用于承载分析块的行为模式标签，应优先使用稳定的 pattern family，而不是自由文本摘要
- 行为模式命名建议参考 `sisibeloved/cinderx#3` 中的模式族，例如：
  - `NumericLoop`
  - `BranchFSM`
  - `ObjectManipulator`
  - `CallDispatcher`
  - `AsyncStateMachine`
  - `ReflectionMeta`
  - `ImportInit`
- `mappingType` 继续保留，用于表达源码锚点与机器码区块的结构关系，例如一对多、多对多、重排、拆分；但它不应替代 `patternTag` 成为页面主标签

### 5.6 `PatternDetail`

用于模式详情。

字段建议：

- `id`
- `title`
- `summary`
- `confidence`
- `caseIds`
- `functionIds`
- `rootCauseIds`
- `artifactIds`

### 5.7 `RootCauseDetail`

用于根因详情。

字段建议：

- `id`
- `title`
- `summary`
- `confidence`
- `patternIds`
- `artifactIds`
- `optimizationIdeas`
- `validationPlan`

### 5.8 `Opportunity`

用于优化机会排序。

字段建议：

- `id`
- `title`
- `impact`
- `effort`
- `estimatedGainPct`
- `rootCauseId`

### 5.9 `ArtifactDetail`

用于附件元数据。

字段建议：

- `id`
- `title`
- `type`
- `description`
- `path`
- `contentType`

## 6. 关系约束

必须满足以下约束：

- 首页结论必须能追到具体 `Case`、`Pattern` 或 `RootCause`
- 每个 `Pattern` 必须能追到至少一个函数或附件
- 每个 `RootCause` 必须关联至少一个 `Pattern`
- 每个 `Opportunity` 应绑定一个 `RootCause`
- 每个 `ArtifactDetail` 必须能定位到实际文件路径
- 每个总览页必须能够渲染全量分布，不得依赖只包含 Top 项的 summary 数据
- 每个 `ComponentDetail` 和 `CategoryDetail` 必须包含全量热点函数列表
- 每个热点函数都必须能 drill-down 到一个 `FunctionDetail`
- 每个 `FunctionDetail` 都必须能渲染源码视角的机器码 diff

## 7. summary 文件建议

建议至少包含：

- `executive_summary.json`
- `case_index.json`
- `stack_overview.json`
- `pattern_index.json`
- `root_cause_index.json`
- `opportunity_ranking.json`
- `scope.json`

## 8. details 文件建议

建议至少包含：

- `details/cases/:id.json`
- `details/components/:id.json`
- `details/categories/:id.json`
- `details/functions/:id.json`
- `details/patterns/:id.json`
- `details/root_causes/:id.json`
- `details/artifacts/:id.json`

## 9. manifest 建议

```json
{
  "reportId": "sample-report",
  "title": "PyFlink 跨平台性能差异分析",
  "schemaVersion": "0.1.0"
}
```

## 10. 命名规则

- `id` 应保持稳定，便于页面路由与交叉引用
- 面向读者的文本使用可本地化字段承载
- 文件路径使用 ASCII 命名即可，但展示文本可以为中文

## 11. 页面与数据关系

建议如下：

- 首页 -> `summary/executive_summary.json`
- Scope -> `summary/scope.json`
- Cases -> `summary/case_index.json`
- By Case -> `summary/case_index.json`
- By Stack -> `summary/stack_overview.json`
- Insights -> `summary/opportunity_ranking.json` + `summary/pattern_index.json` + `summary/root_cause_index.json`
- 详情页 -> 对应 `details/*`
- Artifact 页 -> `details/artifacts/:id.json` + `artifacts/*`

## 12. 兼容性要求

- 页面不得依赖一次性脚本生成的临时字段名
- schema 升级时应优先通过新增字段兼容，而非重命名核心字段
- 前端应允许 summary 命中真实 JSON、details 命中 mock fallback 的混合模式

## 13. 公开示例包与内网包

公开示例包：

- 可展示结构
- 可使用脱敏样例
- 可省略真实证据内容

内网真实包：

- 使用同一 schema
- 仅替换 `summary/`、`details/`、`artifacts/`
- 不改页面代码

## 14. 最低可用校验

一份数据包至少应通过以下校验：

- `manifest` 存在
- 首页 summary 存在
- 至少存在一个 case
- 至少存在一个 pattern
- 至少存在一个 root cause
- 至少存在一个可打开的 artifact
