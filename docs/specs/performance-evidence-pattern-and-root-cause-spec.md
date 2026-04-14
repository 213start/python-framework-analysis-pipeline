# 性能证据、模式与根因规范 v0.1

## 1. 目标

本规范用于定义热点函数、机器码片段、源码片段、模式与根因之间的关系，确保报告中的结论可以被证据链支撑。

本规范强调两个要求：

- `Pattern` 必须是对外可见的分析层
- `RootCause` 不应直接绑定单个函数或单段源码，而应通过 `Pattern` 聚合多处证据

此外，报告的整体表达必须遵循“性能差异全景优先”的原则：

- 证据链用于解释差异
- 但总览页首先要展示两个平台在完整分布上的差异全景
- 根因和模式不应取代全景表达，只能建立在全景表达之上

## 2. 证据链结构

证据链采用以下结构：

- `FunctionHotspot`
- `AsmSnippet`
- `SourceSnippet`
- `Pattern`
- `RootCause`

关系方向为：

- 函数、机器码、源码片段指向 `Pattern`
- 一个或多个 `Pattern` 再指向 `RootCause`

## 3. 各对象职责

### 3.1 `FunctionHotspot`

职责：

- 表示某个平台或双平台对齐后的热点函数现象
- 承载函数级绝对耗时、差值、调用路径等信息

### 3.2 `AsmSnippet`

职责：

- 表示某个热点函数中的代表性机器码片段
- 用于展示 Arm 与 x86 的对齐证据

### 3.3 `SourceSnippet`

职责：

- 表示与热点相关的源码位置与实现上下文
- 解释机器码背后的实现逻辑

### 3.4 `Pattern`

职责：

- 表示跨函数、跨片段、跨源码位置重复出现的共性性能模式
- 是工程分析层的主要收敛对象

示例：

- 高频临时对象创建
- 重复字典查找
- 边界 marshalling 过重
- 小对象分配路径压力偏高
- 解释器分派被多层包装放大

### 3.5 `RootCause`

职责：

- 表示一个或多个 pattern 背后的更高层原因判断
- 为优化建议和验证计划提供落点

示例：

- 当前数据表示方式导致对象 churn 过高
- UDF 边界设计引入重复包装与拆包
- 某类适配层级放大了解释器开销

## 4. 关系约束

关系约束如下：

- 一个 `Pattern` 可以关联多个 `FunctionHotspot`
- 一个 `Pattern` 可以关联多个 `AsmSnippet`
- 一个 `Pattern` 可以关联多个 `SourceSnippet`
- 一个 `RootCause` 可以关联多个 `Pattern`
- 一个 `Pattern` 可以被多个 `Case` 复用
- `RootCause` 不要求直接绑定单个函数或单段源码

## 5. 入库原则

### 5.1 热点函数入库

每条热点函数记录至少包含：

- `caseId`
- `component`
- `category`
- `platform`
- `symbol`
- `inclusiveTime`
- `exclusiveTime`
- `sampleCount`
- `callPath`
- `artifactRefs`

### 5.2 机器码入库

Arm 与 x86 的汇编不得裸对比，必须先有对齐说明。每条机器码片段记录至少包含：

- `functionId`
- `platform`
- `symbol`
- `basicBlock`
- `sourceLocation`
- `instructionRange`
- `snippet`
- `annotation`
- `alignmentReason`

补充要求：

- 机器码入库不得默认假设与源码行线性一一对应
- 对于同一源码语义单元，可记录多个离散机器码区块
- 若编译器重排、拆分、合并或消除某段源码对应的机器码，必须在对齐说明中显式记录
- 机器码展示应优先支持“逻辑分析块 -> 平台离散区块”的组织方式，而不是伪逐行对照
- 对于长机器码片段，入库记录应支持首屏摘录与完整展开两种展示层次

### 5.3 源码片段入库

每条源码片段记录至少包含：

- `file`
- `location`
- `summary`
- `snippet`
- `relatedFunctionIds`
- `relatedPatternIds`

补充要求：

- 源码片段入库应支持一个逻辑分析块关联多个源码锚点
- 源码锚点应允许与多个离散机器码区块建立关联
- 对于较长源码片段，应提供首屏摘录与完整展开两种展示层次

## 6. Pattern 定义要求

每个 `Pattern` 至少应包含：

- 标题
- 简述
- 置信度
- 受影响 case 列表
- 支撑函数列表
- 关联根因列表
- 关联 artifact 列表

一个 `Pattern` 之所以成立，必须满足：

- 不是单点偶发热点
- 能在多个样本、多个位置或多个上下文中重复出现
- 对性能解释具有共性价值

## 7. RootCause 定义要求

每个 `RootCause` 至少应包含：

- 标题
- 摘要
- 置信度
- 关联 pattern 列表
- 关联 artifact 列表
- 优化建议
- 验证计划

`RootCause` 不是现象本身，而是对一组 pattern 的更高层归纳。

## 8. 证据等级

建议使用以下证据等级：

- `confirmed`
- `high_confidence`
- `hypothesis`

升级原则：

- 同时具备时间差、热点差、机器码或源码支撑，可到 `confirmed`
- 具备时间差和热点差，但缺少底层支撑，只能到 `high_confidence`
- 只有现象描述，没有稳定支撑，只能为 `hypothesis`

## 9. 建议数据结构

### 9.1 Pattern

```json
{
  "id": "pattern_001",
  "title": "高频临时对象创建",
  "summary": "临时对象在行转换与包装路径中被反复创建。",
  "confidence": "high",
  "caseIds": ["q01", "q12"],
  "functionIds": ["func_001"],
  "rootCauseIds": ["rc_001"],
  "artifactIds": ["source_row_adapter_excerpt"]
}
```

### 9.2 RootCause

```json
{
  "id": "rc_001",
  "title": "当前行表示设计导致临时对象过多",
  "summary": "对象密集型行转换逻辑会反复创建短生命周期包装对象，并放大平台敏感的内存开销。",
  "confidence": "high_confidence",
  "patternIds": ["pattern_001"],
  "artifactIds": ["source_row_adapter_excerpt", "asm_arm_func_001", "asm_x86_func_001"],
  "optimizationIdeas": [
    "减少临时包装对象创建",
    "在安全前提下复用中间对象"
  ],
  "validationPlan": [
    "对比改写前后的对象分配次数",
    "对比 Arm 与 x86 上分配敏感型微基准"
  ]
}
```

## 10. 报告展示要求

### 10.1 Pattern 可见

报告必须直接展示 `Pattern` 页面或 `Pattern` 卡片，不能把 pattern 仅作为内部中间层。

### 10.2 RootCause 可见

报告必须直接展示 `RootCause` 页面或 `RootCause` 卡片，用于承接优化建议与验证计划。

### 10.3 证据链可钻取

读者应能沿以下路径下钻：

- case -> function
- function -> pattern
- pattern -> root cause
- function / pattern / root cause -> artifact

## 11. 正例

正例：

- `_PyObject_Malloc` 在多个 case 中都是热点
- 行包装源码中也出现相同对象 churn 模式
- Arm / x86 汇编片段可对齐
- 最终归纳为“当前行表示设计导致临时对象过多”

这时应建模为：

- `FunctionHotspot(func_001)`
- `SourceSnippet(source_row_adapter_excerpt)`
- `Pattern(pattern_001)`
- `RootCause(rc_001)`

## 12. 反例

反例 1：

- 发现一个热点函数后，直接写成根因

问题：

- 没有经过 pattern 层收敛，结论太早

反例 2：

- 一个根因只绑定一处源码，且没有重复模式支撑

问题：

- 根因退化成了局部实现备注，不具备报告层级的解释力

反例 3：

- 页面上只出现根因标题，没有 pattern 与 artifact

问题：

- 读者无法追溯证据链，报告可信度不足
