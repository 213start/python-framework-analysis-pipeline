# 报告页面字段规范 v0.1

## 1. 目标

本规范定义交互式报告各页面的字段、区块与 drill-down 目标，保证页面表达与数据 schema 对齐。

本规范增加一个全局要求：

- 报告必须展示性能差异的全景，而不是只展示摘要结论或 Top 项
- 任一总览页都应优先体现两个平台在完整分布上的差异，再提供排序、根因和 drill-down
- 图表与表格应以“全量分布可见”为默认原则，避免只保留首要项、摘要项或少量卡片

本规范再增加一个实现优先级要求：

- 组件/分类耗时分布 -> 全量热点函数 -> 单函数下钻 -> 源码视角机器码 diff，这条链是报告的核心主线
- 该主线应投入约 80% 的设计与实现精力
- 其他页面、摘要卡、装饰性可视化、非关键导航均降为次优先级

## 2. 页面树

建议页面树如下：

- `/` 首页 / 执行摘要
- `/scope` 实验边界与口径
- `/cases` 用例资产中心
- `/analysis/by-case` 按用例分析
- `/analysis/by-stack` 按性能栈分析
- `/case/:caseId` 单用例详情
- `/component/:componentId` 单组件详情
- `/category/:categoryId` 单分类详情
- `/function/:functionId` 热点函数详情
- `/pattern/:patternId` 模式详情
- `/root-cause/:rootCauseId` 根因详情
- `/artifact/:artifactId` 附件详情
- `/insights` 优化机会与验证路线图

## 3. 首页

主要数据源：

- `summary/executive_summary.json`
- `summary/opportunity_ranking.json`

必备区块：

- 执行摘要标题
- 指标摘要卡
- 主结论区
- 顶层机会列表
- 推荐浏览路径

建议字段：

- 指标标题
- Arm 值
- x86 值
- 差异
- drill-down 目标
- Top Pattern
- Top Root Cause

## 4. Scope 页面

主要数据源：

- `summary/scope.json`

必备区块：

- 范围摘要卡
- 纳入范围
- 排除范围
- 指标边界总览
- 指标定义
- 分类体系

设计要求：

- `Scope` 页面不是纯文字说明页，应承担“分析合法性总览”的职责
- 首屏应快速回答：
  - 当前报告纳入了什么
  - 排除了什么
  - 共有多少一级分类和组件轴
  - Unknown 阈值是多少
- 指标部分应同时提供：
  - 可快速扫读的边界总览卡片/图形化区块
  - 可追溯的定义明细表
- 分类体系部分应明确展示：
  - 一级分类集合
  - 组件轴集合
  - Unknown 告警阈值

建议字段：

- page highlights
- includedScope
- excludedScope
- metric definition
- boundary
- normalization
- taxonomy

## 5. Cases 页面

主要数据源：

- `summary/case_index.json`

必备区块：

- 用例摘要卡
- 四类指标差异全景
- 用例列表
- 每个用例的 demo / tm / operator / framework 差异
- 用例资产编目

设计要求：

- `Cases` 页面应同时承担两个目标：
  - 作为用例差异总览页，展示各用例在 Demo / TM / 业务算子 / 框架调用四类指标上的差异
  - 作为资产编目页，展示每个用例对应的 SQL、Python UDF 和语义状态
- “四类指标差异全景”应允许快速比较每个 case 的四类差异，不应只保留 Demo 或框架单一指标
- 资产编目区应至少展示：
  - 规模
  - 工作负载形态
  - 语义状态
  - Python UDF 形态
  - SQL 资产入口
  - Python UDF 实现入口
  - 简短说明

点击目标：

- `/case/:caseId`

## 6. By Case 页面

主要数据源：

- `summary/case_index.json`

必备区块：

- case ranking
- 关键差异摘要
- case delta 条带图
- 用例表格

建议字段：

- case name
- demo delta
- tm delta
- operator delta
- framework delta

## 7. By Stack 页面

主要数据源：

- `summary/stack_overview.json`

必备区块：

- 组件双平台堆叠柱状图
- 分类双平台堆叠柱状图
- 组件全量明细表
- 分类全量明细表

设计要求：

- `By Stack` 的目标是展示性能差异全景，不展示“首要组件”“首要分类”“热点入口函数”这类摘要卡
- 页面顶部必须先展示两个全景图：
  - 一个组件维度图
  - 一个分类维度图
- 两张图都必须使用双平台对照：
  - 每个图中包含两个柱体，分别代表 Arm 和 x86
  - 每个柱体内部按组件或分类做堆叠
  - 必须提供图例
  - 必须能够清晰体现两个平台在耗时分布上的差异，而不仅是总量差异
- 图表默认展示绝对耗时分布；若支持切换，占比视图只能作为辅助视图，不得替代默认视图
- 图下方的明细表必须展示全量行，不得只展示 Top N

组件图字段要求：

- platform
- total time
- component segments
- 每个 component segment 的 time
- 每个 component segment 的 share

分类图字段要求：

- platform
- total time
- category segments
- 每个 category segment 的 time
- 每个 category segment 的 share

组件明细表字段要求：

- component name
- Arm time
- x86 time
- Arm share
- x86 share
- absolute delta
- delta contribution
- drill-down target

分类明细表字段要求：

- category name
- level
- Arm time
- x86 time
- Arm share
- x86 share
- absolute delta
- delta contribution
- top function
- drill-down target

建议字段：

- component name
- category name
- level
- arm time
- x86 time
- arm share
- x86 share
- absolute delta
- delta contribution
- top function

点击目标：

- `/component/:componentId`
- `/category/:categoryId`
- `/function/:functionId`

## 8. Case Detail 页面

主要数据源：

- `details/cases/:caseId.json`

必备区块：

- 用例摘要
- 时间拆解
- 热点函数
- 关联模式
- 关联根因
- 附件入口

建议字段：

- case name
- semantic notes
- known deviations
- metrics
- hotspots
- pattern ids
- root cause ids
- artifact ids

## 9. Component Detail 页面

主要数据源：

- `details/components/:componentId.json`

必备区块：

- 组件摘要
- 双平台耗时分布
- 全量热点函数表
- 关联模式与根因
- 附件入口

设计要求：

- 页面必须把该组件下的热点函数列全，不允许只展示 Top N
- 热点函数表必须成为页面主区块，而不是附属信息
- 热点函数表中每一行都必须可点击进入函数详情页
- 页面表达应直接承接 `By Stack` 的组件分布图，让读者从组件分布顺滑进入函数层

热点函数表字段要求：

- function symbol
- Arm self time
- x86 self time
- Arm total time
- x86 total time
- Arm share
- x86 share
- absolute delta
- delta contribution
- category
- drill-down target

## 10. Category Detail 页面

主要数据源：

- `details/categories/:categoryId.json`

必备区块：

- 分类摘要
- 双平台耗时分布
- 受影响 case
- 全量热点函数表
- 关联模式
- 附件入口

设计要求：

- 页面必须把该分类下的热点函数列全，不允许只展示 Top N
- 热点函数表中每一行都必须可点击进入函数详情页
- 页面应支持从分类分布直接下钻到函数 diff 证据链

热点函数表字段要求：

- function symbol
- Arm self time
- x86 self time
- Arm total time
- x86 total time
- Arm share
- x86 share
- absolute delta
- delta contribution
- component
- drill-down target

## 11. Function Detail 页面

主要数据源：

- `details/functions/:functionId.json`

必备区块：

- 函数摘要
- self / total 时间对比
- 调用路径
- 源码对齐视图
- 机器码 diff 视图
- 关联 case
- 关联 pattern
- 附件入口

设计要求：

- 每个热点函数都必须还能再往下钻一级，进入差异对比界面
- 函数详情页的重点不是摘要文字，而是两个平台机器码差异的源码视角对照
- diff 界面不得假设源码与机器码是一一按顺序对齐的
- diff 界面必须采用明确层级：
  - 第一层是逻辑分析块
  - 第二层是源码锚点
  - 第三层是单个源码锚点展开后的 Arm / x86 机器码区块
- 分析块标题必须作为视觉重心，字号与字重显著高于正文
- 源码锚点标题也必须放大，成为分析块内部的第二视觉重心
- 一个逻辑分析块可关联多个源码锚点，也可关联每个平台上的多个离散机器码区块
- 分析块标题右侧的 tag 必须表示行为模式，而不是映射关系类型
- 行为模式命名参考 `sisibeloved/cinderx#3` 中的模式族，首批推荐：
  - `NumericLoop`
  - `BranchFSM`
  - `ObjectManipulator`
  - `CallDispatcher`
  - `AsyncStateMachine`
  - `ReflectionMeta`
  - `ImportInit`
- `Arm 机器码` 和 `x86 机器码` 仅用于分栏识别，不应成为视觉重点：
  - 不加粗
  - 使用更浅的颜色
  - 层级低于源码锚点标题
- “关注指令”仅显示 opcode 本体，不展示参数
- diff 界面必须可被视为这条主线的终点证据页
- 若源码或机器码过长，页面必须提供折叠与展开能力
- 默认视图应展示“关键差异摘要 + 首屏关键片段”，其余内容按块展开
- 折叠应至少支持：
  - 整个逻辑分析块折叠
  - 单个源码锚点折叠
  - 单个平台单个机器码区块折叠

源码对齐 diff 视图字段要求：

- source file
- source location
- diff guide
- logical block label
- logical block summary
- pattern tag
- mapping type
- source anchors
- Arm asm regions
- x86 asm regions
- diff signals
- alignment note
- performance note
- collapse state metadata

## 12. Pattern Detail 页面

主要数据源：

- `details/patterns/:patternId.json`

必备区块：

- 模式标题与摘要
- 置信度
- 支撑 case
- 支撑函数
- 关联根因
- 代表性附件

## 13. Root Cause Detail 页面

主要数据源：

- `details/root_causes/:rootCauseId.json`

必备区块：

- 根因标题与摘要
- 置信度
- 支撑模式
- 支撑附件
- 优化建议
- 验证计划

## 14. Artifact 页面

主要数据源：

- `details/artifacts/:artifactId.json`
- `artifacts/*`

必备区块：

- 附件元数据
- 附件内容
- 若可对齐，展示 Arm / x86 对照

设计要求：

- 对于函数级机器码证据，`Artifact` 页面应作为原始材料页，而不是主要 diff 页面
- 主要对比体验应在函数详情页完成；artifact 页面用于查看完整原文与补充证据

建议字段：

- title
- type
- description
- path
- content type
- content

## 15. Insights 页面

主要数据源：

- `summary/opportunity_ranking.json`
- `summary/pattern_index.json`
- `summary/root_cause_index.json`

必备区块：

- pattern -> root cause -> opportunity 收口视图
- 机会排序
- 模式总览
- 根因总览

建议字段：

- title
- confidence
- impact
- effort
- estimated gain
- root cause binding

## 16. Drill-down 规则

必须保证以下路径存在：

- 首页 KPI -> case 或 stack 详情
- case overview -> case detail
- stack overview -> component / category detail
- case detail hotspot -> function detail
- function detail -> pattern detail
- pattern detail -> root cause detail
- root cause detail -> insights
- function / pattern / root cause -> artifact

任何管理层可见数字都不应停留在“不可下钻”的终点。

## 17. 展示规则

- 总览页优先展示一级摘要，再展示明细表格
- pattern 与 root cause 必须显式可见
- artifact 原始内容不得直接堆在首页
- 长列表默认 Top N，必要时再展开

## 18. 空状态规则

若页面数据不足，必须明确说明：

- 缺的是哪一组字段
- 是未采集、未接线还是被过滤
- 当前页面还能否支持部分结论

缺少 pattern 或 root cause 时，不得伪造“分析已完成”的占位内容。

## 19. 最低校验清单

在认为前端与本规范对齐前，应确认：

- 每个路由都有主数据源
- 每个主要卡片或表格都有字段定义
- 每个摘要字段至少有一个 drill-down 目标
- pattern 页面已从结构化数据填充
- root cause 页面不会脱离 pattern 单独出现
- artifact 页面可以打开实际附件
