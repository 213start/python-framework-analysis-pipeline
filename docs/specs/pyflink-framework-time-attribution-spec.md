# PyFlink 框架耗时归属规范 v0.1

## 1. 目标

本规范用于约束 PyFlink 跨平台性能分析中“PyFlink 框架耗时”的边界、分类体系与判定顺序，确保 Arm 与 x86 的分析结果可复现、可解释、可追溯。

本规范主要回答三个问题：

- 哪些时间属于 PyFlink 框架耗时，哪些不属于
- 属于框架耗时的样本应该如何稳定归类
- 哪些样本允许进入正式报告结论链

## 2. 适用范围

本规范适用于以下数据：

- PyFlink workload 执行期间采集到的 profiling 样本
- Python 用户态、native 用户态、内核态热点样本
- 端到端耗时拆解中与框架相关的时间
- 函数级、分类级、组件级差异分析结果

本规范不负责定义：

- TPC-H 到 Python UDF 的语义等价规则
- 热点函数、机器码与源码证据的入库格式
- 优化建议与收益评估规则

上述内容由其他规范负责。

## 3. 分析边界

本报告只分析 PyFlink 框架部分的跨平台差异。

纳入范围：

- Python 运行时中由 PyFlink 路径触发的执行、调度、桥接、类型转换、对象包装、序列化、结果收发等成本
- 上述路径触发的 CPython、第三方库、glibc、kernel 成本
- 完成 PyFlink workload 执行所必需的框架性 runtime 成本

排除范围：

- 业务 UDF 自身逻辑耗时
- Flink Java 侧业务算子执行耗时
- 与框架无关的外部 IO、网络抖动与环境噪声
- 缺乏证据支撑、无法确认属于框架路径的主观归因

## 4. 归属原则

所有样本必须遵循以下原则：

- 先判断是否在分析范围内，再判断分类
- 先判断一级分类，再判断二级分类
- 优先按调用路径和语义职责归类，而不是按符号名字符串猜测
- 无法稳定归属的样本进入 `Unknown`
- 一旦命中排除规则，不得进入框架耗时统计

## 5. 判定顺序

每个样本必须按以下顺序处理：

1. 是否位于 PyFlink 框架路径
2. 是否命中排除规则
3. 一级分类
4. 二级分类
5. 是否关联热点函数
6. 是否允许支撑正式结论

禁止跳过前置步骤，直接从函数名反推一级分类。

## 6. 一级分类

一级分类固定为以下 9 类：

- `Interpreter`
- `Memory`
- `GC`
- `Object Model`
- `Type Operations`
- `Calls / Dispatch`
- `Native Boundary`
- `Kernel`
- `Unknown`

一级分类用于总览页、分类对比页与管理摘要页，不应随意扩展。更细的差异统一落在二级分类。

## 7. 一级分类定义

### 7.1 `Interpreter`

定义：CPython 解释器主执行路径上的成本，包括字节码分发、frame 执行、解释器循环中的控制逻辑。

包含：

- bytecode dispatch
- frame evaluation
- eval loop
- 解释器路径上的通用 lookup

排除：

- 容器结构操作，归到 `Object Model`
- 类型语义运算，归到 `Type Operations`
- 调用协议与分派，归到 `Calls / Dispatch`

### 7.2 `Memory`

定义：对象分配、释放、引用计数、allocator 路径、内存池与 arena 管理相关成本。

包含：

- alloc/free
- refcount inc/dec
- pymalloc / malloc 路径
- arena / pool / block 管理

排除：

- GC 扫描与回收，归到 `GC`
- 具体对象结构逻辑，优先归到 `Object Model`

### 7.3 `GC`

定义：垃圾回收器的 tracking、扫描、回收与链表维护成本。

包含：

- object tracking / untracking
- generation scan
- collection
- gc list maintenance

排除：

- 普通 refcount 路径，归到 `Memory`

### 7.4 `Object Model`

定义：Python 对象模型及常见容器对象的结构性操作成本。

包含：

- `Dict`
- `Tuple`
- `Misc Objects`
- 容器构造、读写、迭代
- 对象布局与结构操作

排除：

- 具体数值与字符串语义运算，归到 `Type Operations`
- 主解释器循环，归到 `Interpreter`

### 7.5 `Type Operations`

定义：围绕具体内建类型的数据处理操作，强调类型语义而非容器结构。

包含：

- `Int`
- `Float`
- `String`
- 数值转换、算术、比较
- 字符串构造、拼接、编码处理

排除：

- dict / tuple 等容器结构操作，归到 `Object Model`
- 纯内存分配，归到 `Memory`

### 7.6 `Calls / Dispatch`

定义：函数调用、跨层调度、动态分派、参数组织、call protocol 相关成本。

包含：

- `Calls`
- `Dynamic`
- Python function call
- C function call
- vectorcall / argument marshalling

排除：

- 解释器主循环，归到 `Interpreter`
- 第三方库内部执行，归到 `Native Boundary`

### 7.7 `Native Boundary`

定义：由 PyFlink 框架路径触发、位于 Python 运行时之外的用户态 native 成本。

包含：

- `Library`
- `libc`
- 第三方 C/C++ 扩展
- libc 例程
- bridge / serialization 中的 native 路径

排除：

- 内核态行为，归到 `Kernel`
- CPython 内部实现，归到前述 CPython 各类

### 7.8 `Kernel`

定义：内核态成本与系统调用、调度、缺页、中断等待等系统层开销。

包含：

- syscall
- page fault
- futex
- scheduler
- io wait

排除：

- 用户态 libc，归到 `Native Boundary`

### 7.9 `Unknown`

定义：当前无法稳定归属或证据不足以分类的样本。

包含：

- `Other`
- unresolved symbol
- incomplete stack
- classification conflict

排除：

- 任何已有明确归属证据的样本

## 8. 原始分类映射

现有分类建议映射如下：

- `Interpreter` -> `Interpreter`
- `Memory` -> `Memory`
- `Dynamic` -> `Calls / Dispatch.dynamic_dispatch`
- `Library` -> `Native Boundary.third_party_library`
- `Tuple` -> `Object Model.tuple`
- `GC` -> `GC`
- `Int` -> `Type Operations.int`
- `Float` -> `Type Operations.float`
- `String` -> `Type Operations.string`
- `Calls` -> `Calls / Dispatch.calls`
- `Misc Objects` -> `Object Model.misc_objects`
- `libc` -> `Native Boundary.libc`
- `Kernel` -> `Kernel`
- `Dict` -> `Object Model.dict`
- `Lookup` -> 优先归到 `Interpreter.lookup`
- `Other` -> `Unknown`

## 9. 二级分类建议

二级分类用于工程下钻，不直接用于首页展示。建议首版固定为：

- `Interpreter`
  - `bytecode_dispatch`
  - `frame_eval`
  - `lookup`
- `Memory`
  - `alloc_free`
  - `refcount`
  - `arena_pool`
- `GC`
  - `tracking`
  - `collection`
- `Object Model`
  - `dict`
  - `tuple`
  - `misc_objects`
- `Type Operations`
  - `int`
  - `float`
  - `string`
- `Calls / Dispatch`
  - `python_calls`
  - `c_calls`
  - `dynamic_dispatch`
- `Native Boundary`
  - `third_party_library`
  - `libc`
- `Kernel`
  - `syscall`
  - `page_fault`
  - `scheduler_wait`
- `Unknown`
  - `unresolved`
  - `incomplete_stack`
  - `classification_conflict`

## 10. 归属字段

每条归因记录至少应包含以下字段：

```json
{
  "platform": "arm64",
  "caseId": "q01",
  "threadId": "1234",
  "component": "cpython",
  "categoryL1": "Memory",
  "categoryL2": "alloc_free",
  "symbol": "_PyObject_Malloc",
  "inclusiveTimeMs": 12.4,
  "exclusiveTimeMs": 8.1,
  "inScope": true,
  "scopeReason": "Triggered by PyFlink runtime object creation path"
}
```

必填字段：

- `platform`
- `caseId`
- `component`
- `categoryL1`
- `symbol`
- `inclusiveTimeMs`
- `exclusiveTimeMs`
- `inScope`
- `scopeReason`

建议字段：

- `threadId`
- `processId`
- `categoryL2`
- `sampleCount`
- `callPath`
- `artifactRefs`

## 11. 组件轴

除分类外，每条记录还应带组件标签。组件轴固定为：

- `cpython`
- `third_party`
- `glibc`
- `kernel`
- `unknown`

组件轴与分类轴并行存在，不互相替代。例如：

- `component=cpython, categoryL1=Memory`
- `component=glibc, categoryL1=Native Boundary`
- `component=kernel, categoryL1=Kernel`

## 12. 排除规则

以下样本不进入 PyFlink 框架耗时：

- 明确属于业务 UDF 函数体内部逻辑
- 明确属于 Flink Java 业务算子执行
- 仅反映外部等待且与框架行为无关
- 无法确认属于 PyFlink 框架路径，且缺乏纳入证据

被排除的样本必须保留排除原因，不得直接丢弃原始记录。

## 13. Unknown 规则

`Unknown` 仅用于保守兜底。

约束：

- `Unknown` 总占比超过 `5%` 时必须告警
- 超过 `10%` 时不得输出高置信根因结论
- 若本可分类的样本大量落入 `Unknown`，应视为归因流程缺陷

## 14. 正例

正例 1：

- 样本位于 `_PyEval_EvalFrameDefault`
- 调用链确认处于 PyFlink runtime 路径
- 主要耗时来自 frame 执行

归属：

- `inScope=true`
- `component=cpython`
- `categoryL1=Interpreter`
- `categoryL2=frame_eval`

正例 2：

- 样本位于 `_PyObject_Malloc`
- 由 PyFlink 数据包装与中间对象创建触发

归属：

- `inScope=true`
- `component=cpython`
- `categoryL1=Memory`
- `categoryL2=alloc_free`

正例 3：

- 样本位于 `malloc`
- 来自 PyFlink 调用的 native serialization 路径

归属：

- `inScope=true`
- `component=glibc`
- `categoryL1=Native Boundary`
- `categoryL2=libc`

## 15. 反例

反例 1：

- 样本位于用户自定义 Python UDF 业务逻辑

处理：

- `inScope=false`
- `scopeReason=Business UDF logic excluded`

反例 2：

- 样本位于 Java 侧 SQL 业务算子执行

处理：

- `inScope=false`
- `scopeReason=Flink Java business execution excluded`

反例 3：

- 符号缺失，仅知在用户态，但缺乏调用链上下文

处理：

- 无法确认 in-scope 时先排除
- 已确认 in-scope 但无法分类时归到 `Unknown`

## 16. 报告展示规则

默认报告只展示一级分类。二级分类用于详情页和工程下钻页。

总览页可展示：

- 一级分类耗时
- 一级分类占比
- Arm / x86 一级分类差值

详情页可展示：

- 二级分类
- 热点函数
- 机器码与源码证据

## 17. 结论准入规则

只有同时满足以下条件的记录，才允许进入正式结论链：

- `inScope=true`
- 一级分类明确
- 组件明确
- 非纯 `Unknown`
- 可关联到具体 case 或热点函数

否则只能进入观察区或待确认区，不得直接支撑首页结论。
