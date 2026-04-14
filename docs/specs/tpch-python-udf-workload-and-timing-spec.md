# TPC-H 到整体 Python UDF 工作负载与计时规范 v0.1

## 1. 目标

本规范定义如何将 TPC-H SQL 改写为一个整体 Python UDF 工作负载，并规定相关时间指标的采集与归一化方法。

本规范覆盖两个目标：

- 保证 TPC-H SQL 的业务语义作为整体被映射到一个 Python UDF
- 明确 Demo 总耗时、TM 端到端耗时、业务算子耗时、框架调用耗时的采集口径

## 2. 核心原则

- 每个 TPC-H SQL 必须改写为一个整体 Python UDF
- 不允许把一条 SQL 的语义拆成多个 Python UDF
- 调用 PyFlink Python API 后，算子仍会由 Java 侧执行，因此用于测量的 Python UDF 必须自行实现
- 任何额外业务逻辑都不得混入测量用 UDF

## 3. 工作负载形式

每个用例都应包含以下资产：

- 原始 TPC-H SQL
- 整体 Python UDF 实现
- SQL 到 Python UDF 的语义映射说明
- 结果正确性验证结果

本规范中的“整体 Python UDF”强调：

- SQL 的完整业务语义保留在同一段 Python UDF 中
- 不把 SQL 语义切分到多个 UDF 或多个临时桥接层
- 保持 Arm 与 x86 的测量边界一致

## 4. 三段计时链路

时间采集采用三段式：

1. `Java PreUDF`
   - 在进入 UDF 链路前记录 `java_start_time`
2. `Python UDF`
   - 在 Python UDF 内记录 `python_start_time`
   - 在 Python UDF 内记录 `python_end_time`
3. `Java PostUDF`
   - 在 UDF 链路结束后记录 `java_end_time`

## 5. 核心指标定义

### 5.1 Demo 总耗时

定义：

- 从 Client 提交 Job 到 Job 执行完成的总时间

用途：

- 用于描述大数据场景下的外部可见总耗时

### 5.2 TM 端到端耗时

定义：

- TaskManager 侧 SubTask 开始执行到执行结束的时间

用途：

- 用于隔离 Client 提交链路之外的执行期差异

### 5.3 业务算子耗时

定义：

- `Python UDF Time`
- `python_udf_time = python_end_time - python_start_time`

用途：

- 表示 Python UDF 内部的业务算子执行时间

### 5.4 框架调用耗时

定义：

- `framework_call_time = java_end_time - java_start_time - python_udf_time`

用途：

- 表示围绕 Python UDF 边界的 PyFlink 包装、桥接、编解码与收发成本

## 6. 指标含义说明

需要明确以下区别：

- `Demo Total Time` 是最外层业务观测指标
- `TM End-to-End Time` 是 TaskManager 层面的执行指标
- `Business Operator Time` 是 Python UDF 内部业务执行时间
- `Framework Call Time` 是 Python UDF 外围的框架包装与桥接成本

因此，`Framework Call Time` 不等同于“整个 Python 侧总耗时”，它只表示 Java-Python-Java 边界外壳的框架成本。

## 7. 归一化规则

本规范默认保留三种视角：

- batch total
- per UDF invocation
- per record

默认展示口径：

- `per UDF invocation`

辅助展示口径：

- `per record`

原因：

- 当前测量边界围绕一次 `Java PreUDF -> Python UDF -> Java PostUDF` 调用建立
- `per invocation` 最贴近框架调用口径
- `per record` 适合解释 batch 大小变化带来的影响

## 8. 必填元数据

每个用例至少需要记录以下元数据：

- `caseId`
- `platform`
- `batchSize`
- `invocationCount`
- `recordCount`
- `warmupCount`
- `repeatCount`
- `timingSource`
- `java_start_time`
- `python_start_time`
- `python_end_time`
- `java_end_time`

## 9. 结果正确性要求

性能采集前必须先完成语义正确性验证，至少包括：

- 结果行数一致
- 关键字段值一致
- 聚合结果一致
- 若 SQL 有排序语义，则排序结果一致

若语义未验证通过，则该用例不得进入正式性能对比。

## 10. 允许与禁止事项

允许：

- 在不破坏业务语义的前提下，用 Python UDF 重写 TPC-H SQL
- 为保证边界一致而做必要的框架包装
- 记录 batch total、per invocation、per record 三类指标

禁止：

- 将一条 SQL 拆分成多个 Python UDF
- 在 UDF 中混入与 SQL 无关的额外业务逻辑
- 用不同平台的不同实现路径规避真实差异
- 在未声明偏差的前提下修改语义

## 11. 建议输出格式

```json
{
  "caseId": "q01",
  "platform": "arm64",
  "batchSize": 1024,
  "invocationCount": 1024,
  "recordCount": 1024,
  "metrics": {
    "demoTotalTime": "5.23 s",
    "tmEndToEndTime": "4.18 s",
    "businessOperatorTime": "1.77 ms",
    "frameworkCallTime": "0.91 ms"
  },
  "normalization": {
    "default": "per_invocation",
    "available": ["batch_total", "per_invocation", "per_record"]
  }
}
```

## 12. 正例

正例：

- TPC-H Q1 被整体改写为一个 Python UDF
- Java PreUDF / Python UDF / Java PostUDF 三段计时齐全
- 结果正确性已验证
- 输出同时包含 batch total、per invocation、per record

## 13. 反例

反例 1：

- 将一条 SQL 拆成多个 Python UDF，再拼出总结果

问题：

- 测量边界不再稳定，无法比较平台差异

反例 2：

- 在 Python UDF 中加入额外业务逻辑或临时调试逻辑

问题：

- 业务算子耗时被污染，框架调用耗时解释失真

反例 3：

- 仅记录总时间，不记录 invocation 数和 record 数

问题：

- 无法稳定换算成 per invocation 与 per record 口径

## 14. 报告展示建议

首页与总览页建议展示：

- Demo 总耗时
- TM 端到端耗时
- 业务算子耗时
- 框架调用耗时

详情页建议同时提供：

- batch total
- per invocation
- per record

默认显示：

- per invocation
