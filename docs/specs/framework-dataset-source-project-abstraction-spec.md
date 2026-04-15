# 框架 / 数据 / 源码 / 项目 四层抽象方案 v0.1

## 1. 目标

本方案用于把当前面向 PyFlink 的性能差异分析报告，抽象成可复用于其他 Python 框架类软件的通用模型。

目标不是“在代码里写死多个软件实例”，而是建立一套稳定接口，使报告壳只消费标准化输入对象：

- `Framework`
- `Dataset`
- `Source`
- `Project`

其中：

- `Framework` 定义分析对象的框架边界和分析语义
- `Dataset` 定义性能数据与证据数据
- `Source` 定义源码与源码附件资产
- `Project` 负责把前三者显式装配起来

本方案的核心要求：

- 框架、数据、源码三者必须解耦
- 用例不再作为全局公共对象，而是属于某个数据集
- 不同软件即使 benchmark family 相同，也不得共享同一个 case 定义
- 第一版只支持显式静态绑定，不做自动绑定推断

## 2. 设计原则

### 2.1 接口与实现分离

前端报告壳不应理解 `PyFlink`、`PySpark`、`PyTorch` 的具体业务语义，只应依赖四层对象定义和绑定表。

### 2.2 单一职责

- `Framework` 不携带具体性能数据
- `Dataset` 不携带源码仓细节
- `Source` 不携带平台性能结论
- `Project` 不承担推理职责，只承担显式映射职责

### 2.3 显式优先

第一版不总结自动绑定规则，不依赖推断。所有 case、函数、源码锚点、机器码区块的对应关系都通过 `Project` 显式声明。

### 2.4 软件隔离

不同软件的 benchmark 即使名字相同，也只共享 `benchmarkFamily`，不共享具体 `Case`。

例如：

- `pyflink/tpch-q1`
- `pyspark/tpch-q1`

是两个不同 case。

## 3. 范围边界

本方案默认只关心 Python 相关分析对象：

- CPython
- Python 三方库
- 目标框架自身的 Python 侧代码

默认排除：

- JVM
- CUDA / cuDNN / NCCL
- 外部加速运行时
- 非 Python 侧业务执行体

换句话说，`Source` 所表示的源码资产，也只指 Python 相关源码与附件。

## 4. 四层对象

## 4.1 `Framework`

### 4.1.1 职责

定义某一类框架分析对象的稳定语义边界，不包含任何具体实验数据和源码仓 revision。

### 4.1.2 应包含的信息

- `id`
- `name`
- `kind`
- `version`
- `languageScope`
- `analysisScope`
- `excludedScope`
- `metricDefinitions`
- `taxonomy`
- `pageConfig`

### 4.1.3 不应包含的信息

- case 实例
- 平台耗时结果
- profiling 样本
- repo revision
- 具体源码文件路径

### 4.1.4 示例

```json
{
  "id": "pyflink",
  "name": "PyFlink",
  "kind": "python-framework",
  "version": "1.x",
  "languageScope": ["cpython", "python-third-party", "framework-python-code"],
  "analysisScope": ["python-side framework overhead"],
  "excludedScope": ["jvm runtime", "business udf logic"],
  "metricDefinitions": ["demo_total_time", "tm_end_to_end_time", "business_operator_time", "framework_call_time"],
  "taxonomyRef": "taxonomy-python-framework-v1"
}
```

## 4.2 `Dataset`

### 4.2.1 职责

定义一次分析项目中的数据结果与证据数据，不包含源码仓结构和框架定义细节。

### 4.2.2 应包含的信息

- `id`
- `name`
- `frameworkId`
- `benchmarkFamily`
- `platforms`
- `cases`
- `stackOverview`
- `patterns`
- `rootCauses`
- `opportunities`
- `artifacts`

### 4.2.3 关键约束

- `Case` 属于 `Dataset`
- 不同 `Dataset` 的 case 不共享 identity
- `Dataset` 只描述“有什么数据”，不描述“这些数据对应源码仓哪里”

### 4.2.4 `Case` 的新定位

`Case` 应改为 `DatasetCase`，字段建议：

- `id`
- `datasetId`
- `name`
- `benchmarkFamily`
- `implementationForm`
- `metrics`
- `hotspots`
- `patternIds`
- `rootCauseIds`
- `artifactIds`

### 4.2.5 示例

```json
{
  "id": "tpch-on-pyflink-2026q2",
  "name": "TPC-H on PyFlink",
  "frameworkId": "pyflink",
  "benchmarkFamily": "TPC-H",
  "platforms": ["arm64", "x86_64"],
  "cases": [
    {
      "id": "tpch-q1-pyflink",
      "name": "TPC-H Q1",
      "benchmarkFamily": "TPC-H",
      "implementationForm": "single-python-udf"
    }
  ]
}
```

## 4.3 `Source`

### 4.3.1 职责

定义源码资产与源码附件，不包含性能结论和 case 数据。

### 4.3.2 应包含的信息

- `id`
- `frameworkId`
- `repo`
- `revision`
- `sourceRoots`
- `sourceFiles`
- `symbolIndex`
- `artifactIndex`

### 4.3.3 不应包含的信息

- 热点函数是否慢
- 模式和根因结论
- case 的指标结果

### 4.3.4 说明

`Source` 是一个源码资产包，可以指向：

- 代码仓 URL
- 特定 revision
- 本地导出的源码快照
- 反汇编与源码索引

## 4.4 `Project`

### 4.4.1 职责

`Project` 是四层模型的装配层。它负责把：

- 一个 `Framework`
- 一份 `Dataset`
- 一份 `Source`

组合成一个可展示、可钻取、可追溯的报告项目。

### 4.4.2 核心原则

`Project` 不负责推断，只负责声明。

也就是说，`Project` 不做“自动绑定”，只保存显式映射。

### 4.4.3 应包含的信息

- `id`
- `name`
- `frameworkRef`
- `datasetRef`
- `sourceRef`
- `caseBindings`
- `functionBindings`
- `patternBindings`
- `rootCauseBindings`

## 5. 绑定表设计

## 5.1 为什么绑定表必须存在

因为以下关系不稳定，不能从对象本身自动推导：

- 某个 case 对应哪些源码文件
- 某个热点函数对应哪些源码锚点
- 某个源码锚点对应哪些 Arm / x86 机器码区块
- 某个 pattern 跨哪些函数与源码位置

因此，第一版必须通过显式绑定表表达。

## 5.2 `caseBindings`

### 作用

把 `DatasetCase` 绑定到源码资产与实现资产。

### 字段建议

- `caseId`
- `sourceBundleId`
- `sourceFileIds`
- `primaryArtifactIds`
- `notes`

### 示例

```json
{
  "caseId": "tpch-q1-pyflink",
  "sourceBundleId": "pyflink-main",
  "sourceFileIds": ["pyflink/table/udf.py", "pyflink/fn_execution/beam_runner.py"],
  "primaryArtifactIds": ["sql_q01", "pyflink_q01"]
}
```

## 5.3 `functionBindings`

### 作用

把数据集中的热点函数绑定到源码锚点与机器码区块。

### 字段建议

- `functionId`
- `sourceAnchorIds`
- `armArtifactIds`
- `x86ArtifactIds`
- `notes`

### 示例

```json
{
  "functionId": "func_001",
  "sourceAnchorIds": ["anchor_alloc_fastpath", "anchor_alloc_slowpath"],
  "armArtifactIds": ["asm_arm_func_001"],
  "x86ArtifactIds": ["asm_x86_func_001"]
}
```

## 5.4 `patternBindings`

### 作用

把 `Pattern` 和多个函数、源码锚点、附件联系起来。

### 字段建议

- `patternId`
- `functionIds`
- `sourceAnchorIds`
- `artifactIds`

### 说明

`Pattern` 是跨函数、跨源码位置的共性行为模式，因此不能直接附着在单个函数上。

## 5.5 `rootCauseBindings`

### 作用

把根因和 pattern 聚合关系显式写出来。

### 字段建议

- `rootCauseId`
- `patternIds`
- `artifactIds`

## 6. 目录结构建议

建议目录结构采用“四层输入 + 运行时组装”：

```text
analysis-workspace/
  frameworks/
    pyflink.framework.json
    pyspark.framework.json
    pytorch.framework.json
  datasets/
    tpch-on-pyflink-2026q2.dataset.json
    tpch-on-pyspark-2026q2.dataset.json
  sources/
    pyflink-main.source.json
    pyspark-main.source.json
  projects/
    tpch-pyflink-q2.project.json
    tpch-pyspark-q2.project.json
  artifacts/
```

这里要点是：

- `frameworks/` 只放框架定义
- `datasets/` 只放结果数据
- `sources/` 只放源码资产定义
- `projects/` 只放绑定关系
- `artifacts/` 作为公共附件仓

如果后续改成 API，也只是把这四类 JSON 换成四类接口响应，模型本身不变。

## 7. 页面层该如何消费

前端报告壳应按以下顺序加载：

1. 读取 `project`
2. 根据 `frameworkRef` 读取 `framework`
3. 根据 `datasetRef` 读取 `dataset`
4. 根据 `sourceRef` 读取 `source`
5. 根据 `project.bindings` 完成跨对象 drill-down

页面不应直接写死：

- `PyFlink`
- `PySpark`
- `PyTorch`

这些都只能作为 `framework.name` 的数据值出现。

## 8. 对现有 PyFlink 模型的迁移建议

现有 PyFlink 模型可按下列方式拆分：

### 8.1 进入 `Framework`

- 分析边界
- 一级分类体系
- 指标定义
- 页面展示原则

来源：

- `pyflink-framework-time-attribution-spec.md`
- `report-page-field-spec.md` 中的通用部分

### 8.2 进入 `Dataset`

- `case_index`
- `stack_overview`
- `case detail`
- `component/category/function/pattern/root-cause` 的结果数据

来源：

- `datasets/*.dataset.json`
- 由组装层输出的页面 view model

### 8.3 进入 `Source`

- SQL 资产索引
- Python UDF 资产索引
- 源码文件
- 机器码附件
- 源码摘录与 artifact 索引

### 8.4 进入 `Project`

- case 到源码资产绑定
- function 到 source anchor / asm artifact 绑定
- pattern 到 functions / source anchors 绑定
- root cause 到 patterns 绑定

## 9. 当前前端接入形态

当前仓库中的前端已经切到四层模型：

- 四层输入位于 `web/public/examples/four-layer/pyflink-reference/`
- `web/src/data/assembly.ts` 读取 `Project / Framework / Dataset / Source`
- `web/src/data/loaders.ts` 调用组装层输出页面 view model
- 页面组件消费 view model，不直接消费四层原始对象

`web/public/examples/four-layer/pyflink-reference/artifacts/` 是示例证据附件目录，必须通过 `Source.artifactIndex` 引用；页面不能绕过 `Source` 直接硬编码附件路径。

## 10. 组装流程

前端或数据接入层必须执行显式组装步骤：

1. 读取 `Project`
2. 读取 `Framework`
3. 读取 `Dataset`
4. 读取 `Source`
5. 根据绑定表组装页面 view model

要求：

- 页面组件尽量不直接读取四层原始对象
- 页面仍消费组装后的 summary/detail 风格结构
- 现有路由、字段和 drill-down 尽量不变

## 11. 第一版不做什么

为了保持模型稳定，第一版明确不做以下内容：

- 自动绑定规则
- 根据命名自动推断 case 和源码关系
- 根据函数名自动推断源码锚点
- 根据 benchmark family 自动复用 case identity
- 跨 framework 共享同一个 case 对象

## 12. 结论

这套抽象的核心不是“支持多个软件目录”，而是：

- 让报告壳只依赖标准接口
- 把框架定义、结果数据、源码资产拆开
- 用 `Project` 作为唯一装配层
- 用显式绑定表保证 drill-down 可审计、可追溯、可控

一句话概括：

> 报告壳消费 `Framework + Dataset + Source + Project` 四层对象；前三者互相解耦，`Project` 通过显式静态绑定表把它们组装成一份具体的软件分析报告。
