# PyFlink 四层模型最小示例

这组文件是 PyFlink 参考 demo 在四层模型下的一组输入对象：

- `frameworks/pyflink.framework.json`
- `datasets/tpch-on-pyflink-2026q2.dataset.json`
- `sources/pyflink-reference-source.source.json`
- `projects/tpch-pyflink-reference.project.json`

它们的定位是：

- 用作四层模型的示例输入
- 用作前端 demo 的默认数据来源
- 用作后续新增 PySpark、PyTorch 等 Python 框架示例的结构参考

## 对应关系

当前 PyFlink 参考 demo 的主要归属如下：

- 框架边界、指标、分类体系进入 `Framework`
- 用例、栈全景、函数、模式、根因、优化机会进入 `Dataset`
- 源码文件、源码锚点、附件索引进入 `Source`
- case/function/pattern/root-cause 的跨对象关系进入 `Project`
- `artifacts/*` 作为 `Source.artifactIndex` 引用的真实附件路径

## 约束

这组示例刻意保持“可审、可演示”：

- 覆盖当前 PyFlink demo 的主链页面
- 不追求伪造真实内网规模
- 但必须覆盖：
  - `Framework`
  - `Dataset`
  - `Source`
  - `Project`
  - `caseBindings`
  - `functionBindings`
  - `patternBindings`
  - `rootCauseBindings`

## 目的

这组示例用于回答两个问题：

1. 四层对象能否支撑当前 PyFlink 主链页面
2. 后续新增框架时需要提供哪些最小字段才能接入同一个报告壳
