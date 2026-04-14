# python-framework-analysis-pipeline

用于构建 Python 框架分析流程的仓库，当前内置的是一套面向 PyFlink 的性能差异分析模型、规范文档、前端 demo 与示例数据。

## 当前范围

当前仓库当前主要包含以下内容：

- PyFlink 框架耗时归属规范
- TPC-H SQL 到整体 Python UDF 工作负载与计时规范
- 热点函数、模式、根因与证据建模规范
- 报告数据 schema 规范
- 报告页面字段规范
- 前端 demo 骨架与示例数据包
- 一套可继续抽象到其他 Python 框架的软件分析流程基础

## 目录结构

- `docs/specs/`
  - 报告规范、数据规范、分析规范与抽象方案草案
- `docs/plans/`
  - 前端实现计划与阶段性设计记录
- `web/`
  - 前端 demo
- `web/public/report-package/`
  - 示例数据包与示例附件

## 后续方向

- 将当前 PyFlink 模型进一步整理成可复用抽象
- 将 schema 进一步落成 JSON Schema 或类型模型
- 扩充更完整的示例数据包
- 继续完善正式汇报 demo 的页面表达与证据对比能力

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

应用默认从 `web/public/report-package/` 读取示例数据；当某些文件尚未接线完成时，会回退到内置 mock 数据。
