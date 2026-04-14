import { DataTable, EmptyState, MetricCard, PageHeader, SectionCard, SplitPanel, Tag } from "../components";
import { loadScopeSummary } from "../data/loaders";
import { useAsyncData } from "../hooks/useAsyncData";

export default function ScopePage() {
  const state = useAsyncData(loadScopeSummary);

  if (state.loading) {
    return <EmptyState title="正在加载分析边界" message="正在读取范围与指标定义。" />;
  }

  if (state.error || !state.data) {
    return <EmptyState title="分析边界不可用" message={state.error ?? "当前还没有可展示的边界数据。"} />;
  }

  const highlights =
    state.data.pageHighlights ?? [
      {
        label: "纳入项",
        value: String(state.data.includedScope.length),
        detail: "当前报告纳入的范围条目数。",
      },
      {
        label: "排除项",
        value: String(state.data.excludedScope.length),
        detail: "当前报告排除的范围条目数。",
      },
      {
        label: "一级分类",
        value: String(state.data.taxonomy.level1Categories.length),
        detail: "一级分类数量。",
      },
      {
        label: "组件轴",
        value: String(state.data.taxonomy.componentAxis.length),
        detail: "组件轴数量。",
      },
    ];

  return (
    <>
      <PageHeader
        title="分析边界与指标定义"
        description="当前示例报告的范围边界、指标含义与分类体系。"
      />
      <section className="summary-band">
        {highlights.map((item) => (
          <MetricCard
            key={item.label}
            title={item.label}
            primaryValue={item.value}
            secondaryValue={item.detail}
          />
        ))}
      </section>
      <SplitPanel className="split-panel--balanced">
        <SectionCard title="纳入范围">
          <div className="tag-list">
            {state.data.includedScope.map((item) => (
              <Tag key={item}>{item}</Tag>
            ))}
          </div>
        </SectionCard>
        <SectionCard title="排除范围">
          <div className="tag-list">
            {state.data.excludedScope.map((item) => (
              <Tag key={item}>{item}</Tag>
            ))}
          </div>
        </SectionCard>
      </SplitPanel>
      <SectionCard title="计时边界流程图">
        <div className="scope-boundary-flow">
          <article className="scope-boundary-flow__layer scope-boundary-flow__layer--outer">
            <div className="scope-boundary-flow__title-row">
              <strong>Demo 总耗时</strong>
              <span>Client 提交 Job → Job 执行完成</span>
            </div>
            <p>最外层边界，用于回答从提交到完成的整体外部可见差异。</p>
            <article className="scope-boundary-flow__layer scope-boundary-flow__layer--middle">
              <div className="scope-boundary-flow__title-row">
                <strong>TM 端到端耗时</strong>
                <span>TaskManager 侧 SubTask 开始 → 结束</span>
              </div>
              <p>中间层边界，用于收敛到 TaskManager 侧的执行差异。</p>
              <article className="scope-boundary-flow__layer scope-boundary-flow__layer--inner">
                <div className="scope-boundary-flow__title-row">
                  <strong>Java PreUDF → Python UDF → Java PostUDF</strong>
                  <span>桥接与包装链路</span>
                </div>
                <div className="scope-boundary-flow__split">
                  <div className="scope-boundary-flow__metric">
                    <strong>业务算子耗时</strong>
                    <span>Python UDF Start → Python UDF End</span>
                  </div>
                  <div className="scope-boundary-flow__metric">
                    <strong>框架调用耗时</strong>
                    <span>Java End - Java Start - Python UDF Time</span>
                  </div>
                </div>
              </article>
            </article>
          </article>
        </div>
      </SectionCard>
      <SectionCard title="指标边界总览">
        <div className="signal-grid">
          {state.data.metrics.map((metric) => (
            <article key={metric.name} className="signal-card">
              <strong>{metric.name}</strong>
              <span>{metric.definition}</span>
              <span>边界：{metric.boundary}</span>
              <span>归一化：{metric.normalization}</span>
            </article>
          ))}
        </div>
      </SectionCard>
      <SectionCard title="指标定义明细">
        <DataTable
          columns={[
            { key: "name", header: "指标", render: (row) => row.name },
            { key: "definition", header: "定义", render: (row) => row.definition },
            { key: "boundary", header: "边界", render: (row) => row.boundary },
            { key: "normalization", header: "归一化口径", render: (row) => row.normalization },
          ]}
          rows={state.data.metrics}
          getRowKey={(row) => row.name}
        />
      </SectionCard>
      <SplitPanel className="split-panel--balanced">
        <SectionCard title="一级分类体系">
          <div className="tag-list">
            {state.data.taxonomy.level1Categories.map((item) => (
              <Tag key={item}>{item}</Tag>
            ))}
          </div>
        </SectionCard>
        <SectionCard title="组件轴与告警阈值">
          <div className="tag-list">
            {state.data.taxonomy.componentAxis.map((item) => (
              <Tag key={item}>{item}</Tag>
            ))}
          </div>
          <p className="scope-note">Unknown 告警阈值：{state.data.taxonomy.unknownWarningThreshold}</p>
        </SectionCard>
      </SplitPanel>
    </>
  );
}
