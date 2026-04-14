import { MetricCard, PageHeader, SectionCard, SplitPanel, Tag } from "../components";
import { loadExecutiveSummary, loadOpportunityRanking } from "../data/loaders";
import { useAsyncData } from "../hooks/useAsyncData";
import { Link } from "react-router-dom";

function parseMagnitude(value: string) {
  const parsed = Number.parseFloat(value.replace("%", "").replace("ms", "").trim());
  return Number.isFinite(parsed) ? Math.abs(parsed) : 0;
}

export default function HomePage() {
  const summary = useAsyncData(loadExecutiveSummary);
  const opportunities = useAsyncData(loadOpportunityRanking);
  const metrics = summary.data?.metrics ?? [];
  const opportunityItems = opportunities.data ?? [];
  const summaryData = summary.data;

  return (
    <>
      <PageHeader
        title="执行摘要"
        description={summaryData?.subtitle ?? "当前报告数据包下 Arm 与 x86 差异的高层概览。"}
      />
      {!summary.loading && !summary.error && summaryData ? (
        <section className="executive-hero">
          <div className="executive-hero__content">
            <p className="eyebrow">跨平台摘要</p>
            <h3>Arm 仍落后于 x86，但差距主要集中在少数运行时主题上。</h3>
            <p>
              当前示例数据表明，对象 churn 和适配层驱动的解释器开销，是造成框架差距的主要来源。
            </p>
            <div className="tag-list">
              <Tag>管理层摘要</Tag>
              <Tag>工程下钻</Tag>
              <Tag>证据可追溯</Tag>
            </div>
          </div>
          <div className="executive-hero__aside">
            <div className="executive-callout">
              <p className="eyebrow">首要模式</p>
              <p className="executive-callout__title">{summaryData.topPattern}</p>
            </div>
            <div className="executive-callout">
              <p className="eyebrow">首要根因</p>
              <p className="executive-callout__title">{summaryData.topRootCause}</p>
            </div>
          </div>
        </section>
      ) : null}
      <SectionCard title="核心指标">
        {summary.loading ? (
          <p>正在加载摘要指标...</p>
        ) : summary.error ? (
          <p role="alert">无法加载摘要指标：{summary.error}</p>
        ) : metrics.length > 0 ? (
          <section className="metric-card-grid" aria-label="摘要指标">
            {metrics.map((metric) => (
              metric.target ? (
                <Link key={metric.label} to={metric.target}>
                  <MetricCard
                    title={metric.label}
                    primaryValue={`${metric.armValue} vs ${metric.x86Value}`}
                    secondaryValue={metric.delta}
                  />
                </Link>
              ) : (
                <MetricCard
                  key={metric.label}
                  title={metric.label}
                  primaryValue={`${metric.armValue} vs ${metric.x86Value}`}
                  secondaryValue={metric.delta}
                />
              )
            ))}
          </section>
        ) : (
          <p>当前还没有可展示的摘要指标。</p>
        )}
      </SectionCard>
      {!summary.loading && !summary.error && metrics.length > 0 ? (
        <SectionCard title="差异画像">
          <div className="delta-bar-list">
            {metrics.map((metric) => {
              const width = Math.min(100, Math.max(12, parseMagnitude(metric.delta) * 4));
              return (
                <div key={metric.label} className="delta-bar-list__row">
                  <div>
                    <strong>{metric.label}</strong>
                    <p>{metric.armValue} vs {metric.x86Value}</p>
                  </div>
                  <div className="delta-bar-list__bar-wrap">
                    <div className="delta-bar-list__bar" style={{ width: `${width}%` }} />
                    <span>{metric.delta}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </SectionCard>
      ) : null}
      <SplitPanel className="split-panel--balanced">
        <SectionCard title="决策信号">
          <ul className="highlight-list">
            <li>在当前示例包中，框架侧差异仍然高于业务算子侧差异。</li>
            <li>CPython 的内存路径和解释器路径是首选下钻入口。</li>
            <li>当前优化机会已经具备可增量验证的范围边界。</li>
          </ul>
        </SectionCard>
        <SectionCard title="推荐浏览路径">
          <ol className="highlight-list">
            <li>先看 <Link to="/analysis/by-case">按用例分析</Link>，识别哪些 workload 会放大差距。</li>
            <li>再切到 <Link to="/analysis/by-stack">按栈分析</Link>，确认由哪些运行时层主导。</li>
            <li>最后进入 <Link to="/insights">优化洞察</Link>，并打开证据页做双平台对照。</li>
          </ol>
        </SectionCard>
      </SplitPanel>
      <SectionCard title="优先优化机会">
        {opportunities.loading ? (
          <p>正在加载优化机会...</p>
        ) : opportunities.error ? (
          <p role="alert">无法加载优化机会：{opportunities.error}</p>
        ) : opportunityItems.length > 0 ? (
          <ul className="opportunity-list">
            {opportunityItems.map((item) => (
              <li key={item.id} className="opportunity-list__item">
                <div>
                  <strong>
                    {item.rootCauseId ? <Link to={`/root-cause/${item.rootCauseId}`}>{item.title}</Link> : item.title}
                  </strong>
                  <p>影响 {item.impact} · 成本 {item.effort}</p>
                </div>
                <Tag>预估收益 {item.estimatedGainPct}%</Tag>
              </li>
            ))}
          </ul>
        ) : (
          <p>当前还没有可展示的优化机会。</p>
        )}
      </SectionCard>
    </>
  );
}
