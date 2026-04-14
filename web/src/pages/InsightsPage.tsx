import { DataTable, EmptyState, MetricCard, PageHeader, SectionCard, SplitPanel, Tag } from "../components";
import { loadOpportunityRanking, loadPatternIndex, loadRootCauseIndex } from "../data/loaders";
import { useAsyncData } from "../hooks/useAsyncData";
import { Link } from "react-router-dom";

export default function InsightsPage() {
  const opportunities = useAsyncData(loadOpportunityRanking);
  const patterns = useAsyncData(loadPatternIndex);
  const rootCauses = useAsyncData(loadRootCauseIndex);

  if (opportunities.loading || rootCauses.loading || patterns.loading) {
    return <EmptyState title="正在加载优化洞察" message="正在读取优化机会与根因摘要。" />;
  }

  if (opportunities.error || rootCauses.error || patterns.error) {
    return (
        <EmptyState
        title="优化洞察不可用"
        message={opportunities.error ?? rootCauses.error ?? patterns.error ?? "当前还没有可展示的优化洞察数据。"}
      />
    );
  }

  const rootCauseRows = rootCauses.data ?? [];
  const patternRows = patterns.data ?? [];
  const opportunityRows = opportunities.data ?? [];

  return (
    <>
      <PageHeader
        title="优化机会与验证路线图"
        description="将优化机会映射到当前示例根因的收口页面。"
      />
      <section className="summary-band">
        <MetricCard title="报告中的模式" primaryValue={String(patternRows.length)} secondaryValue="已进入证据链" />
        <MetricCard title="报告中的根因" primaryValue={String(rootCauseRows.length)} secondaryValue="当前归纳层" />
        <MetricCard title="优化机会" primaryValue={String(opportunityRows.length)} secondaryValue="下一步优先动作" />
      </section>
      <SplitPanel className="split-panel--balanced">
        <SectionCard title="证据链">
          <div className="evidence-flow">
            <div className="evidence-flow__column">
              <p className="eyebrow">模式</p>
              {patternRows.map((pattern) => (
                <Link key={pattern.id} to={`/pattern/${pattern.id}`} className="signal-card">
                  <strong>{pattern.title}</strong>
                  <span>{pattern.confidence}</span>
                </Link>
              ))}
            </div>
            <div className="evidence-flow__arrow">→</div>
            <div className="evidence-flow__column">
              <p className="eyebrow">根因</p>
              {rootCauseRows.map((rootCause) => (
                <Link key={rootCause.id} to={`/root-cause/${rootCause.id}`} className="signal-card">
                  <strong>{rootCause.title}</strong>
                  <span>{rootCause.confidence}</span>
                </Link>
              ))}
            </div>
            <div className="evidence-flow__arrow">→</div>
            <div className="evidence-flow__column">
              <p className="eyebrow">机会</p>
              {opportunityRows.map((opportunity) => (
                <div key={opportunity.id} className="signal-card">
                  <strong>{opportunity.title}</strong>
                  <span>预估收益 {opportunity.estimatedGainPct}%</span>
                </div>
              ))}
            </div>
          </div>
        </SectionCard>
        <SectionCard title="叙事摘要">
          <ul className="highlight-list">
            <li>模式把跨函数、跨源码位置重复出现的行为显性化。</li>
            <li>根因将这些模式收敛为与平台差异相关的解释。</li>
            <li>优化机会再把解释转成可执行的验证路线图。</li>
          </ul>
        </SectionCard>
      </SplitPanel>
      <SectionCard title="优化机会">
        <DataTable
          columns={[
            { key: "title", header: "机会", render: (row) => row.rootCauseId ? <Link to={`/root-cause/${row.rootCauseId}`}>{row.title}</Link> : row.title },
            { key: "impact", header: "影响", render: (row) => row.impact },
            { key: "effort", header: "成本", render: (row) => row.effort },
            { key: "gain", header: "预估收益", render: (row) => `${row.estimatedGainPct}%` },
          ]}
          rows={opportunityRows}
          getRowKey={(row) => row.id}
        />
      </SectionCard>
      <SectionCard title="模式总览">
        <div className="tag-list">
          {patternRows.map((pattern) => (
            <Link key={pattern.id} to={`/pattern/${pattern.id}`}>
              <Tag>{pattern.title}</Tag>
            </Link>
          ))}
        </div>
      </SectionCard>
      <SectionCard title="根因总览">
        <DataTable
          columns={[
            { key: "title", header: "根因", render: (row) => <Link to={`/root-cause/${row.id}`}>{row.title}</Link> },
            { key: "confidence", header: "置信度", render: (row) => row.confidence },
          ]}
          rows={rootCauseRows}
          getRowKey={(row) => row.id}
        />
      </SectionCard>
    </>
  );
}
