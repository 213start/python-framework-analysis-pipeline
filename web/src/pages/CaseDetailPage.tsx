import { DataTable, EmptyState, PageHeader, SectionCard, Tag } from "../components";
import { loadCaseDetail } from "../data/loaders";
import { useAsyncData } from "../hooks/useAsyncData";
import { Link, useParams } from "react-router-dom";

export default function CaseDetailPage() {
  const { caseId = "q01" } = useParams();
  const state = useAsyncData(() => loadCaseDetail(caseId), [caseId]);

  if (state.loading) {
    return <EmptyState title="正在加载用例详情" message="正在从报告数据包读取用例详情。" />;
  }

  if (state.error) {
    return <EmptyState title="用例详情不可用" message={state.error} />;
  }

  if (!state.data) {
    return <EmptyState title="用例详情不可用" message="当前还没有可展示的用例详情。" />;
  }

  const rows = [
    { id: "demo", label: "Demo 总耗时", ...state.data.metrics.demo },
    { id: "tm", label: "TM 端到端耗时", ...state.data.metrics.tm },
    { id: "operator", label: "业务算子耗时", ...state.data.metrics.operator },
    { id: "framework", label: "框架调用耗时", ...state.data.metrics.framework },
  ];

  return (
    <>
      <PageHeader
        title={state.data.name}
        description={state.data.semanticNotes}
      />
      <SectionCard title="用例摘要">
        <p>用例 ID：<Tag>{state.data.id}</Tag></p>
        <p>已知偏差：{state.data.knownDeviations.length}</p>
      </SectionCard>
      <SectionCard title="耗时拆解">
        <DataTable
          columns={[
            { key: "label", header: "指标", render: (row) => row.label },
            { key: "arm", header: "Arm 平台", render: (row) => row.arm },
            { key: "x86", header: "x86 平台", render: (row) => row.x86 },
            { key: "delta", header: "差值", render: (row) => row.delta },
          ]}
          rows={rows}
          getRowKey={(row) => row.id}
        />
      </SectionCard>
      <SectionCard title="热点函数">
        <DataTable
          columns={[
            { key: "symbol", header: "函数", render: (row) => <Link to={`/function/${row.id}`}>{row.symbol}</Link> },
            { key: "component", header: "组件", render: (row) => row.component },
            { key: "category", header: "分类", render: (row) => row.category },
            { key: "delta", header: "差值", render: (row) => row.delta },
          ]}
          rows={state.data.hotspots}
          getRowKey={(row) => row.id}
        />
      </SectionCard>
      <SectionCard title="关联分析">
        <div>
          <p>模式</p>
          <div className="tag-list">
            {state.data.patterns.map((patternId) => (
              <Link key={patternId} to={`/pattern/${patternId}`}>
                <Tag>{patternId}</Tag>
              </Link>
            ))}
          </div>
        </div>
        <div>
          <p>根因</p>
          <div className="tag-list">
            {state.data.rootCauses.map((rootCauseId) => (
              <Link key={rootCauseId} to={`/root-cause/${rootCauseId}`}>
                <Tag>{rootCauseId}</Tag>
              </Link>
            ))}
          </div>
        </div>
      </SectionCard>
      <SectionCard title="用例证据">
        <div className="tag-list">
          {state.data.artifactIds.map((artifactId) => (
            <Link key={artifactId} to={`/artifact/${artifactId}`}>
              <Tag>{artifactId}</Tag>
            </Link>
          ))}
        </div>
      </SectionCard>
    </>
  );
}
