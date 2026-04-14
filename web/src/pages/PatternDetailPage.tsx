import { DataTable, EmptyState, PageHeader, SectionCard, Tag } from "../components";
import { loadPatternDetail } from "../data/loaders";
import { useAsyncData } from "../hooks/useAsyncData";
import { Link, useParams } from "react-router-dom";

export default function PatternDetailPage() {
  const { patternId = "pattern_001" } = useParams();
  const state = useAsyncData(() => loadPatternDetail(patternId), [patternId]);

  if (state.loading) {
    return <EmptyState title="正在加载模式详情" message="正在从报告数据包读取模式详情。" />;
  }

  if (state.error) {
    return <EmptyState title="模式详情不可用" message={state.error} />;
  }

  if (!state.data) {
    return <EmptyState title="模式详情不可用" message="当前还没有可展示的模式详情。" />;
  }

  return (
    <>
      <PageHeader
        title={state.data.title}
        description={state.data.summary}
      />
      <SectionCard title="模式摘要">
        <p>模式 ID：<Tag>{state.data.id}</Tag></p>
        <p>置信度：<Tag>{state.data.confidence}</Tag></p>
      </SectionCard>
      <SectionCard title="支撑证据">
        <DataTable
          columns={[
            { key: "kind", header: "类型", render: (row) => row.kind },
            { key: "value", header: "内容", render: (row) => row.value },
          ]}
          rows={[
            {
              id: "cases",
              kind: "用例",
              value: state.data.caseIds.map((caseId) => (
                <Link key={caseId} to={`/case/${caseId}`}>{caseId}</Link>
              )),
            },
            {
              id: "functions",
              kind: "函数",
              value: state.data.functionIds.map((functionId) => (
                <Link key={functionId} to={`/function/${functionId}`}>{functionId}</Link>
              )),
            },
            {
              id: "root",
              kind: "根因",
              value: state.data.rootCauseIds.map((rootCauseId) => (
                <Link key={rootCauseId} to={`/root-cause/${rootCauseId}`}>{rootCauseId}</Link>
              )),
            },
          ]}
          getRowKey={(row) => row.id}
        />
      </SectionCard>
      <SectionCard title="代表性证据">
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
