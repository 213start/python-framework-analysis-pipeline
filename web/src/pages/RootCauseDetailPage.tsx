import { EmptyState, PageHeader, SectionCard, Tag } from "../components";
import { loadRootCauseDetail } from "../data/loaders";
import { useAsyncData } from "../hooks/useAsyncData";
import { Link, useParams } from "react-router-dom";

export default function RootCauseDetailPage() {
  const { rootCauseId = "rc_001" } = useParams();
  const state = useAsyncData(() => loadRootCauseDetail(rootCauseId), [rootCauseId]);

  if (state.loading) {
    return <EmptyState title="正在加载根因详情" message="正在从报告数据包读取根因详情。" />;
  }

  if (state.error) {
    return <EmptyState title="根因详情不可用" message={state.error} />;
  }

  if (!state.data) {
    return <EmptyState title="根因详情不可用" message="当前还没有可展示的根因详情。" />;
  }

  return (
    <>
      <PageHeader
        title={state.data.title}
        description={state.data.summary}
      />
      <SectionCard title="根因摘要">
        <p>置信度：<Tag>{state.data.confidence}</Tag></p>
        <div>
          <p>关联模式</p>
          <div className="tag-list">
            {state.data.patternIds.map((patternId) => (
              <Link key={patternId} to={`/pattern/${patternId}`}>
                <Tag>{patternId}</Tag>
              </Link>
            ))}
          </div>
        </div>
      </SectionCard>
      <SectionCard title="支撑证据">
        <div className="tag-list">
          {state.data.artifactIds.map((artifactId) => (
            <Link key={artifactId} to={`/artifact/${artifactId}`}>
              <Tag>{artifactId}</Tag>
            </Link>
          ))}
        </div>
      </SectionCard>
      <SectionCard title="优化思路">
        <ul>
          {state.data.optimizationIdeas.map((idea) => (
            <li key={idea}>{idea}</li>
          ))}
        </ul>
      </SectionCard>
      <SectionCard title="验证计划">
        <ul>
          {state.data.validationPlan.map((note) => (
            <li key={note}>{note}</li>
          ))}
        </ul>
      </SectionCard>
    </>
  );
}
