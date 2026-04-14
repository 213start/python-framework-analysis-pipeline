import { EmptyState, PageHeader, SectionCard, SplitPanel, Tag } from "../components";
import { loadArtifactDetail } from "../data/loaders";
import { useAsyncData } from "../hooks/useAsyncData";
import { useParams } from "react-router-dom";

function getComparisonArtifactId(artifactId: string) {
  if (artifactId.startsWith("asm_arm_")) {
    return artifactId.replace("asm_arm_", "asm_x86_");
  }

  if (artifactId.startsWith("asm_x86_")) {
    return artifactId.replace("asm_x86_", "asm_arm_");
  }

  return null;
}

export default function ArtifactDetailPage() {
  const { artifactId = "asm_arm_func_001" } = useParams();
  const state = useAsyncData(() => loadArtifactDetail(artifactId), [artifactId]);
  const comparisonArtifactId = getComparisonArtifactId(artifactId);
  const comparison = useAsyncData(
    () => (comparisonArtifactId ? loadArtifactDetail(comparisonArtifactId) : Promise.resolve(null)),
    [comparisonArtifactId],
  );

  if (state.loading) {
    return <EmptyState title="正在加载证据详情" message="正在读取证据附件内容。" />;
  }

  if (state.error || !state.data) {
    return <EmptyState title="证据详情不可用" message={state.error ?? "当前还没有可展示的证据详情。"} />;
  }

  return (
    <>
      <PageHeader title={state.data.title} description={state.data.description} />
      <SectionCard title="证据元数据">
        <p>证据 ID：<Tag>{state.data.id}</Tag></p>
        <p>类型：<Tag>{state.data.type}</Tag></p>
        <p>内容类型：<Tag>{state.data.contentType}</Tag></p>
        <p>路径：<code>{state.data.path}</code></p>
      </SectionCard>
      <SectionCard title="证据内容">
        <pre className="artifact-content">
          <code>{state.data.content}</code>
        </pre>
      </SectionCard>
      {comparisonArtifactId && comparison.data ? (
        <SectionCard title="跨平台对比">
          <SplitPanel className="split-panel--compare">
            <div className="artifact-compare">
              <p className="eyebrow">{state.data.id}</p>
              <pre className="artifact-content">
                <code>{state.data.content}</code>
              </pre>
            </div>
            <div className="artifact-compare">
              <p className="eyebrow">{comparison.data.id}</p>
              <pre className="artifact-content">
                <code>{comparison.data.content}</code>
              </pre>
            </div>
          </SplitPanel>
        </SectionCard>
      ) : null}
    </>
  );
}
