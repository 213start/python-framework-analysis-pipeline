import { DataTable, EmptyState, PageHeader, SectionCard, Tag } from "../components";
import { loadComponentDetail } from "../data/loaders";
import { useAsyncData } from "../hooks/useAsyncData";
import { Link, useParams } from "react-router-dom";

export default function ComponentDetailPage() {
  const { componentId = "cpython" } = useParams();
  const state = useAsyncData(() => loadComponentDetail(componentId), [componentId]);

  if (state.loading) {
    return <EmptyState title="正在加载组件详情" message="正在从报告数据包读取组件详情。" />;
  }

  if (state.error || !state.data) {
    return <EmptyState title="组件详情不可用" message={state.error ?? "当前还没有可展示的组件详情。"} />;
  }

  return (
    <>
      <PageHeader title={state.data.name} description="组件级耗时与关联分析。" />
      <SectionCard title="组件摘要">
        <p>组件 ID：<Tag>{state.data.id}</Tag></p>
        <p>Arm：{state.data.armTime}</p>
        <p>x86：{state.data.x86Time}</p>
        <p>差值：{state.data.delta}</p>
        {state.data.deltaContribution ? <p>差值贡献：{state.data.deltaContribution}</p> : null}
      </SectionCard>
      <SectionCard title="分类">
        <DataTable
          columns={[
            { key: "name", header: "分类", render: (row) => <Link to={`/category/${row.id}`}>{row.name}</Link> },
            { key: "delta", header: "差值", render: (row) => row.delta },
          ]}
          rows={state.data.categories}
          getRowKey={(row) => row.id}
        />
      </SectionCard>
      <SectionCard title="全量热点函数">
        <DataTable
          columns={[
            { key: "symbol", header: "函数", render: (row) => <Link to={`/function/${row.id}`}>{row.symbol}</Link> },
            { key: "category", header: "分类", render: (row) => row.category },
            { key: "selfArm", header: "Arm 自耗时", render: (row) => row.selfArm },
            { key: "selfX86", header: "x86 自耗时", render: (row) => row.selfX86 },
            { key: "totalArm", header: "Arm 总耗时", render: (row) => row.totalArm },
            { key: "totalX86", header: "x86 总耗时", render: (row) => row.totalX86 },
            { key: "armShare", header: "Arm 占比", render: (row) => row.armShare },
            { key: "x86Share", header: "x86 占比", render: (row) => row.x86Share },
            { key: "delta", header: "差值", render: (row) => row.delta },
            { key: "deltaContribution", header: "差值贡献", render: (row) => row.deltaContribution },
          ]}
          rows={state.data.hotspots}
          getRowKey={(row) => row.id}
        />
      </SectionCard>
      <SectionCard title="关联分析">
        <p>模式：{state.data.patternIds.map((id) => <Link key={id} to={`/pattern/${id}`}>{id} </Link>)}</p>
        <p>根因：{state.data.rootCauseIds.map((id) => <Link key={id} to={`/root-cause/${id}`}>{id} </Link>)}</p>
      </SectionCard>
      <SectionCard title="证据附件">
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
