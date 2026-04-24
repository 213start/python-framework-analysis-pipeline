import { DataTable, EmptyState, PageHeader, SectionCard, Tag } from "../components";
import { loadCategoryDetail } from "../data/loaders";
import { useAsyncData } from "../hooks/useAsyncData";
import { Link, useParams } from "react-router-dom";

export default function CategoryDetailPage() {
  const { categoryId = "memory" } = useParams();
  const state = useAsyncData(() => loadCategoryDetail(categoryId), [categoryId]);

  if (state.loading) {
    return <EmptyState title="正在加载分类详情" message="正在从报告数据包读取分类详情。" />;
  }

  if (state.error || !state.data) {
    return <EmptyState title="分类详情不可用" message={state.error ?? "当前还没有可展示的分类详情。"} />;
  }

  return (
    <>
      <PageHeader title={state.data.name} description="分类级耗时与关联证据。" />
      <SectionCard title="分类摘要">
        <p>分类 ID：<Tag>{state.data.id}</Tag></p>
        <p>层级：<Tag>{state.data.level}</Tag></p>
        <p>组件：{state.data.componentIds.map((id) => <Link key={id} to={`/component/${id}`}>{id} </Link>)}</p>
        <p>Arm：{state.data.armTime}</p>
        <p>x86：{state.data.x86Time}</p>
        <p>差值：{state.data.delta}</p>
        {state.data.deltaContribution ? <p>差值贡献：{state.data.deltaContribution}</p> : null}
      </SectionCard>
      <SectionCard title="关联证据">
        <p>用例：{state.data.caseIds.map((id) => <Link key={id} to={`/case/${id}`}>{id} </Link>)}</p>
        <p>模式：{state.data.patternIds.map((id) => <Link key={id} to={`/pattern/${id}`}>{id} </Link>)}</p>
      </SectionCard>
      <SectionCard title="全量热点函数">
        <DataTable
          columns={[
            { key: "symbol", header: "函数", render: (row) => <Link to={`/function/${row.id}`}>{row.symbol}</Link> },
            { key: "sourceFile", header: "来源", render: (row) => row.sourceFile || "—" },
            { key: "component", header: "组件", render: (row) => row.component },
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
