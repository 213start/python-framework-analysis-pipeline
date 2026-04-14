import { DataTable, EmptyState, PageHeader, SectionCard } from "../components";
import { loadStackOverview } from "../data/loaders";
import { useAsyncData } from "../hooks/useAsyncData";
import { Link } from "react-router-dom";

const STACK_SEGMENT_STYLES = [
  "linear-gradient(180deg, #d97b57 0%, #ab4e2c 100%)",
  "linear-gradient(180deg, #aa6c4c 0%, #7f4d35 100%)",
  "linear-gradient(180deg, #557c74 0%, #395d56 100%)",
  "linear-gradient(180deg, #6f7fa3 0%, #49587b 100%)",
  "linear-gradient(180deg, #8d7b54 0%, #67573b 100%)",
  "linear-gradient(180deg, #8a5d73 0%, #684457 100%)",
];

function parseMagnitude(value: string) {
  return Number.parseFloat(value.replace("%", "").replace("ms", "").trim()) || 0;
}

function renderStackBar(
  items: Array<{ id: string; name: string; value: string }>,
  platformLabel: string,
  total: string,
  linkPrefix: "component" | "category",
) {
  const totalValue = Math.max(parseMagnitude(total), 1);

  return (
    <div className="stack-panorama__platform">
      <div className="stack-panorama__platform-header">
        <strong>{platformLabel}</strong>
        <span>总耗时 {total}</span>
      </div>
      <div className="stack-panorama__bar" aria-label={`${platformLabel} 堆叠分布`}>
        {items.map((item) => {
          const width = `${Math.max((parseMagnitude(item.value) / totalValue) * 100, 4)}%`;
          return (
            <Link
              key={item.id}
              to={`/${linkPrefix}/${item.id}`}
              className="stack-panorama__segment"
              style={{
                width,
                background: STACK_SEGMENT_STYLES[items.indexOf(item) % STACK_SEGMENT_STYLES.length],
              }}
              title={`${item.name}: ${item.value}`}
            >
              <span>{item.name}</span>
            </Link>
          );
        })}
      </div>
    </div>
  );
}

function renderLegend(items: Array<{ id: string; name: string }>, linkPrefix: "component" | "category") {
  return (
    <div className="stack-panorama__legend">
      <strong>图例</strong>
      <div className="stack-panorama__legend-items">
        {items.map((item, index) => (
          <Link key={item.id} to={`/${linkPrefix}/${item.id}`} className="stack-panorama__legend-item">
            <span
              className="stack-panorama__legend-swatch"
              style={{ background: STACK_SEGMENT_STYLES[index % STACK_SEGMENT_STYLES.length] }}
              aria-hidden="true"
            />
            <span>{item.name}</span>
          </Link>
        ))}
      </div>
    </div>
  );
}

export default function ByStackPage() {
  const state = useAsyncData(loadStackOverview);

  if (state.loading) {
    return <EmptyState title="正在加载栈总览" message="正在读取组件与分类摘要。" />;
  }

  if (state.error || !state.data) {
    return <EmptyState title="栈总览不可用" message={state.error ?? "当前还没有可展示的栈级数据。"} />;
  }

  return (
    <>
      <PageHeader
        title="按栈分析总览"
        description="先看两个平台在组件和分类维度上的完整耗时分布，再下钻到热点函数与机器码差异。"
      />
      <SectionCard title="组件双平台耗时分布">
        <div className="stack-panorama">
          {renderStackBar(
            state.data.components.map((component) => ({
              id: component.id,
              name: component.name,
              value: component.armTime,
            })),
            "Arm",
            state.data.platformTotals.arm,
            "component",
          )}
          {renderStackBar(
            state.data.components.map((component) => ({
              id: component.id,
              name: component.name,
              value: component.x86Time,
            })),
            "x86",
            state.data.platformTotals.x86,
            "component",
          )}
          {renderLegend(state.data.components, "component")}
        </div>
      </SectionCard>
      <SectionCard title="分类双平台耗时分布">
        <div className="stack-panorama">
          {renderStackBar(
            state.data.categories.map((category) => ({
              id: category.id,
              name: category.name,
              value: category.armTime,
            })),
            "Arm",
            state.data.platformTotals.arm,
            "category",
          )}
          {renderStackBar(
            state.data.categories.map((category) => ({
              id: category.id,
              name: category.name,
              value: category.x86Time,
            })),
            "x86",
            state.data.platformTotals.x86,
            "category",
          )}
          {renderLegend(state.data.categories, "category")}
        </div>
      </SectionCard>
      <SectionCard title="组件明细">
        <DataTable
          columns={[
            { key: "name", header: "组件", render: (row) => <Link to={`/component/${row.id}`}>{row.name}</Link> },
            { key: "armTime", header: "Arm 耗时", render: (row) => row.armTime },
            { key: "x86Time", header: "x86 耗时", render: (row) => row.x86Time },
            { key: "armShare", header: "Arm 占比", render: (row) => row.armShare },
            { key: "x86Share", header: "x86 占比", render: (row) => row.x86Share },
            { key: "delta", header: "差值", render: (row) => row.delta },
            { key: "deltaContribution", header: "差值贡献", render: (row) => row.deltaContribution },
          ]}
          rows={state.data.components}
          getRowKey={(row) => row.id}
        />
      </SectionCard>
      <SectionCard title="分类明细">
        <DataTable
          columns={[
            { key: "name", header: "分类", render: (row) => <Link to={`/category/${row.id}`}>{row.name}</Link> },
            { key: "level", header: "层级", render: (row) => row.level },
            { key: "armTime", header: "Arm 耗时", render: (row) => row.armTime },
            { key: "x86Time", header: "x86 耗时", render: (row) => row.x86Time },
            { key: "armShare", header: "Arm 占比", render: (row) => row.armShare },
            { key: "x86Share", header: "x86 占比", render: (row) => row.x86Share },
            { key: "delta", header: "差值", render: (row) => row.delta },
            { key: "deltaContribution", header: "差值贡献", render: (row) => row.deltaContribution },
            { key: "top", header: "热点函数", render: (row) => row.topFunctionId ? <Link to={`/function/${row.topFunctionId}`}>{row.topFunction}</Link> : row.topFunction },
          ]}
          rows={state.data.categories}
          getRowKey={(row) => row.id}
        />
      </SectionCard>
    </>
  );
}
