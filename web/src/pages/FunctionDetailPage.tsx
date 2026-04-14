import { useState } from "react";
import { DataTable, EmptyState, PageHeader, SectionCard, Tag } from "../components";
import { loadFunctionDetail } from "../data/loaders";
import { useAsyncData } from "../hooks/useAsyncData";
import { Link, useParams } from "react-router-dom";
import type { FunctionDetail } from "../types/report";

function extractOpcode(value: string) {
  return value.trim().split(/\s+/)[0] ?? value;
}

function AnchorAsmColumn({
  title,
  regions,
}: {
  title: string;
  regions: NonNullable<FunctionDetail["diffView"]>["analysisBlocks"][number]["armRegions"];
}) {
  return (
    <div className="anchor-asm-column">
      <h6>{title}</h6>
      {regions.length > 0 ? (
        <div className="anchor-asm-list">
          {regions.map((region) => {
            const opcodes = Array.from(new Set(region.highlights.map(extractOpcode)));
            return (
              <article key={region.id} className="diff-snippet-card">
                <div className="diff-snippet-card__meta">
                  <div>
                    <strong>{region.label}</strong>
                    <span>{region.location}</span>
                  </div>
                </div>
                {opcodes.length > 0 ? (
                  <div className="diff-opcode-list">
                    {opcodes.map((opcode) => (
                      <Tag key={opcode}>{opcode}</Tag>
                    ))}
                  </div>
                ) : null}
                <pre className="artifact-content">
                  <code>{region.snippet}</code>
                </pre>
              </article>
            );
          })}
        </div>
      ) : (
        <p className="diff-empty-note">无直接对应</p>
      )}
    </div>
  );
}

export default function FunctionDetailPage() {
  const { functionId = "func_001" } = useParams();
  const state = useAsyncData(() => loadFunctionDetail(functionId), [functionId]);
  const [expandedBlocks, setExpandedBlocks] = useState<Record<string, boolean>>({});
  const [expandedAnchors, setExpandedAnchors] = useState<Record<string, boolean>>({});

  if (state.loading) {
    return <EmptyState title="正在加载函数详情" message="正在从报告数据包读取函数详情。" />;
  }

  if (state.error) {
    return <EmptyState title="函数详情不可用" message={state.error} />;
  }

  if (!state.data) {
    return <EmptyState title="函数详情不可用" message="当前还没有可展示的函数详情。" />;
  }

  const metricRows = [
    { id: "self-arm", label: "Arm 自耗时", value: state.data.metrics.selfArm },
    { id: "self-x86", label: "x86 自耗时", value: state.data.metrics.selfX86 },
    { id: "total-arm", label: "Arm 总耗时", value: state.data.metrics.totalArm },
    { id: "total-x86", label: "x86 总耗时", value: state.data.metrics.totalX86 },
    { id: "delta", label: "差值", value: state.data.metrics.delta },
  ];

  return (
    <>
      <PageHeader title={state.data.symbol} description="函数级耗时与贡献拆解。" />
      <SectionCard title="函数标识">
        <p>
          组件：<Tag>{state.data.component}</Tag>
        </p>
        <p>
          分类：<Tag>{state.data.categoryL1}</Tag> <Tag>{state.data.categoryL2}</Tag>
        </p>
      </SectionCard>
      <SectionCard title="函数指标">
        <DataTable
          columns={[
            { key: "label", header: "指标", render: (row) => row.label },
            { key: "value", header: "数值", render: (row) => row.value },
          ]}
          rows={metricRows}
          getRowKey={(row) => row.id}
        />
      </SectionCard>
      <SectionCard title="调用路径">
        <ol>
          {state.data.callPath.map((entry) => (
            <li key={entry}>{entry}</li>
          ))}
        </ol>
      </SectionCard>
      {state.data.diffView ? (
        <SectionCard title="源码对齐机器码差异">
          <div className="diff-source-meta">
            <p>
              源码文件：<code>{state.data.diffView.sourceFile}</code>
            </p>
            <p>
              源码区间：<code>{state.data.diffView.sourceLocation}</code>
            </p>
          </div>
          <div className="diff-guide">
            <p className="eyebrow">非线性映射说明</p>
            <p>{state.data.diffView.diffGuide}</p>
            <div className="diff-guide__legend">
              <p><strong>模式：</strong>说明该分析块的行为模式。</p>
              <p><strong>关注指令：</strong>只显示关键 opcode，不展示参数。</p>
            </div>
          </div>
          <div className="diff-block-list">
            {state.data.diffView.analysisBlocks.map((block) => {
              const isExpanded = expandedBlocks[block.id] ?? block.defaultExpanded;
              const patternTag = block.patternTag ?? block.mappingType;
              return (
                <article key={block.id} className="diff-block">
                  <div className="diff-block__header">
                    <div className="diff-block__meta">
                      <div className="diff-block__title-row">
                        <h4 className="diff-block__title">{block.label}</h4>
                        <Tag>模式：{patternTag}</Tag>
                      </div>
                      <p>{block.summary}</p>
                    </div>
                    <button
                      type="button"
                      className="diff-toggle"
                      onClick={() =>
                        setExpandedBlocks((current) => ({
                          ...current,
                          [block.id]: !isExpanded,
                        }))
                      }
                    >
                      {isExpanded ? "收起分析块" : "展开分析块"}
                    </button>
                  </div>
                  {isExpanded ? (
                    <div className="diff-anchor-list">
                      <p className="eyebrow">源码锚点</p>
                      {block.sourceAnchors.map((anchor) => {
                        const relatedMappings = (block.mappings ?? []).filter((mapping) =>
                          mapping.sourceAnchorIds.includes(anchor.id),
                        );
                        const armRegionIds = new Set(relatedMappings.flatMap((mapping) => mapping.armRegionIds));
                        const x86RegionIds = new Set(relatedMappings.flatMap((mapping) => mapping.x86RegionIds));
                        const armRegions = block.armRegions.filter((region) => armRegionIds.has(region.id));
                        const x86Regions = block.x86Regions.filter((region) => x86RegionIds.has(region.id));
                        const isAnchorExpanded = expandedAnchors[anchor.id] ?? anchor.defaultExpanded;

                        return (
                          <article key={anchor.id} className="diff-anchor-card">
                            <div className="diff-anchor-card__header">
                              <div className="diff-anchor-card__meta">
                                <div className="diff-anchor-card__title-row">
                                  <h5 className="diff-anchor-card__title">{anchor.label}</h5>
                                  {anchor.role ? <Tag>{anchor.role}</Tag> : null}
                                </div>
                                <span>{anchor.location}</span>
                              </div>
                              <button
                                type="button"
                                className="diff-toggle"
                                onClick={() =>
                                  setExpandedAnchors((current) => ({
                                    ...current,
                                    [anchor.id]: !isAnchorExpanded,
                                  }))
                                }
                              >
                                {isAnchorExpanded ? `收起源码锚点：${anchor.label}` : `展开源码锚点：${anchor.label}`}
                              </button>
                            </div>
                            <pre className="artifact-content">
                              <code>{anchor.snippet}</code>
                            </pre>
                            {isAnchorExpanded ? (
                              <div className="anchor-asm-grid">
                                <AnchorAsmColumn title="Arm 机器码" regions={armRegions} />
                                <AnchorAsmColumn title="x86 机器码" regions={x86Regions} />
                              </div>
                            ) : null}
                          </article>
                        );
                      })}
                    </div>
                  ) : null}
                </article>
              );
            })}
          </div>
        </SectionCard>
      ) : null}
      <SectionCard title="关联用例">
        <div className="tag-list">
          {state.data.caseIds.map((caseId) => (
            <Link key={caseId} to={`/case/${caseId}`}>
              <Tag>{caseId}</Tag>
            </Link>
          ))}
        </div>
      </SectionCard>
      <SectionCard title="关联模式">
        <div className="tag-list">
          {state.data.patternIds.map((patternId) => (
            <Link key={patternId} to={`/pattern/${patternId}`}>
              <Tag>{patternId}</Tag>
            </Link>
          ))}
        </div>
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
