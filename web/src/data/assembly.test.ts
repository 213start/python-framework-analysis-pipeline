import { afterEach, describe, expect, test, vi } from "vitest";
import {
  assembleExecutiveSummary,
  assembleScopeSummary,
  assembleCaseIndex,
  assembleCaseDetail,
  assembleComponentDetail,
  assembleCategoryDetail,
  assembleFunctionDetail,
  assembleArtifactDetail,
  assembleOpportunityRanking,
  assemblePatternIndex,
  assemblePatternDetail,
  assembleRootCauseIndex,
  assembleRootCauseDetail,
  assembleStackOverview,
  loadAssemblyContext,
} from "./assembly";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("four-layer source adapters", () => {
  test("loads a project bundle from four-layer JSON inputs", async () => {
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);

      if (url.endsWith("/projects/tpch-pyflink-reference.project.json")) {
        return Promise.resolve({
          ok: true,
          json: vi.fn().mockResolvedValue({
            id: "tpch-pyflink-reference",
            frameworkRef: "pyflink",
            datasetRef: "tpch-on-pyflink-2026q2",
            sourceRef: "pyflink-reference-source",
          }),
        });
      }

      if (url.endsWith("/frameworks/pyflink.framework.json")) {
        return Promise.resolve({
          ok: true,
          json: vi.fn().mockResolvedValue({
            id: "pyflink",
            name: "PyFlink",
            analysisScope: ["python-side framework overhead"],
          }),
        });
      }

      if (url.endsWith("/datasets/tpch-on-pyflink-2026q2.dataset.json")) {
        return Promise.resolve({
          ok: true,
          json: vi.fn().mockResolvedValue({
            id: "tpch-on-pyflink-2026q2",
            cases: [],
            stackOverview: {
              platformTotals: { arm: "0 ms", x86: "0 ms" },
              components: [],
              categories: [],
            },
          }),
        });
      }

      if (url.endsWith("/sources/pyflink-reference-source.source.json")) {
        return Promise.resolve({
          ok: true,
          json: vi.fn().mockResolvedValue({
            id: "pyflink-reference-source",
            artifactIndex: [],
            sourceAnchors: [],
          }),
        });
      }

      return Promise.resolve({
        ok: false,
        json: vi.fn(),
      });
    });

    vi.stubGlobal("fetch", fetchMock);

    const context = await loadAssemblyContext("tpch-pyflink-reference");

    expect(context.project.id).toBe("tpch-pyflink-reference");
    expect(context.framework.id).toBe("pyflink");
    expect(context.dataset.id).toBe("tpch-on-pyflink-2026q2");
    expect(context.source.id).toBe("pyflink-reference-source");
  });
});

describe("four-layer assemblers", () => {
  test("assembles executive summary and scope summary from framework and dataset", () => {
    const ctx = {
      project: { id: "tpch-pyflink-reference" },
      framework: {
        id: "pyflink",
        name: "PyFlink",
        analysisScope: ["python-side framework overhead"],
        excludedScope: ["business udf logic"],
        metricDefinitions: ["demo_total_time", "framework_call_time"],
        taxonomy: {
          categoriesL1: ["Interpreter", "Memory"],
          components: ["cpython", "glibc"],
        },
        metricGuardrails: {
          unknownWarnThreshold: "5%",
        },
      },
      dataset: {
        id: "tpch-on-pyflink-2026q2",
        cases: [{ id: "tpch-q01-pyflink", name: "TPC-H Q1" }],
        stackOverview: {
          platformTotals: { arm: "182.0 ms", x86: "129.0 ms" },
          components: [{ id: "cpython", name: "CPython", armTime: "96.0 ms", x86Time: "61.0 ms", armShare: "52.7%", x86Share: "47.3%", delta: "+35.0 ms", deltaContribution: "66.0%" }],
          categories: [{ id: "memory", name: "内存", level: "L1", armTime: "54.0 ms", x86Time: "31.0 ms", armShare: "29.7%", x86Share: "24.0%", delta: "+32.0 ms", deltaContribution: "43.4%", topFunctionId: "func_001" }],
        },
        patterns: [{ id: "pattern_001", title: "热点路径上存在高频临时对象抖动", summary: "", confidence: "高", caseIds: [], functionIds: [], rootCauseIds: [], artifactIds: [] }],
        rootCauses: [{ id: "rc_001", title: "当前行表示设计导致临时对象过多", summary: "", confidence: "高置信", patternIds: [], artifactIds: [], optimizationIdeas: [], validationPlan: [] }],
      },
      source: { id: "pyflink-reference-source" },
    } as never;

    expect(assembleExecutiveSummary(ctx).topPattern).toBe("热点路径上存在高频临时对象抖动");
    expect(assembleScopeSummary(ctx).taxonomy.unknownWarningThreshold).toBe("5%");
  });

  test("assembles case index entries from dataset cases and project bindings", () => {
    const result = assembleCaseIndex({
      project: {
        id: "tpch-pyflink-reference",
        caseBindings: [
          {
            caseId: "tpch-q01-pyflink",
            primaryArtifactIds: ["sql_q01", "pyflink_q01"],
          },
        ],
      },
      framework: {
        id: "pyflink",
      },
      dataset: {
        id: "tpch-on-pyflink-2026q2",
        cases: [
          {
            id: "tpch-q01-pyflink",
            legacyCaseId: "q01",
            name: "TPC-H Q1",
            implementationForm: "single-python-udf",
            semanticStatus: "verified",
            metrics: {
              demoDelta: "+8.2%",
              tmDelta: "+7.6%",
              operatorDelta: "+4.1%",
              frameworkDelta: "+12.3%",
            },
          },
        ],
      },
      source: {
        id: "pyflink-reference-source",
      },
    });

    expect(result).toEqual([
      {
        id: "tpch-q01-pyflink",
        name: "TPC-H Q1",
        demoDelta: "+8.2%",
        tmDelta: "+7.6%",
        operatorDelta: "+4.1%",
        frameworkDelta: "+12.3%",
        workloadForm: "single-python-udf",
        semanticStatus: "verified",
        sourceSqlArtifactId: "sql_q01",
        pythonUdfArtifactId: "pyflink_q01",
      },
    ]);
  });

  test("assembles stack overview using dataset functions to resolve category top functions", () => {
    const result = assembleStackOverview({
      project: {
        id: "tpch-pyflink-reference",
      },
      framework: {
        id: "pyflink",
      },
      dataset: {
        id: "tpch-on-pyflink-2026q2",
        stackOverview: {
          platformTotals: {
            arm: "182.0 ms",
            x86: "129.0 ms",
          },
          components: [
            {
              id: "cpython",
              name: "CPython",
              armTime: "96.0 ms",
              x86Time: "61.0 ms",
              armShare: "52.7%",
              x86Share: "47.3%",
              delta: "+35.0 ms",
              deltaContribution: "66.0%",
            },
          ],
          categories: [
            {
              id: "memory",
              name: "内存",
              level: "L1",
              armTime: "54.0 ms",
              x86Time: "31.0 ms",
              armShare: "29.7%",
              x86Share: "24.0%",
              delta: "+32.0 ms",
              deltaContribution: "43.4%",
              topFunctionId: "func_001",
            },
          ],
        },
        functions: [
          {
            id: "func_001",
            symbol: "_PyObject_Malloc",
          },
        ],
      },
      source: {
        id: "pyflink-reference-source",
      },
    });

    expect(result).toEqual({
      platformTotals: {
        arm: "182.0 ms",
        x86: "129.0 ms",
      },
      components: [
        {
          id: "cpython",
          name: "CPython",
          armTime: "96.0 ms",
          x86Time: "61.0 ms",
          armShare: "52.7%",
          x86Share: "47.3%",
          delta: "+35.0 ms",
          deltaContribution: "66.0%",
        },
      ],
      categories: [
        {
          id: "memory",
          name: "内存",
          level: "L1",
          armTime: "54.0 ms",
          x86Time: "31.0 ms",
          armShare: "29.7%",
          x86Share: "24.0%",
          delta: "+32.0 ms",
          deltaContribution: "43.4%",
          topFunction: "_PyObject_Malloc",
          topFunctionId: "func_001",
        },
      ],
    });
  });

  test("assembles case detail, indexes, and pattern/root-cause details from dataset data", () => {
    const ctx = {
      project: {
        id: "tpch-pyflink-reference",
        caseBindings: [
          {
            caseId: "tpch-q01-pyflink",
            primaryArtifactIds: ["sql_q01", "pyflink_q01"],
            notes: "Q1 主要绑定对象分配与行包装路径。",
          },
        ],
      },
      framework: { id: "pyflink" },
      dataset: {
        id: "tpch-on-pyflink-2026q2",
        cases: [
          {
            id: "tpch-q01-pyflink",
            legacyCaseId: "q01",
            name: "TPC-H Q1",
            metrics: {
              demo: { arm: "5.23 s", x86: "4.83 s", delta: "+8.2%" },
              tm: { arm: "4.18 s", x86: "3.88 s", delta: "+7.6%" },
              operator: { arm: "1.77 ms", x86: "1.69 ms", delta: "+4.1%" },
              framework: { arm: "0.91 ms", x86: "0.81 ms", delta: "+12.3%" },
            },
            patterns: ["pattern_001"],
            rootCauses: ["rc_001"],
          },
        ],
        functions: [
          {
            id: "func_001",
            symbol: "_PyObject_Malloc",
            component: "cpython",
            categoryL1: "内存",
            patternIds: ["pattern_001"],
            caseIds: ["tpch-q01-pyflink"],
            metrics: { delta: "+14.6 ms" },
          },
        ],
        patterns: [
          {
            id: "pattern_001",
            title: "热点路径上存在高频临时对象抖动",
            summary: "summary",
            confidence: "高",
            caseIds: ["tpch-q01-pyflink"],
            functionIds: ["func_001"],
            rootCauseIds: ["rc_001"],
            artifactIds: ["source_row_adapter_excerpt"],
          },
        ],
        rootCauses: [
          {
            id: "rc_001",
            title: "当前行表示设计导致临时对象过多",
            summary: "summary",
            confidence: "高置信",
            patternIds: ["pattern_001"],
            artifactIds: ["source_row_adapter_excerpt"],
            optimizationIdeas: ["减少临时包装对象创建"],
            validationPlan: ["对比改写前后的对象分配次数"],
          },
        ],
        opportunities: [
          {
            id: "opp_001",
            title: "减少临时包装对象创建",
            impact: "高",
            effort: "中",
            estimatedGainPct: 8,
            rootCauseId: "rc_001",
          },
        ],
      },
      source: { id: "pyflink-reference-source" },
    } as never;

    expect(assembleCaseDetail(ctx, "q01").artifactIds).toEqual(["sql_q01", "pyflink_q01"]);
    expect(assemblePatternIndex(ctx)).toEqual([{ id: "pattern_001", title: "热点路径上存在高频临时对象抖动", confidence: "高" }]);
    expect(assembleRootCauseIndex(ctx)).toEqual([{ id: "rc_001", title: "当前行表示设计导致临时对象过多", confidence: "高置信" }]);
    expect(assembleOpportunityRanking(ctx)[0].id).toBe("opp_001");
    expect(assemblePatternDetail(ctx, "pattern_001").summary).toBe("summary");
    expect(assembleRootCauseDetail(ctx, "rc_001").optimizationIdeas[0]).toBe("减少临时包装对象创建");
  });

  test("assembles category detail with full hotspot rows from four-layer dataset data", () => {
    const result = assembleCategoryDetail(
      {
        project: {
          id: "tpch-pyflink-reference",
        },
        framework: {
          id: "pyflink",
        },
        dataset: {
          id: "tpch-on-pyflink-2026q2",
          categoryDetails: [
            {
              id: "memory",
              name: "内存",
              level: "L1",
              componentIds: ["cpython"],
              armTime: "70.0 ms",
              x86Time: "38.0 ms",
              armShare: "29.7%",
              x86Share: "24.0%",
              delta: "+32.0 ms",
              deltaContribution: "43.4%",
              caseIds: ["tpch-q01-pyflink", "tpch-q12-pyflink"],
              hotspotIds: ["func_001"],
              patternIds: ["pattern_001"],
              artifactIds: ["asm_arm_func_001", "asm_x86_func_001"],
            },
          ],
          functions: [
            {
              id: "func_001",
              symbol: "_PyObject_Malloc",
              component: "cpython",
              metrics: {
                selfArm: "21.2 ms",
                selfX86: "10.4 ms",
                totalArm: "33.5 ms",
                totalX86: "18.9 ms",
                armShare: "11.6%",
                x86Share: "8.1%",
                delta: "+14.6 ms",
                deltaContribution: "27.5%",
              },
            },
          ],
        },
        source: {
          id: "pyflink-reference-source",
        },
      },
      "memory",
    );

    expect(result).toEqual({
      id: "memory",
      name: "内存",
      level: "L1",
      componentIds: ["cpython"],
      armTime: "70.0 ms",
      x86Time: "38.0 ms",
      armShare: "29.7%",
      x86Share: "24.0%",
      delta: "+32.0 ms",
      deltaContribution: "43.4%",
      caseIds: ["tpch-q01-pyflink", "tpch-q12-pyflink"],
      hotspots: [
        {
          id: "func_001",
          symbol: "_PyObject_Malloc",
          component: "cpython",
          selfArm: "21.2 ms",
          selfX86: "10.4 ms",
          totalArm: "33.5 ms",
          totalX86: "18.9 ms",
          armShare: "11.6%",
          x86Share: "8.1%",
          delta: "+14.6 ms",
          deltaContribution: "27.5%",
        },
      ],
      patternIds: ["pattern_001"],
      artifactIds: ["asm_arm_func_001", "asm_x86_func_001"],
    });
  });

  test("assembles function detail and enriches diff view with bound source anchors", () => {
    const result = assembleFunctionDetail(
      {
        project: {
          id: "tpch-pyflink-reference",
          functionBindings: [
            {
              functionId: "func_001",
              sourceAnchorIds: ["anchor_alloc_fastpath", "anchor_alloc_slowpath"],
              armArtifactIds: ["asm_arm_func_001"],
              x86ArtifactIds: ["asm_x86_func_001"],
            },
          ],
        },
        framework: {
          id: "pyflink",
        },
        dataset: {
          id: "tpch-on-pyflink-2026q2",
          functions: [
            {
              id: "func_001",
              symbol: "_PyObject_Malloc",
              component: "cpython",
              categoryL1: "内存",
              categoryL2: "alloc_free",
              caseIds: ["tpch-q01-pyflink"],
              patternIds: ["pattern_001"],
              artifactIds: ["source_row_adapter_excerpt"],
              metrics: {
                selfArm: "21.2 ms",
                selfX86: "10.4 ms",
                totalArm: "33.5 ms",
                totalX86: "18.9 ms",
                delta: "+14.6 ms",
              },
              callPath: [
                "pyflink.operator.invoke",
                "_PyEval_EvalFrameDefault",
                "_PyObject_Malloc",
              ],
              diffView: {
                functionId: "func_001",
                sourceFile: "Objects/obmalloc.c",
                sourceLocation: "Objects/obmalloc.c:1421-1432",
                diffGuide: "按逻辑分析块对齐。",
                analysisBlocks: [
                  {
                    id: "blk_001",
                    label: "Fast-Path Alloc",
                    summary: "对齐 freelist 探测与慢路径回退。",
                    patternTag: "ObjectManipulator",
                    mappingType: "一对多",
                    sourceAnchors: [],
                    armRegions: [
                      {
                        id: "arm_001",
                        label: "Freeblock Load",
                        location: "0x0000-0x0008",
                        role: "Fast Path",
                        snippet: "ldr x9, [x0, #16]\ncbz x9, slow_path",
                        highlights: ["ldr", "cbz"],
                        defaultExpanded: true,
                      },
                    ],
                    x86Regions: [
                      {
                        id: "x86_001",
                        label: "Freeblock Load",
                        location: "0x0000-0x0006",
                        role: "Fast Path",
                        snippet: "mov rax, qword ptr [rdi+16]\ntest rax, rax",
                        highlights: ["mov", "test"],
                        defaultExpanded: true,
                      },
                    ],
                    mappings: [],
                    diffSignals: ["额外加载链"],
                    alignmentNote: "按 fast path block 对齐",
                    performanceNote: "Arm 依赖链更长。",
                    defaultExpanded: true,
                  },
                ],
              },
            },
          ],
        },
        source: {
          id: "pyflink-reference-source",
          sourceAnchors: [
            {
              id: "anchor_alloc_fastpath",
              label: "Freeblock 判空",
              location: "Objects/obmalloc.c:1421",
              role: "Fast Path",
              snippet: "if (pool->freeblock != NULL) {",
            },
            {
              id: "anchor_alloc_slowpath",
              label: "Slow Path 回退",
              location: "Objects/obmalloc.c:1432",
              role: "Slow Path",
              snippet: "goto allocate_from_new_pool(pool);",
            },
          ],
        },
      },
      "func_001",
    );

    expect(result.id).toBe("func_001");
    expect(result.artifactIds).toEqual([
      "source_row_adapter_excerpt",
      "asm_arm_func_001",
      "asm_x86_func_001",
    ]);
    expect(result.diffView?.analysisBlocks[0].sourceAnchors).toEqual([
      {
        id: "anchor_alloc_fastpath",
        label: "Freeblock 判空",
        role: "Fast Path",
        location: "Objects/obmalloc.c:1421",
        snippet: "if (pool->freeblock != NULL) {",
        defaultExpanded: true,
      },
      {
        id: "anchor_alloc_slowpath",
        label: "Slow Path 回退",
        role: "Slow Path",
        location: "Objects/obmalloc.c:1432",
        snippet: "goto allocate_from_new_pool(pool);",
        defaultExpanded: false,
      },
    ]);
  });

  test("assembles component detail with category summary and full hotspot rows", () => {
    const result = assembleComponentDetail(
      {
        project: {
          id: "tpch-pyflink-reference",
        },
        framework: {
          id: "pyflink",
        },
        dataset: {
          id: "tpch-on-pyflink-2026q2",
          componentDetails: [
            {
              id: "cpython",
              name: "CPython",
              armTime: "180.0 ms",
              x86Time: "110.0 ms",
              armShare: "52.7%",
              x86Share: "47.3%",
              delta: "+70.0 ms",
              deltaContribution: "66.0%",
              categories: [
                { id: "memory", name: "内存", delta: "+32.0 ms" },
                { id: "interpreter", name: "解释器", delta: "+18.0 ms" },
              ],
              hotspotIds: ["func_001", "func_002"],
              patternIds: ["pattern_001", "pattern_002"],
              rootCauseIds: ["rc_001", "rc_002"],
              artifactIds: ["asm_arm_func_001", "asm_x86_func_001", "source_interpreter_loop_excerpt"],
            },
          ],
          functions: [
            {
              id: "func_001",
              symbol: "_PyObject_Malloc",
              categoryL1: "内存",
              metrics: {
                selfArm: "21.2 ms",
                selfX86: "10.4 ms",
                totalArm: "33.5 ms",
                totalX86: "18.9 ms",
                armShare: "11.6%",
                x86Share: "8.1%",
                delta: "+14.6 ms",
                deltaContribution: "27.5%",
              },
            },
            {
              id: "func_002",
              symbol: "_PyEval_EvalFrameDefault",
              categoryL1: "解释器",
              metrics: {
                selfArm: "19.8 ms",
                selfX86: "13.7 ms",
                totalArm: "28.1 ms",
                totalX86: "21.5 ms",
                armShare: "10.9%",
                x86Share: "10.6%",
                delta: "+6.6 ms",
                deltaContribution: "12.5%",
              },
            },
          ],
        },
        source: {
          id: "pyflink-reference-source",
        },
      },
      "cpython",
    );

    expect(result).toEqual({
      id: "cpython",
      name: "CPython",
      armTime: "180.0 ms",
      x86Time: "110.0 ms",
      armShare: "52.7%",
      x86Share: "47.3%",
      delta: "+70.0 ms",
      deltaContribution: "66.0%",
      categories: [
        { id: "memory", name: "内存", delta: "+32.0 ms" },
        { id: "interpreter", name: "解释器", delta: "+18.0 ms" },
      ],
      hotspots: [
        {
          id: "func_001",
          symbol: "_PyObject_Malloc",
          category: "内存",
          selfArm: "21.2 ms",
          selfX86: "10.4 ms",
          totalArm: "33.5 ms",
          totalX86: "18.9 ms",
          armShare: "11.6%",
          x86Share: "8.1%",
          delta: "+14.6 ms",
          deltaContribution: "27.5%",
        },
        {
          id: "func_002",
          symbol: "_PyEval_EvalFrameDefault",
          category: "解释器",
          selfArm: "19.8 ms",
          selfX86: "13.7 ms",
          totalArm: "28.1 ms",
          totalX86: "21.5 ms",
          armShare: "10.9%",
          x86Share: "10.6%",
          delta: "+6.6 ms",
          deltaContribution: "12.5%",
        },
      ],
      patternIds: ["pattern_001", "pattern_002"],
      rootCauseIds: ["rc_001", "rc_002"],
      artifactIds: ["asm_arm_func_001", "asm_x86_func_001", "source_interpreter_loop_excerpt"],
    });
  });

  test("assembles artifact detail from source artifact index and file content", async () => {
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);

      if (url.endsWith("/raw/source_row_adapter_excerpt.txt")) {
        return Promise.resolve({
          ok: true,
          text: vi.fn().mockResolvedValue("row adapter excerpt content"),
        });
      }

      return Promise.resolve({
        ok: false,
        text: vi.fn(),
      });
    });

    vi.stubGlobal("fetch", fetchMock);

    const result = await assembleArtifactDetail(
      {
        project: {
          id: "tpch-pyflink-reference",
        },
        framework: {
          id: "pyflink",
        },
        dataset: {
          id: "tpch-on-pyflink-2026q2",
        },
        source: {
          id: "pyflink-reference-source",
          artifactIndex: [
            {
              id: "source_row_adapter_excerpt",
              title: "行适配器源码摘录",
              type: "source",
              description: "展示包装对象分配与行转换层级的源码摘录。",
              path: "/raw/source_row_adapter_excerpt.txt",
              contentType: "text/plain",
            },
          ],
        },
      },
      "source_row_adapter_excerpt",
    );

    expect(result).toEqual({
      id: "source_row_adapter_excerpt",
      title: "行适配器源码摘录",
      type: "source",
      description: "展示包装对象分配与行转换层级的源码摘录。",
      path: "/raw/source_row_adapter_excerpt.txt",
      contentType: "text/plain",
      content: "row adapter excerpt content",
    });
  });
});
