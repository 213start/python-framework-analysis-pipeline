import { afterEach, describe, expect, test, vi } from "vitest";
import { loadExecutiveSummary, loadFunctionDetail, loadOpportunityRanking, loadStackOverview } from "./loaders";
import * as assembly from "./assembly";

vi.mock("./assembly", async () => {
  const actual = await vi.importActual<typeof import("./assembly")>("./assembly");
  return {
    ...actual,
    loadAssemblyContext: vi.fn(),
    assembleExecutiveSummary: vi.fn(),
    assembleFunctionDetail: vi.fn(),
    assembleOpportunityRanking: vi.fn(),
    assembleStackOverview: vi.fn(),
  };
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.clearAllMocks();
});

describe("report data paths", () => {
  test("loads all page data through the four-layer assembly context", async () => {
    vi.mocked(assembly.loadAssemblyContext).mockResolvedValue({
      project: { id: "tpch-pyflink-reference" },
      framework: { id: "pyflink" },
      dataset: { id: "tpch-on-pyflink-2026q2" },
      source: { id: "pyflink-reference-source" },
    } as Awaited<ReturnType<typeof assembly.loadAssemblyContext>>);
    vi.mocked(assembly.assembleExecutiveSummary).mockReturnValue({
      title: "执行摘要",
      subtitle: "四层模型",
      metrics: [],
      topPattern: "Pattern",
      topRootCause: "Root Cause",
    });
    vi.mocked(assembly.assembleOpportunityRanking).mockReturnValue([
      {
        id: "opp_001",
        title: "优化机会",
        impact: "高",
        effort: "中",
        estimatedGainPct: 8,
      },
    ]);
    vi.mocked(assembly.assembleFunctionDetail).mockReturnValue({
      id: "func_001",
      symbol: "_PyObject_Malloc",
      component: "cpython",
      categoryL1: "内存",
      categoryL2: "alloc_free",
      caseIds: [],
      artifactIds: [],
      metrics: {
        selfArm: "1 ms",
        selfX86: "1 ms",
        totalArm: "1 ms",
        totalX86: "1 ms",
        delta: "+0 ms",
      },
      callPath: [],
      patternIds: [],
    });
    vi.mocked(assembly.assembleStackOverview).mockReturnValue({
      platformTotals: { arm: "1 ms", x86: "2 ms" },
      components: [],
      categories: [],
    });
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    await expect(loadExecutiveSummary()).resolves.toMatchObject({ subtitle: "四层模型" });
    await expect(loadOpportunityRanking()).resolves.toHaveLength(1);
    await expect(loadFunctionDetail("func_001")).resolves.toMatchObject({ id: "func_001" });
    await expect(loadStackOverview()).resolves.toMatchObject({ platformTotals: { arm: "1 ms", x86: "2 ms" } });

    expect(fetchMock).not.toHaveBeenCalled();
    expect(assembly.loadAssemblyContext).toHaveBeenCalledTimes(4);
  });
});

describe("report loaders", () => {
  test("prefers the four-layer assembly path for stack overview", async () => {
    vi.mocked(assembly.loadAssemblyContext).mockResolvedValue({
      project: { id: "tpch-pyflink-reference" },
      framework: { id: "pyflink" },
      dataset: { id: "tpch-on-pyflink-2026q2" },
      source: { id: "pyflink-reference-source" },
    } as Awaited<ReturnType<typeof assembly.loadAssemblyContext>>);
    vi.mocked(assembly.assembleStackOverview).mockReturnValue({
      platformTotals: { arm: "1 ms", x86: "2 ms" },
      components: [],
      categories: [],
    });

    await expect(loadStackOverview()).resolves.toEqual({
      platformTotals: { arm: "1 ms", x86: "2 ms" },
      components: [],
      categories: [],
    });
    expect(assembly.loadAssemblyContext).toHaveBeenCalledWith("tpch-pyflink-reference");
    expect(assembly.assembleStackOverview).toHaveBeenCalled();
  });

  test("does not use legacy data when four-layer assembly fails", async () => {
    vi.mocked(assembly.loadAssemblyContext).mockRejectedValue(new Error("no project bundle"));
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    await expect(loadStackOverview()).rejects.toThrow("no project bundle");
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
