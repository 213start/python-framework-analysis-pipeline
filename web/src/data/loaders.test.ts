import { afterEach, describe, expect, test, vi } from "vitest";
import { mockExecutiveSummary } from "./mock/executiveSummary";
import { mockFunctionDetail } from "./mock/functionDetail";
import { mockOpportunityRanking } from "./mock/opportunityRanking";
import { loadExecutiveSummary, loadFunctionDetail, loadOpportunityRanking } from "./loaders";
import { getSummaryFilePath } from "./paths";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("report data paths", () => {
  test("builds the executive summary path", () => {
    expect(getSummaryFilePath("executive_summary.json")).toBe(
      "/report-package/summary/executive_summary.json",
    );
  });
});

describe("report loaders", () => {
  test("loads executive summary from JSON when available", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: vi.fn().mockResolvedValue({
        title: "Executive Summary",
        subtitle: "Loaded from JSON",
        metrics: [],
        topPattern: "JSON pattern",
        topRootCause: "JSON root cause",
      }),
    });

    vi.stubGlobal("fetch", fetchMock);

    await expect(loadExecutiveSummary()).resolves.toEqual({
      title: "Executive Summary",
      subtitle: "Loaded from JSON",
      metrics: [],
      topPattern: "JSON pattern",
      topRootCause: "JSON root cause",
    });
    expect(fetchMock).toHaveBeenCalledWith(
      getSummaryFilePath("executive_summary.json"),
    );
  });

  test("falls back to the mock opportunity ranking when JSON parsing fails", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: vi.fn().mockRejectedValue(new Error("invalid json")),
    });

    vi.stubGlobal("fetch", fetchMock);

    await expect(loadOpportunityRanking()).resolves.toEqual(
      mockOpportunityRanking,
    );
    expect(fetchMock).toHaveBeenCalledWith(
      getSummaryFilePath("opportunity_ranking.json"),
    );
  });

  test("falls back to the mock executive summary when fetch rejects", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));

    vi.stubGlobal("fetch", fetchMock);

    await expect(loadExecutiveSummary()).resolves.toEqual(mockExecutiveSummary);
    expect(fetchMock).toHaveBeenCalledWith(
      getSummaryFilePath("executive_summary.json"),
    );
  });

  test("falls back to the mock function detail and preserves diffView", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));

    vi.stubGlobal("fetch", fetchMock);

    await expect(loadFunctionDetail("func_missing")).resolves.toEqual(mockFunctionDetail);
    expect(mockFunctionDetail.diffView?.analysisBlocks.length).toBeGreaterThan(0);
  });
});
