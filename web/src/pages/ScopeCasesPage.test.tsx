import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, test, vi } from "vitest";
import CasesPage from "./CasesPage";
import ScopePage from "./ScopePage";
import { useAsyncData } from "../hooks/useAsyncData";

vi.mock("../hooks/useAsyncData", () => ({
  useAsyncData: vi.fn(),
}));

const mockUseAsyncData = vi.mocked(useAsyncData);

afterEach(() => {
  cleanup();
  mockUseAsyncData.mockReset();
});

test("renders scope overview with highlight cards and taxonomy sections", () => {
  mockUseAsyncData.mockReturnValue({
    data: {
      pageHighlights: [
        { label: "纳入项", value: "3", detail: "demo 报告当前只覆盖 3 类纳入项。" },
        { label: "一级分类", value: "9", detail: "一级分类用于全景展示。" },
      ],
      includedScope: ["PyFlink 框架开销"],
      excludedScope: ["真实生产证据"],
      metrics: [
        {
          name: "框架调用耗时",
          definition: "围绕 Python UDF 边界的桥接与包装开销。",
          boundary: "Java PreUDF -> Java PostUDF 减去 Python UDF 时间",
          normalization: "按调用、按记录",
        },
      ],
      taxonomy: {
        level1Categories: ["解释器", "内存"],
        componentAxis: ["cpython", "glibc"],
        unknownWarningThreshold: "5%",
      },
    },
    loading: false,
    error: null,
  } as never);

  render(
    <MemoryRouter>
      <ScopePage />
    </MemoryRouter>,
  );

  expect(screen.getByText("纳入项")).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "计时边界流程图" })).toBeInTheDocument();
  expect(screen.getByText("Demo 总耗时")).toBeInTheDocument();
  expect(screen.getByText("TM 端到端耗时")).toBeInTheDocument();
  expect(screen.getByText("业务算子耗时")).toBeInTheDocument();
  expect(screen.getAllByText("框架调用耗时").length).toBeGreaterThan(1);
  expect(screen.getByRole("heading", { name: "指标边界总览" })).toBeInTheDocument();
  expect(screen.getAllByText("框架调用耗时").length).toBeGreaterThan(1);
  expect(screen.getByRole("heading", { name: "一级分类体系" })).toBeInTheDocument();
  expect(screen.getByText("Unknown 告警阈值：5%")).toBeInTheDocument();
});

test("renders cases overview with metric panorama and asset catalog", () => {
  mockUseAsyncData.mockReturnValue({
    data: [
      {
        id: "q01",
        name: "TPC-H Q1",
        scale: "100G",
        workloadForm: "单个 Python UDF",
        semanticStatus: "已验证",
        pythonUdfMode: "整体改写",
        sourceSqlArtifactId: "sql_q01",
        pythonUdfArtifactId: "pyflink_q01",
        notes: "聚焦聚合与行包装链路。",
        demoDelta: "+8.2%",
        tmDelta: "+7.6%",
        operatorDelta: "+4.1%",
        frameworkDelta: "+12.3%",
      },
      {
        id: "q06",
        name: "TPC-H Q6",
        scale: "100G",
        workloadForm: "单个 Python UDF",
        semanticStatus: "已验证",
        pythonUdfMode: "整体改写",
        sourceSqlArtifactId: "sql_q06",
        pythonUdfArtifactId: "pyflink_q06",
        notes: "聚焦过滤、包装与等待路径。",
        demoDelta: "+6.4%",
        tmDelta: "+5.8%",
        operatorDelta: "+3.5%",
        frameworkDelta: "+10.1%",
      },
    ],
    loading: false,
    error: null,
  } as never);

  render(
    <MemoryRouter>
      <CasesPage />
    </MemoryRouter>,
  );

  expect(screen.getByRole("heading", { name: "四类指标差异全景" })).toBeInTheDocument();
  expect(screen.getAllByText("Demo").length).toBeGreaterThan(1);
  expect(screen.getAllByText("框架调用").length).toBeGreaterThan(1);
  expect(screen.getByRole("heading", { name: "SQL 与 Python UDF 成对资产入口" })).toBeInTheDocument();
  expect(screen.getAllByText("SQL 基准").length).toBeGreaterThan(1);
  expect(screen.getAllByText("Python UDF 实现").length).toBeGreaterThan(1);
  expect(screen.getByRole("heading", { name: "用例资产编目" })).toBeInTheDocument();
  expect(screen.getAllByRole("link", { name: "sql_q01" })[0]).toHaveAttribute("href", "/artifact/sql_q01");
  expect(screen.getAllByRole("link", { name: "pyflink_q06" })[0]).toHaveAttribute("href", "/artifact/pyflink_q06");
  expect(screen.getAllByText("聚焦过滤、包装与等待路径。").length).toBeGreaterThan(1);
});
