import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, expect, test, vi } from "vitest";
import ArtifactDetailPage from "./ArtifactDetailPage";
import CaseDetailPage from "./CaseDetailPage";
import FunctionDetailPage from "./FunctionDetailPage";
import PatternDetailPage from "./PatternDetailPage";
import RootCauseDetailPage from "./RootCauseDetailPage";
import { useAsyncData } from "../hooks/useAsyncData";

vi.mock("../hooks/useAsyncData", () => ({
  useAsyncData: vi.fn(),
}));

const mockUseAsyncData = vi.mocked(useAsyncData);

afterEach(() => {
  cleanup();
  mockUseAsyncData.mockReset();
});

function renderAt(path: string, routePath: string, element: React.ReactNode) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path={routePath} element={element} />
      </Routes>
    </MemoryRouter>,
  );
}

test("renders case detail data from the selected case", () => {
  mockUseAsyncData.mockReturnValue({
    data: {
      id: "q01",
      name: "TPC-H Q1",
      semanticNotes: "仅用于脚手架联调的示例用例说明。",
      knownDeviations: [],
      artifactIds: ["sql_q01"],
      metrics: {
        demo: { arm: "5.23 s", x86: "4.83 s", delta: "+8.2%" },
        tm: { arm: "4.18 s", x86: "3.88 s", delta: "+7.6%" },
        operator: { arm: "1.77 ms", x86: "1.69 ms", delta: "+4.1%" },
        framework: { arm: "0.91 ms", x86: "0.81 ms", delta: "+12.3%" },
      },
      hotspots: [
        {
          id: "func_001",
          symbol: "_PyObject_Malloc",
          component: "cpython",
          category: "内存",
          delta: "+14.6 ms",
          patternCount: 1,
        },
      ],
      patterns: ["pattern_001"],
      rootCauses: ["rc_001"],
    },
    loading: false,
    error: null,
  } as never);

  renderAt("/case/q01", "/case/:caseId", <CaseDetailPage />);

  expect(screen.getByRole("heading", { name: "TPC-H Q1" })).toBeInTheDocument();
  expect(screen.getByText("仅用于脚手架联调的示例用例说明。")).toBeInTheDocument();
  expect(screen.getByText("_PyObject_Malloc")).toBeInTheDocument();
  expect(screen.getByText("rc_001")).toBeInTheDocument();
});

test("renders function detail data for the selected function", () => {
  mockUseAsyncData.mockReturnValue({
    data: {
      id: "func_001",
      symbol: "_PyObject_Malloc",
      component: "cpython",
      categoryL1: "内存",
      categoryL2: "alloc_free",
      caseIds: ["q01"],
      artifactIds: ["asm_arm_func_001"],
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
      patternIds: ["pattern_001"],
    },
    loading: false,
    error: null,
  } as never);

  renderAt("/function/func_001", "/function/:functionId", <FunctionDetailPage />);

  expect(screen.getByRole("heading", { name: "_PyObject_Malloc" })).toBeInTheDocument();
  expect(screen.getByText("alloc_free")).toBeInTheDocument();
  expect(screen.getByText("pyflink.operator.invoke")).toBeInTheDocument();
  expect(screen.getByText("pattern_001")).toBeInTheDocument();
});

test("renders pattern detail data for the selected pattern", () => {
  mockUseAsyncData.mockReturnValue({
    data: {
      id: "pattern_001",
      title: "热点路径上的临时对象 churn",
      summary:
        "短生命周期对象的重复创建同时出现在行转换和包装层中。",
      confidence: "高",
      caseIds: ["q01"],
      functionIds: ["func_001"],
      rootCauseIds: ["rc_001"],
      artifactIds: ["source_row_adapter_excerpt"],
    },
    loading: false,
    error: null,
  } as never);

  renderAt("/pattern/pattern_001", "/pattern/:patternId", <PatternDetailPage />);

  expect(
    screen.getByRole("heading", { name: "热点路径上的临时对象 churn" }),
  ).toBeInTheDocument();
  expect(screen.getByText("短生命周期对象的重复创建同时出现在行转换和包装层中。")).toBeInTheDocument();
  expect(screen.getByText("rc_001")).toBeInTheDocument();
});

test("renders root cause detail data for the selected root cause", () => {
  mockUseAsyncData.mockReturnValue({
    data: {
      id: "rc_001",
      title: "当前行表示设计导致临时对象过多",
      summary:
        "对象较重的行转换逻辑会反复分配临时包装对象，放大平台敏感的内存成本。",
      confidence: "高置信",
      patternIds: ["pattern_001"],
      artifactIds: ["asm_arm_func_001"],
      optimizationIdeas: [
        "减少临时包装对象创建",
        "在安全前提下复用中间对象",
      ],
      validationPlan: [
        "对比改写前后的对象分配次数",
      ],
    },
    loading: false,
    error: null,
  } as never);

  renderAt("/root-cause/rc_001", "/root-cause/:rootCauseId", <RootCauseDetailPage />);

  expect(
    screen.getByRole("heading", {
      name: "当前行表示设计导致临时对象过多",
    }),
  ).toBeInTheDocument();
  expect(screen.getByText("pattern_001")).toBeInTheDocument();
  expect(screen.getByText("减少临时包装对象创建")).toBeInTheDocument();
  expect(
    screen.getByText("对比改写前后的对象分配次数"),
  ).toBeInTheDocument();
});

test("renders artifact detail data for the selected artifact", () => {
  mockUseAsyncData.mockReturnValue({
    data: {
      id: "asm_arm_func_001",
      title: "_PyObject_Malloc 的 Arm 汇编",
      type: "assembly",
      description: "Arm 侧对象分配路径的代表性片段。",
      path: "artifacts/asm/arm/func_001.s",
      contentType: "text/plain",
      content: "blr _PyObject_Malloc",
    },
    loading: false,
    error: null,
  } as never);

  renderAt("/artifact/asm_arm_func_001", "/artifact/:artifactId", <ArtifactDetailPage />);

  expect(screen.getByRole("heading", { name: "_PyObject_Malloc 的 Arm 汇编" })).toBeInTheDocument();
  expect(screen.getByText("Arm 侧对象分配路径的代表性片段。")).toBeInTheDocument();
  expect(screen.getAllByText("blr _PyObject_Malloc").length).toBeGreaterThan(0);
});
