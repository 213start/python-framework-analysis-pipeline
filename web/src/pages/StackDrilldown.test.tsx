import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, expect, test, vi } from "vitest";
import CategoryDetailPage from "./CategoryDetailPage";
import ComponentDetailPage from "./ComponentDetailPage";
import FunctionDetailPage from "./FunctionDetailPage";
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

test("renders complete hotspot table for component detail", () => {
  mockUseAsyncData.mockReturnValue({
    data: {
      id: "cpython",
      name: "CPython",
      armTime: "96.0 ms",
      x86Time: "61.0 ms",
      armShare: "52.7%",
      x86Share: "47.3%",
      delta: "+35.0 ms",
      deltaContribution: "66.0%",
      categories: [],
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
      ],
      patternIds: ["pattern_001"],
      rootCauseIds: ["rc_001"],
      artifactIds: ["asm_arm_func_001"],
    },
    loading: false,
    error: null,
  } as never);

  renderAt("/component/cpython", "/component/:componentId", <ComponentDetailPage />);

  expect(screen.getByRole("heading", { name: "全量热点函数" })).toBeInTheDocument();
  expect(screen.getByRole("columnheader", { name: "Arm 自耗时" })).toBeInTheDocument();
  expect(screen.getByRole("columnheader", { name: "x86 总耗时" })).toBeInTheDocument();
  expect(screen.getByRole("columnheader", { name: "差值贡献" })).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "_PyObject_Malloc" })).toHaveAttribute("href", "/function/func_001");
});

test("renders complete hotspot table for category detail", () => {
  mockUseAsyncData.mockReturnValue({
    data: {
      id: "memory",
      name: "内存",
      level: "L1",
      componentIds: ["cpython"],
      caseIds: ["q01"],
      armTime: "54.0 ms",
      x86Time: "31.0 ms",
      armShare: "29.7%",
      x86Share: "24.0%",
      delta: "+23.0 ms",
      deltaContribution: "43.4%",
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
      artifactIds: ["asm_arm_func_001"],
    },
    loading: false,
    error: null,
  } as never);

  renderAt("/category/memory", "/category/:categoryId", <CategoryDetailPage />);

  expect(screen.getByRole("heading", { name: "全量热点函数" })).toBeInTheDocument();
  expect(screen.getByRole("columnheader", { name: "组件" })).toBeInTheDocument();
  expect(screen.getByRole("columnheader", { name: "Arm 占比" })).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "_PyObject_Malloc" })).toHaveAttribute("href", "/function/func_001");
});

test("renders logical-block diff with non-linear mappings and fold controls for function detail", () => {
  mockUseAsyncData.mockReturnValue({
    data: {
      id: "func_001",
      symbol: "_PyObject_Malloc",
      component: "cpython",
      categoryL1: "内存",
      categoryL2: "alloc_free",
      caseIds: ["q01"],
      artifactIds: ["asm_arm_func_001", "asm_x86_func_001"],
      metrics: {
        selfArm: "21.2 ms",
        selfX86: "10.4 ms",
        totalArm: "33.5 ms",
        totalX86: "18.9 ms",
        delta: "+14.6 ms",
      },
      callPath: ["pyflink.operator.invoke", "_PyObject_Malloc"],
      patternIds: ["pattern_001"],
      diffView: {
        functionId: "func_001",
        sourceFile: "Objects/obmalloc.c",
        sourceLocation: "Objects/obmalloc.c:1421-1432",
        diffGuide:
          "本视图按逻辑分析块对齐，不按源码行顺序强行逐行对照。同一源码语义可能映射到多个离散机器码区块。",
        analysisBlocks: [
          {
            id: "blk_001",
            label: "Fast-Path Alloc",
            summary: "对齐 Fast Path 上的 freelist 探测与慢路径回退。",
            patternTag: "ObjectManipulator",
            mappingType: "一对多",
            sourceAnchors: [
              {
                id: "src_001",
                label: "Freeblock 判空",
                role: "Fast Path",
                location: "Objects/obmalloc.c:1421",
                snippet: "if (pool->freeblock != NULL) {",
                defaultExpanded: true,
              },
              {
                id: "src_002",
                label: "Slow Path 回退",
                role: "Slow Path",
                location: "Objects/obmalloc.c:1432",
                snippet: "goto allocate_from_new_pool(pool);",
                defaultExpanded: false,
              },
            ],
            armRegions: [
              {
                id: "arm_001",
                label: "Freeblock Load",
                location: "0x0000-0x0008",
                role: "Fast Path",
                snippet: "ldr x9, [x0, #16]\ncbz x9, slow_path",
                highlights: ["ldr x9", "cbz x9"],
                defaultExpanded: true,
              },
              {
                id: "arm_002",
                label: "Pool Reload",
                location: "0x0018-0x0028",
                role: "Reload Chain",
                snippet: "ldr x10, [x19, #24]\nldr x11, [x10, #8]",
                highlights: ["ldr x10", "ldr x11"],
                defaultExpanded: false,
              },
            ],
            x86Regions: [
              {
                id: "x86_001",
                label: "Freeblock Load",
                location: "0x0000-0x0006",
                role: "Fast Path",
                snippet: "mov rax, qword ptr [rdi+16]\ntest rax, rax",
                highlights: ["mov rax", "test rax"],
                defaultExpanded: true,
              },
            ],
            mappings: [
              {
                id: "map_001",
                label: "freeblock 判空 -> fast path",
                sourceAnchorIds: ["src_001"],
                armRegionIds: ["arm_001"],
                x86RegionIds: ["x86_001"],
                note: "两个平台都直接对应到 fast path 入口区块。",
              },
              {
                id: "map_002",
                label: "slow path 回退 -> Arm pool reload",
                sourceAnchorIds: ["src_002"],
                armRegionIds: ["arm_002"],
                x86RegionIds: [],
                note: "x86 侧被编译器合并到主 fast path 后续路径，Arm 侧保留为独立 reload 区块。",
              },
            ],
            diffSignals: ["额外加载链", "更长依赖链"],
            alignmentNote: "按 fast-path block 对齐",
            performanceNote: "Arm 侧额外依赖链更长",
            defaultExpanded: true,
          },
        ],
      },
    },
    loading: false,
    error: null,
  } as never);

  const { container } = renderAt("/function/func_001", "/function/:functionId", <FunctionDetailPage />);
  const hasText = (value: string) =>
    (_: string, element?: Element | null) => element?.textContent?.includes(value) ?? false;

  expect(screen.getByRole("heading", { name: "Fast-Path Alloc" })).toBeInTheDocument();
  expect(screen.getByText("对齐 Fast Path 上的 freelist 探测与慢路径回退。")).toBeInTheDocument();
  expect(screen.getByText("Freeblock 判空")).toBeInTheDocument();
  expect(screen.getByText("Fast Path")).toBeInTheDocument();
  expect(screen.getByText("if (pool->freeblock != NULL) {")).toBeInTheDocument();
  expect(screen.getByText("goto allocate_from_new_pool(pool);")).toBeInTheDocument();
});
