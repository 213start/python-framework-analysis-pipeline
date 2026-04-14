import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, test, vi } from "vitest";
import ByStackPage from "./ByStackPage";
import { useAsyncData } from "../hooks/useAsyncData";

vi.mock("../hooks/useAsyncData", () => ({
  useAsyncData: vi.fn(),
}));

const mockUseAsyncData = vi.mocked(useAsyncData);

afterEach(() => {
  cleanup();
  mockUseAsyncData.mockReset();
});

test("renders full panorama charts and detailed stack tables", () => {
  mockUseAsyncData.mockReturnValue({
    data: {
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
        {
          id: "glibc",
          name: "glibc",
          armTime: "41.0 ms",
          x86Time: "28.0 ms",
          armShare: "22.5%",
          x86Share: "21.7%",
          delta: "+13.0 ms",
          deltaContribution: "24.5%",
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
          delta: "+23.0 ms",
          deltaContribution: "43.4%",
          topFunction: "_PyObject_Malloc",
          topFunctionId: "func_001",
        },
        {
          id: "interpreter",
          name: "解释器",
          level: "L1",
          armTime: "33.0 ms",
          x86Time: "24.0 ms",
          armShare: "18.1%",
          x86Share: "18.6%",
          delta: "+9.0 ms",
          deltaContribution: "17.0%",
          topFunction: "_PyEval_EvalFrameDefault",
          topFunctionId: "func_002",
        },
      ],
    },
    loading: false,
    error: null,
  } as never);

  render(
    <MemoryRouter>
      <ByStackPage />
    </MemoryRouter>,
  );

  expect(screen.getByRole("heading", { name: "组件双平台耗时分布" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "分类双平台耗时分布" })).toBeInTheDocument();
  expect(screen.getAllByText("图例").length).toBeGreaterThan(0);
  expect(screen.getAllByText("Arm").length).toBeGreaterThan(0);
  expect(screen.getAllByText("x86").length).toBeGreaterThan(0);
  expect(screen.getAllByRole("columnheader", { name: "Arm 耗时" }).length).toBeGreaterThan(0);
  expect(screen.getAllByRole("columnheader", { name: "x86 耗时" }).length).toBeGreaterThan(0);
  expect(screen.getAllByRole("columnheader", { name: "Arm 占比" }).length).toBeGreaterThan(0);
  expect(screen.getAllByRole("columnheader", { name: "x86 占比" }).length).toBeGreaterThan(0);
  expect(screen.getAllByRole("columnheader", { name: "差值贡献" }).length).toBeGreaterThan(0);
  expect(screen.getAllByText("CPython").length).toBeGreaterThan(0);
  expect(screen.getAllByText("解释器").length).toBeGreaterThan(0);
});
