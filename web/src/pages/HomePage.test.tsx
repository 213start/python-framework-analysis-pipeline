import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, test, vi } from "vitest";
import HomePage from "./HomePage";
import { useAsyncData } from "../hooks/useAsyncData";

vi.mock("../hooks/useAsyncData", () => ({
  useAsyncData: vi.fn(),
}));

const mockUseAsyncData = vi.mocked(useAsyncData);

afterEach(() => {
  cleanup();
  mockUseAsyncData.mockReset();
});

function mockSummaryAndOpportunities(summaryState: unknown, opportunityState: unknown) {
  let callIndex = 0;
  mockUseAsyncData.mockImplementation(() => {
    const state = callIndex % 2 === 0 ? summaryState : opportunityState;
    callIndex += 1;
    return state as never;
  });
}

test("renders executive summary metrics and opportunities", async () => {
  mockSummaryAndOpportunities(
    {
      data: {
        metrics: [
          {
            label: "框架调用耗时",
            armValue: "0.91 ms",
            x86Value: "0.81 ms",
            delta: "+12.3%",
          },
        ],
      },
      loading: false,
      error: null,
    },
    {
      data: [
        {
          id: "opp_001",
          title: "减少临时包装对象创建",
          impact: "高",
          effort: "中",
          estimatedGainPct: 8,
        },
      ],
      loading: false,
      error: null,
    },
  );

  render(
    <MemoryRouter>
      <HomePage />
    </MemoryRouter>,
  );

  expect(screen.getAllByText("框架调用耗时").length).toBeGreaterThan(0);
  expect(screen.getByRole("heading", { name: "优先优化机会" })).toBeInTheDocument();
  expect(
    screen.getByText(/减少临时包装对象创建/),
  ).toBeInTheDocument();
});

test("shows empty states when the scaffold has no data yet", () => {
  mockSummaryAndOpportunities(
    { data: { metrics: [] }, loading: false, error: null },
    { data: [], loading: false, error: null },
  );

  render(
    <MemoryRouter>
      <HomePage />
    </MemoryRouter>,
  );

  expect(
    screen.getByText("当前还没有可展示的摘要指标。"),
  ).toBeInTheDocument();
  expect(screen.getByText("当前还没有可展示的优化机会。")).toBeInTheDocument();
});

test("shows a loader failure state", () => {
  mockSummaryAndOpportunities(
    { data: null, loading: false, error: "network down" },
    { data: [], loading: false, error: null },
  );

  render(
    <MemoryRouter>
      <HomePage />
    </MemoryRouter>,
  );

  expect(screen.getByRole("alert")).toHaveTextContent(
    "无法加载摘要指标：network down",
  );
});
