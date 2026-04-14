import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, test } from "vitest";
import App from "./App";

afterEach(() => {
  cleanup();
});

test("renders a non-root route", async () => {
  render(<App useMemoryRouter initialEntries={["/scope"]} />);

  expect(await screen.findByRole("heading", { name: "分析边界与指标定义" })).toBeInTheDocument();
  expect(screen.getByRole("main")).toHaveTextContent(
    "分析边界与指标定义",
  );
  expect(screen.getByRole("link", { name: "分析边界" })).toHaveAttribute(
    "aria-current",
    "page",
  );
  expect(
    screen.getByRole("link", { name: "执行摘要" }),
  ).not.toHaveAttribute("aria-current");
});

test("redirects unknown routes to the executive summary", () => {
  render(<App useMemoryRouter initialEntries={["/not-a-real-route"]} />);

  expect(screen.getByRole("main")).toHaveTextContent("执行摘要");
  expect(screen.getByRole("link", { name: "执行摘要" })).toHaveAttribute(
    "aria-current",
    "page",
  );
});
