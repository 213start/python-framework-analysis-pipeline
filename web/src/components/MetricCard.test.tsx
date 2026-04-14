import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { expect, test } from "vitest";
import { MetricCard } from "./MetricCard";

test("renders metric labels and values", () => {
  render(
    <MetricCard
      title="Framework Call Time"
      primaryValue="0.89 ms"
      secondaryValue="Arm +12.4%"
    />,
  );

  expect(screen.getByText("Framework Call Time")).toBeInTheDocument();
  expect(screen.getByText("0.89 ms")).toBeInTheDocument();
  expect(screen.getByText("Arm +12.4%")).toBeInTheDocument();
});
