import type { ReactElement } from "react";
import ArtifactDetailPage from "./pages/ArtifactDetailPage";
import ByCasePage from "./pages/ByCasePage";
import ByStackPage from "./pages/ByStackPage";
import CategoryDetailPage from "./pages/CategoryDetailPage";
import CasesPage from "./pages/CasesPage";
import CaseDetailPage from "./pages/CaseDetailPage";
import ComponentDetailPage from "./pages/ComponentDetailPage";
import FunctionDetailPage from "./pages/FunctionDetailPage";
import HomePage from "./pages/HomePage";
import InsightsPage from "./pages/InsightsPage";
import PatternDetailPage from "./pages/PatternDetailPage";
import RootCauseDetailPage from "./pages/RootCauseDetailPage";
import ScopePage from "./pages/ScopePage";

export type AppRoute = {
  path: string;
  label: string;
  element: ReactElement;
  showInNav?: boolean;
};

export const appRoutes: AppRoute[] = [
  { path: "/", label: "执行摘要", element: <HomePage /> },
  { path: "/scope", label: "分析边界", element: <ScopePage /> },
  { path: "/cases", label: "用例资产", element: <CasesPage /> },
  {
    path: "/analysis/by-case",
    label: "按用例分析",
    element: <ByCasePage />,
  },
  {
    path: "/analysis/by-stack",
    label: "按栈分析",
    element: <ByStackPage />,
  },
  {
    path: "/case/:caseId",
    label: "用例详情",
    element: <CaseDetailPage />,
    showInNav: false,
  },
  {
    path: "/component/:componentId",
    label: "组件详情",
    element: <ComponentDetailPage />,
    showInNav: false,
  },
  {
    path: "/category/:categoryId",
    label: "分类详情",
    element: <CategoryDetailPage />,
    showInNav: false,
  },
  {
    path: "/function/:functionId",
    label: "函数详情",
    element: <FunctionDetailPage />,
    showInNav: false,
  },
  {
    path: "/artifact/:artifactId",
    label: "证据详情",
    element: <ArtifactDetailPage />,
    showInNav: false,
  },
  {
    path: "/pattern/:patternId",
    label: "模式详情",
    element: <PatternDetailPage />,
    showInNav: false,
  },
  {
    path: "/root-cause/:rootCauseId",
    label: "根因详情",
    element: <RootCauseDetailPage />,
    showInNav: false,
  },
  { path: "/insights", label: "优化洞察", element: <InsightsPage /> },
];

export const navRoutes = appRoutes.filter((route) => route.showInNav !== false);
