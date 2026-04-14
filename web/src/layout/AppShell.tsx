import { PropsWithChildren } from "react";
import { FilterBar } from "./FilterBar";
import { SideNav } from "./SideNav";
import { TopBar } from "./TopBar";

export function AppShell({ children }: PropsWithChildren) {
  const filterItems = [
    { label: "平台", value: "Arm vs x86" },
    { label: "单位", value: "按每次 UDF 调用" },
    { label: "检索", value: "暂未接线" },
  ];

  return (
    <div className="app-shell">
      <SideNav />
      <div className="app-shell__main">
        <TopBar />
        <FilterBar items={filterItems} />
        <main className="page-content">{children}</main>
      </div>
    </div>
  );
}
