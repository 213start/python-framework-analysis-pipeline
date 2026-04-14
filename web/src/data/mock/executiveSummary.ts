import type { ExecutiveSummary } from "../../types/report";

export const mockExecutiveSummary: ExecutiveSummary = {
  title: "执行摘要",
  subtitle: "用于前端骨架展示的示例数据包",
  metrics: [
    {
      label: "框架调用耗时",
      armValue: "0.91 ms",
      x86Value: "0.81 ms",
      delta: "+12.3%",
      target: "/analysis/by-case",
    },
  ],
  topPattern: "热点路径上存在高频临时对象抖动",
  topRootCause:
    "当前行表示设计导致临时对象过多",
};
