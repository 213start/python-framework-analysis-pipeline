import type { CaseDetail } from "../../types/report";

export const mockCaseDetail: CaseDetail = {
  id: "q01",
  name: "TPC-H Q1",
  semanticNotes:
    "仅用于前端示例展示；这里用一个示意性的 Python UDF 承载整条 SQL 的完整语义。",
  knownDeviations: [],
  artifactIds: ["sql_q01", "pyflink_q01"],
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
};
