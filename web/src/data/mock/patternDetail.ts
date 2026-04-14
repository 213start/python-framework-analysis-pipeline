import type { PatternDetail } from "../../types/report";

export const mockPatternDetail: PatternDetail = {
  id: "pattern_001",
  title: "热点路径上存在高频临时对象抖动",
  summary:
    "短生命周期对象会在行转换与包装层中被反复创建。",
  confidence: "高",
  caseIds: ["q01"],
  functionIds: ["func_001"],
  rootCauseIds: ["rc_001"],
  artifactIds: ["source_row_adapter_excerpt"],
};
