import type { RootCauseDetail } from "../../types/report";

export const mockRootCauseDetail: RootCauseDetail = {
  id: "rc_001",
  title: "当前行表示设计导致临时对象过多",
  summary:
    "对象密集型行转换逻辑会反复分配临时包装对象，并放大平台敏感的内存开销。",
  confidence: "高置信",
  patternIds: ["pattern_001"],
  artifactIds: ["source_row_adapter_excerpt", "asm_arm_func_001", "asm_x86_func_001"],
  optimizationIdeas: [
    "减少临时包装对象创建",
    "在安全前提下复用中间对象",
  ],
  validationPlan: [
    "对比改写前后的对象分配次数",
    "对比 Arm 与 x86 上的分配敏感型微基准",
  ],
};
