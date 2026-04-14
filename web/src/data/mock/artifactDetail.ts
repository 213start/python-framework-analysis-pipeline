import type { ArtifactDetail } from "../../types/report";

export const mockArtifactDetail: ArtifactDetail = {
  id: "asm_arm_func_001",
  title: "_PyObject_Malloc 的 Arm 汇编",
  type: "assembly",
  description: "用于展示热点分配路径的 Arm 侧代表性汇编片段。",
  path: "artifacts/asm/arm/func_001.s",
  contentType: "text/plain",
  content: "stp x29, x30, [sp, #-16]!\nmov x29, sp\nbl _PyObject_Malloc\n",
};
