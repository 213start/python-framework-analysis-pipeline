import type { OpportunityRankingEntry } from "../../types/report";

export const mockOpportunityRanking: OpportunityRankingEntry[] = [
  {
    id: "opp_001",
    title: "减少临时包装对象创建",
    impact: "高",
    effort: "中",
    estimatedGainPct: 8,
    rootCauseId: "rc_001",
  },
  {
    id: "opp_002",
    title: "收敛冗余的行适配层级",
    impact: "中",
    effort: "中",
    estimatedGainPct: 5,
    rootCauseId: "rc_002",
  },
];
