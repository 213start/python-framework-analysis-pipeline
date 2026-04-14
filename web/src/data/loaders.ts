import { mockCaseIndex } from "./mock/caseIndex";
import { mockCaseDetail } from "./mock/caseDetail";
import { mockCategoryDetail } from "./mock/categoryDetail";
import { mockComponentDetail } from "./mock/componentDetail";
import { mockArtifactDetail } from "./mock/artifactDetail";
import { mockExecutiveSummary } from "./mock/executiveSummary";
import { mockFunctionDetail } from "./mock/functionDetail";
import { mockOpportunityRanking } from "./mock/opportunityRanking";
import { mockPatternDetail } from "./mock/patternDetail";
import { mockPatternIndex } from "./mock/patternIndex";
import { mockRootCauseDetail } from "./mock/rootCauseDetail";
import { mockRootCauseIndex } from "./mock/rootCauseIndex";
import { mockScopeSummary } from "./mock/scopeSummary";
import { mockStackOverview } from "./mock/stackOverview";
import { getArtifactFilePath, getDetailFilePath, getSummaryFilePath } from "./paths";
import type {
  ArtifactDetail,
  CaseIndexEntry,
  CaseDetail,
  CategoryDetail,
  ComponentDetail,
  ExecutiveSummary,
  FunctionDetail,
  OpportunityRankingEntry,
  PatternDetail,
  PatternIndexEntry,
  RootCauseIndexEntry,
  RootCauseDetail,
  ScopeSummary,
  StackOverview,
} from "../types/report";

async function loadJson<T>(path: string): Promise<T | null> {
  try {
    const response = await fetch(path);
    if (!response.ok) {
      return null;
    }

    try {
      return (await response.json()) as T;
    } catch {
      return null;
    }
  } catch {
    return null;
  }
}

async function loadText(path: string): Promise<string | null> {
  try {
    const response = await fetch(path);
    if (!response.ok) {
      return null;
    }

    return await response.text();
  } catch {
    return null;
  }
}

export async function loadExecutiveSummary(): Promise<ExecutiveSummary> {
  return (
    (await loadJson<ExecutiveSummary>(getSummaryFilePath("executive_summary.json"))) ??
    mockExecutiveSummary
  );
}

export async function loadCaseIndex(): Promise<CaseIndexEntry[]> {
  return (await loadJson<CaseIndexEntry[]>(getSummaryFilePath("case_index.json"))) ?? mockCaseIndex;
}

export async function loadOpportunityRanking(): Promise<OpportunityRankingEntry[]> {
  return (
    (await loadJson<OpportunityRankingEntry[]>(
      getSummaryFilePath("opportunity_ranking.json"),
    )) ?? mockOpportunityRanking
  );
}

export async function loadScopeSummary(): Promise<ScopeSummary> {
  return (
    (await loadJson<ScopeSummary>(getSummaryFilePath("scope.json"))) ??
    mockScopeSummary
  );
}

export async function loadStackOverview(): Promise<StackOverview> {
  return (
    (await loadJson<StackOverview>(getSummaryFilePath("stack_overview.json"))) ??
    mockStackOverview
  );
}

export async function loadRootCauseIndex(): Promise<RootCauseIndexEntry[]> {
  return (
    (await loadJson<RootCauseIndexEntry[]>(
      getSummaryFilePath("root_cause_index.json"),
    )) ?? mockRootCauseIndex
  );
}

export async function loadPatternIndex(): Promise<PatternIndexEntry[]> {
  return (
    (await loadJson<PatternIndexEntry[]>(
      getSummaryFilePath("pattern_index.json"),
    )) ?? mockPatternIndex
  );
}

export async function loadCaseDetail(id: string): Promise<CaseDetail> {
  return (
    (await loadJson<CaseDetail>(getDetailFilePath("cases", id))) ?? mockCaseDetail
  );
}

export async function loadFunctionDetail(id: string): Promise<FunctionDetail> {
  return (
    (await loadJson<FunctionDetail>(getDetailFilePath("functions", id))) ??
    mockFunctionDetail
  );
}

export async function loadPatternDetail(id: string): Promise<PatternDetail> {
  return (
    (await loadJson<PatternDetail>(getDetailFilePath("patterns", id))) ??
    mockPatternDetail
  );
}

export async function loadRootCauseDetail(id: string): Promise<RootCauseDetail> {
  return (
    (await loadJson<RootCauseDetail>(getDetailFilePath("root_causes", id))) ??
    mockRootCauseDetail
  );
}

export async function loadComponentDetail(id: string): Promise<ComponentDetail> {
  return (
    (await loadJson<ComponentDetail>(getDetailFilePath("components", id))) ??
    mockComponentDetail
  );
}

export async function loadCategoryDetail(id: string): Promise<CategoryDetail> {
  return (
    (await loadJson<CategoryDetail>(getDetailFilePath("categories", id))) ??
    mockCategoryDetail
  );
}

export async function loadArtifactDetail(id: string): Promise<ArtifactDetail> {
  const metadata =
    (await loadJson<Omit<ArtifactDetail, "content">>(getDetailFilePath("artifacts", id))) ??
    (({ content, ...fallback }) => fallback)(mockArtifactDetail);
  const content =
    (await loadText(getArtifactFilePath(metadata.path))) ??
    (id === mockArtifactDetail.id ? mockArtifactDetail.content : "");

  return {
    ...metadata,
    content,
  };
}
