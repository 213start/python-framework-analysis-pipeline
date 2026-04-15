import {
  assembleArtifactDetail as assembleArtifactDetailFromAssembly,
  assembleCaseDetail as assembleCaseDetailFromAssembly,
  assembleCaseIndex as assembleCaseIndexFromAssembly,
  assembleCategoryDetail as assembleCategoryDetailFromAssembly,
  assembleComponentDetail as assembleComponentDetailFromAssembly,
  assembleExecutiveSummary as assembleExecutiveSummaryFromAssembly,
  assembleFunctionDetail as assembleFunctionDetailFromAssembly,
  assembleOpportunityRanking as assembleOpportunityRankingFromAssembly,
  assemblePatternDetail as assemblePatternDetailFromAssembly,
  assemblePatternIndex as assemblePatternIndexFromAssembly,
  assembleRootCauseDetail as assembleRootCauseDetailFromAssembly,
  assembleRootCauseIndex as assembleRootCauseIndexFromAssembly,
  assembleScopeSummary as assembleScopeSummaryFromAssembly,
  assembleStackOverview as assembleStackOverviewFromAssembly,
  loadAssemblyContext,
} from "./assembly";
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

const DEFAULT_PROJECT_ID = "tpch-pyflink-reference";

async function loadDefaultAssemblyContext() {
  return loadAssemblyContext(DEFAULT_PROJECT_ID);
}

export async function loadExecutiveSummary(): Promise<ExecutiveSummary> {
  return assembleExecutiveSummaryFromAssembly(await loadDefaultAssemblyContext());
}

export async function loadCaseIndex(): Promise<CaseIndexEntry[]> {
  return assembleCaseIndexFromAssembly(await loadDefaultAssemblyContext());
}

export async function loadOpportunityRanking(): Promise<OpportunityRankingEntry[]> {
  return assembleOpportunityRankingFromAssembly(await loadDefaultAssemblyContext());
}

export async function loadScopeSummary(): Promise<ScopeSummary> {
  return assembleScopeSummaryFromAssembly(await loadDefaultAssemblyContext());
}

export async function loadStackOverview(): Promise<StackOverview> {
  return assembleStackOverviewFromAssembly(await loadDefaultAssemblyContext());
}

export async function loadRootCauseIndex(): Promise<RootCauseIndexEntry[]> {
  return assembleRootCauseIndexFromAssembly(await loadDefaultAssemblyContext());
}

export async function loadPatternIndex(): Promise<PatternIndexEntry[]> {
  return assemblePatternIndexFromAssembly(await loadDefaultAssemblyContext());
}

export async function loadCaseDetail(id: string): Promise<CaseDetail> {
  return assembleCaseDetailFromAssembly(await loadDefaultAssemblyContext(), id);
}

export async function loadFunctionDetail(id: string): Promise<FunctionDetail> {
  return assembleFunctionDetailFromAssembly(await loadDefaultAssemblyContext(), id);
}

export async function loadPatternDetail(id: string): Promise<PatternDetail> {
  return assemblePatternDetailFromAssembly(await loadDefaultAssemblyContext(), id);
}

export async function loadRootCauseDetail(id: string): Promise<RootCauseDetail> {
  return assembleRootCauseDetailFromAssembly(await loadDefaultAssemblyContext(), id);
}

export async function loadComponentDetail(id: string): Promise<ComponentDetail> {
  return assembleComponentDetailFromAssembly(await loadDefaultAssemblyContext(), id);
}

export async function loadCategoryDetail(id: string): Promise<CategoryDetail> {
  return assembleCategoryDetailFromAssembly(await loadDefaultAssemblyContext(), id);
}

export async function loadArtifactDetail(id: string): Promise<ArtifactDetail> {
  return assembleArtifactDetailFromAssembly(await loadDefaultAssemblyContext(), id);
}
