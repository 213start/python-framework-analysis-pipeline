function encodeSegment(segment: string) {
  return encodeURIComponent(segment);
}

export function getSummaryFilePath(fileName: string) {
  return `/report-package/summary/${encodeSegment(fileName)}`;
}

export function getDetailFilePath(kind: string, id: string) {
  return `/report-package/details/${encodeSegment(kind)}/${encodeSegment(id)}.json`;
}

export function getArtifactFilePath(path: string) {
  return `/report-package/${path.replace(/^\/+/, "")}`;
}
