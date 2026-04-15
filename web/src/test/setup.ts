import { readFile } from "node:fs/promises";
import { join, normalize } from "node:path";
import { beforeEach } from "vitest";

const publicRoot = join(process.cwd(), "public");

function toPublicFilePath(url: string) {
  const { pathname } = new URL(url, "http://localhost");
  if (!pathname.startsWith("/examples/")) {
    return null;
  }

  const filePath = normalize(join(publicRoot, pathname));
  if (!filePath.startsWith(publicRoot)) {
    return null;
  }

  return filePath;
}

async function staticPublicFetch(input: RequestInfo | URL) {
  const filePath = toPublicFilePath(String(input));
  if (!filePath) {
    return new Response("", { status: 404 });
  }

  try {
    const content = await readFile(filePath);
    const contentType = filePath.endsWith(".json") ? "application/json" : "text/plain";
    return new Response(content, {
      status: 200,
      headers: {
        "content-type": contentType,
      },
    });
  } catch {
    return new Response("", { status: 404 });
  }
}

beforeEach(() => {
  globalThis.fetch = staticPublicFetch as typeof fetch;
});
