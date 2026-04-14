import type { PropsWithChildren } from "react";

export function Tag({ children }: PropsWithChildren) {
  return <span className="tag">{children}</span>;
}
