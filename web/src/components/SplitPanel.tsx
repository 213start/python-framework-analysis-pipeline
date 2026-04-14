import type { PropsWithChildren } from "react";

type SplitPanelProps = PropsWithChildren<{
  className?: string;
}>;

export function SplitPanel({ children, className }: SplitPanelProps) {
  return <div className={className ? `split-panel ${className}` : "split-panel"}>{children}</div>;
}
