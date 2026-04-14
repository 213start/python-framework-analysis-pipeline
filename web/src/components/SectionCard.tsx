import type { PropsWithChildren } from "react";

type SectionCardProps = PropsWithChildren<{
  title?: string;
}>;

export function SectionCard({ title, children }: SectionCardProps) {
  return (
    <section className="section-card">
      {title ? <h3 className="section-card__title">{title}</h3> : null}
      {children}
    </section>
  );
}
