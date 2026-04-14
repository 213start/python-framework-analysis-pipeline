type MetricCardProps = {
  title: string;
  primaryValue: string;
  secondaryValue?: string;
};

export function MetricCard({
  title,
  primaryValue,
  secondaryValue,
}: MetricCardProps) {
  return (
    <article className="metric-card">
      <p className="metric-card__title">{title}</p>
      <p className="metric-card__value">{primaryValue}</p>
      {secondaryValue !== undefined && secondaryValue !== null ? (
        <p className="metric-card__meta">{secondaryValue}</p>
      ) : null}
    </article>
  );
}
