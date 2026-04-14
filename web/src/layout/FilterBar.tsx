export type FilterBarItem = {
  label: string;
  value: string;
};

type FilterBarProps = {
  items: FilterBarItem[];
};

export function FilterBar({ items }: FilterBarProps) {
  return (
    <section className="filter-bar" aria-label="全局筛选">
      {items.map((item) => (
        <div key={item.label} className="filter-pill">
          {item.label}: {item.value}
        </div>
      ))}
    </section>
  );
}
