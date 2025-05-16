import { InfoCard, KPIGroup } from "@/services/supabase/types";

export function BlockInfoCard({ infoCard }: { infoCard: InfoCard | KPIGroup }) {
  const items = "items" in infoCard ? infoCard.items : infoCard.kpis;

  return (
    <div className="grid grid-cols-2 gap-3">
      {items.map((item, index) => (
        <div
          key={`${infoCard._key}-${index}`}
          className="rounded bg-primary-foreground p-4 flex flex-col gap-2"
        >
          <label className="text-sm font-medium">{item.label}</label>
          <p className="text-2xl font-bold text-green-900">{item.value}</p>
        </div>
      ))}
    </div>
  );
}
