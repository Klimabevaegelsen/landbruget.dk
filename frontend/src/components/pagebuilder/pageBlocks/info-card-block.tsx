import { InfoCard } from "@/services/supabase/types";

export function InfoCardBlock({ infoCard }: { infoCard: InfoCard }) {
  return (
    <div className="grid grid-cols-2 gap-3">
      {infoCard.items.map((item, index) => (
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
