import { InfoCard } from "@/services/supabase/types";

export function BlockInfoCard({ infoCard }: { infoCard: InfoCard }) {
  const items = infoCard.items;

  return (
    <div className="bg-primary-foreground rounded-lg p-4">
      <div className="grid grid-cols-3 gap-3">
        {items.map((item, index) => (
          <div
            key={`${infoCard._key}-${index}`}
            className="flex flex-col gap-2"
          >
            <label className="font-bold">{item.label}</label>
            <p className="text-sm ">{item.value}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
