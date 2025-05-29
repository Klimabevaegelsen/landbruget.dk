import { TooltipProps } from "recharts";
import { NameType, ValueType } from "recharts/types/component/DefaultTooltipContent";

export default function CustomTooltip({
  active,
  payload,
  label,
}: TooltipProps<ValueType, NameType>) {
  if (!active || !payload || !payload.length) {
    return null;
  }

  return (
    <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-md">
      <p className="text-base font-semibold">{label}</p>
      {payload.map((entry, index) => (
        <p
          key={`item-${index}`}
          style={{
            color: entry.color,
          }}
          className="mt-1 text-sm font-medium"
        >
          {`${entry.name}: ${entry.value?.toLocaleString("da-DK")}`}
        </p>
      ))}
    </div>
  );
}
