import { TooltipProps } from "recharts";
import {
  NameType,
  ValueType,
} from "recharts/types/component/DefaultTooltipContent";

export default function CustomTooltip({
  active,
  payload,
  label,
}: TooltipProps<ValueType, NameType>) {
  if (!active || !payload || !payload.length) {
    return null;
  }

  return (
    <div className="p-4 bg-white rounded-lg shadow-md border border-gray-200">
      <p className="text-base font-semibold">{label}</p>
      {payload.map((entry, index) => (
        <p
          key={`item-${index}`}
          style={{
            color: entry.color,
          }}
          className="text-sm font-medium mt-1"
        >
          {`${entry.name}: ${entry.value?.toLocaleString("da-DK")}`}
        </p>
      ))}
    </div>
  );
}
