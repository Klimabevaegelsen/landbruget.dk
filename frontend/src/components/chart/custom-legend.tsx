import { Payload } from "recharts/types/component/DefaultLegendContent";

interface CustomLegendProps {
  payload?: Payload[];
  onLegendClick?: (dataKey: string) => void;
}

export default function CustomLegend({
  payload,
  onLegendClick,
}: CustomLegendProps) {
  if (!payload || !payload.length) {
    return null;
  }

  return (
    <div className="flex flex-wrap gap-4 ">
      {payload.map((entry, index) => (
        <button
          key={`legend-item-${index}`}
          onClick={() => onLegendClick?.(entry.dataKey as string)}
          className="flex items-center gap-2 px-3 py-1.5 rounded-md hover:bg-gray-50 transition-colors"
          style={{
            opacity: entry.inactive ? 0.5 : 1,
          }}
        >
          <div
            className="size-4 rounded-full"
            style={{
              backgroundColor: entry.color,
            }}
          />
          <span className="text-xs font-medium">{entry.value}</span>
        </button>
      ))}
    </div>
  );
}
