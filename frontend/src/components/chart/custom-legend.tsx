import { Payload } from 'recharts/types/component/DefaultLegendContent';

interface CustomLegendProps {
  payload?: Payload[];
  onLegendClick?: (dataKey: string) => void;
}

export function CustomLegend({ payload, onLegendClick }: CustomLegendProps) {
  if (!payload || !payload.length) {
    return null;
  }

  return (
    <div className="flex flex-wrap gap-2">
      {payload.map((entry, index) => {
        const buttonStyle = { opacity: entry.inactive ? 0.5 : 1 };
        const dotStyle = { backgroundColor: entry.color };
        return (
          <button
            key={`legend-item-${index}`}
            onClick={() => onLegendClick?.(entry.dataKey as string)}
            data-testid={`legend-${entry.dataKey}-button`}
            className="hover:bg-muted flex items-center gap-2 rounded-md px-3 py-1.5 transition-colors"
            style={buttonStyle}
          >
            <div className="size-4 rounded-full" style={dotStyle} />
            <span className="text-xs font-medium">{entry.value}</span>
          </button>
        );
      })}
    </div>
  );
}
