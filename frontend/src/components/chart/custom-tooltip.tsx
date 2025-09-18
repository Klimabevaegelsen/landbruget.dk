import { TooltipProps } from 'recharts';
import {
  NameType,
  ValueType,
} from 'recharts/types/component/DefaultTooltipContent';

interface CustomTooltipProps extends TooltipProps<ValueType, NameType> {
  unit?: string;
}

export default function CustomTooltip({
  active,
  payload,
  label,
  unit,
}: CustomTooltipProps) {
  if (!active || !payload || !payload.length) {
    return null;
  }

  return (
    <div className="bg-background rounded-lg border border-gray-200 p-4 shadow-md">
      <p className="text-base font-semibold">{label}</p>
      {payload.map((entry, index) => (
        <p
          key={`item-${index}`}
          style={{
            color: entry.color,
          }}
          className="mt-1 text-sm font-medium"
        >
          <span>
            {entry.name}: {entry.value?.toLocaleString('da-DK')}
            {unit && (
              <span className="text-muted-foreground bg-muted ml-2 rounded px-1 py-0.5 text-xs font-medium">
                {unit}
              </span>
            )}
          </span>
        </p>
      ))}
    </div>
  );
}
