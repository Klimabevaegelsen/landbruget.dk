'use client';

interface AllocationBarProps {
  name: string;
  areaLabel: string;
  dosage: number;
  percentage: number;
}

export function AllocationBar({
  name,
  areaLabel,
  dosage,
  percentage,
}: AllocationBarProps) {
  const widthStyle = { width: `${percentage}%` };

  return (
    <tr
      className="border-border/50 border-b"
      data-testid={`allocation-${name}`}
    >
      <td className="text-foreground/80 py-2 font-mono">{name}</td>
      <td className="text-muted-foreground py-2 text-right font-mono">
        {areaLabel}
      </td>
      <td className="text-muted-foreground py-2 text-right font-mono">
        {percentage}%
      </td>
      <td className="text-foreground py-2 text-right font-mono font-medium">
        {dosage} L
      </td>
      <td className="py-2 pl-4">
        <div className="bg-muted h-4 w-full overflow-hidden rounded-sm">
          <div
            className="bg-muted-foreground/40 h-full rounded-sm transition-all duration-300"
            style={widthStyle}
          />
        </div>
      </td>
    </tr>
  );
}
