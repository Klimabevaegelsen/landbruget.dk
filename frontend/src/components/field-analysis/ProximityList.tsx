import {
  formatDistanceFromField,
  type ProximityEntry,
} from '@/lib/proximity-parser';

interface ProximityListProps {
  heading: string;
  entries: ProximityEntry[];
}

export function ProximityList({ heading, entries }: ProximityListProps) {
  if (entries.length === 0) return null;

  return (
    <div className="space-y-1">
      <h4 className="text-muted-foreground text-xs font-medium lg:text-sm">
        {heading}
      </h4>
      <ul className="space-y-1 text-xs font-medium lg:text-sm">
        {entries.map((entry) => (
          <li key={`${entry.address}-${entry.distanceM}`}>
            {entry.address}
            {entry.buildingType ? ` (${entry.buildingType})` : ''} –{' '}
            {formatDistanceFromField(entry.distanceM)}
          </li>
        ))}
      </ul>
    </div>
  );
}
