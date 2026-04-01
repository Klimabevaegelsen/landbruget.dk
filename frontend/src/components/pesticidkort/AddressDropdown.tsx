import { cn } from '@/lib/utils';
import { MapPin } from 'lucide-react';
import type { DAWAResult } from '@/components/pesticidkort/address-utils';

interface AddressDropdownProps {
  listboxId: string;
  results: DAWAResult[];
  isLoading: boolean;
  queryLength: number;
  selectedIdx: number;
  onSelect: (r: DAWAResult) => void;
}

export function AddressDropdown({
  listboxId,
  results,
  isLoading,
  queryLength,
  selectedIdx,
  onSelect,
}: AddressDropdownProps) {
  return (
    <div
      id={listboxId}
      role="listbox"
      className="bg-background border-border absolute z-50 mt-2 w-full overflow-hidden rounded-xl border shadow-xl"
    >
      {isLoading && (
        <div className="text-muted-foreground px-4 py-3 text-center text-sm">
          Søger...
        </div>
      )}
      {!isLoading && results.length === 0 && queryLength >= 2 && (
        <div className="text-muted-foreground px-4 py-3 text-center text-sm">
          Ingen resultater
        </div>
      )}
      {results.map((r, i) => (
        <button
          key={`${r.tekst}-${i}`}
          id={`address-option-${i}`}
          role="option"
          aria-selected={i === selectedIdx}
          onClick={() => onSelect(r)}
          data-testid={`landing-result-${i}-button`}
          className={cn(
            'text-foreground hover:bg-muted flex w-full items-center gap-3 px-4 py-3 text-left text-sm transition-colors',
            i === selectedIdx && 'bg-accent'
          )}
        >
          <MapPin className="text-muted-foreground h-4 w-4 shrink-0" />
          <span className="truncate">{r.tekst}</span>
        </button>
      ))}
    </div>
  );
}
