import { cn } from '@/lib/utils';
import { MapPin } from 'lucide-react';
import type { AdressevaelgerResult } from '@/components/pesticidkort/address-utils';

interface AddressDropdownProps {
  listboxId: string;
  results: AdressevaelgerResult[];
  isLoading: boolean;
  queryLength: number;
  selectedIdx: number;
  onSelect: (r: AdressevaelgerResult) => void;
  error?: boolean;
}

export function AddressDropdown({
  listboxId,
  results,
  isLoading,
  queryLength,
  selectedIdx,
  onSelect,
  error = false,
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
      {!isLoading && error && (
        <div
          className="text-muted-foreground px-4 py-3 text-center text-sm"
          role="status"
        >
          Adresseopslag kunne ikke gennemføres. Prøv igen.
        </div>
      )}
      {!isLoading && !error && results.length === 0 && queryLength >= 2 && (
        <div className="text-muted-foreground px-4 py-3 text-center text-sm">
          Ingen resultater
        </div>
      )}
      {!isLoading &&
        !error &&
        results.map((r, i) => (
          <button
            key={`${r.id}-${i}`}
            type="button"
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
            <span className="truncate">{r.titel}</span>
          </button>
        ))}
    </div>
  );
}
