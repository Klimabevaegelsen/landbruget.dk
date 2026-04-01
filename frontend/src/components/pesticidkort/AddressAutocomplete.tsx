'use client';

import { useState, useRef, useEffect, useCallback } from 'react';
import { Search, X } from 'lucide-react';
import type { AddressResult } from '@/components/pesticidkort/types';
import type { DAWAResult } from '@/components/pesticidkort/address-utils';
import { resolveCoordinates } from '@/components/pesticidkort/address-utils';
import { AddressDropdown } from '@/components/pesticidkort/AddressDropdown';

interface AddressAutocompleteProps {
  onSelect: (result: AddressResult) => void;
}

export function AddressAutocomplete({ onSelect }: AddressAutocompleteProps) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<DAWAResult[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [selectedIdx, setSelectedIdx] = useState(-1);
  const [isLoading, setIsLoading] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node))
        setIsOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const selectResult = useCallback(
    async (r: DAWAResult) => {
      setQuery(r.tekst);
      setIsOpen(false);
      const result = await resolveCoordinates(r);
      if (result) onSelect(result);
    },
    [onSelect]
  );

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (!isOpen) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSelectedIdx((i) => Math.min(i + 1, results.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSelectedIdx((i) => Math.max(i - 1, -1));
    } else if (e.key === 'Enter' && selectedIdx >= 0) {
      e.preventDefault();
      selectResult(results[selectedIdx]);
    } else if (e.key === 'Escape') {
      setIsOpen(false);
    }
  };

  const onChange = (val: string) => {
    setQuery(val);
    setSelectedIdx(-1);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    if (val.trim().length < 2) {
      setResults([]);
      setIsOpen(false);
      return;
    }
    debounceRef.current = setTimeout(async () => {
      setIsLoading(true);
      try {
        const url = `https://api.dataforsyningen.dk/adresser/autocomplete?q=${encodeURIComponent(val)}&fuzzy=true&per_side=8`;
        const data = await fetch(url).then((r) => r.json());
        setResults(data || []);
        setIsOpen(true);
      } catch {
        setResults([]);
      } finally {
        setIsLoading(false);
      }
    }, 300);
  };

  const listboxId = 'address-autocomplete-listbox';

  return (
    <div ref={ref} className="relative" onKeyDown={onKeyDown}>
      <div className="relative">
        <Search className="text-muted-foreground pointer-events-none absolute top-1/2 left-4 h-5 w-5 -translate-y-1/2" />
        <input
          type="text"
          value={query}
          onChange={(e) => onChange(e.target.value)}
          placeholder="Indtast din adresse..."
          data-testid="landing-address-input"
          role="combobox"
          aria-expanded={isOpen}
          aria-controls={listboxId}
          aria-activedescendant={
            selectedIdx >= 0 ? `address-option-${selectedIdx}` : undefined
          }
          aria-autocomplete="list"
          aria-label="Søg efter adresse"
          className="border-border bg-background text-foreground placeholder:text-muted-foreground focus:ring-primary h-14 w-full rounded-full border py-3 pr-12 pl-12 text-lg shadow-sm transition-shadow focus:shadow-md focus:ring-2 focus:outline-none"
        />
        {query && (
          <button
            onClick={() => {
              setQuery('');
              setResults([]);
              setIsOpen(false);
            }}
            data-testid="landing-clear-button"
            aria-label="Ryd søgefelt"
            className="text-muted-foreground hover:bg-muted absolute top-1/2 right-2 flex h-10 w-10 -translate-y-1/2 items-center justify-center rounded-full transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        )}
      </div>
      {isOpen && (
        <AddressDropdown
          listboxId={listboxId}
          results={results}
          isLoading={isLoading}
          queryLength={query.length}
          selectedIdx={selectedIdx}
          onSelect={selectResult}
        />
      )}
    </div>
  );
}
