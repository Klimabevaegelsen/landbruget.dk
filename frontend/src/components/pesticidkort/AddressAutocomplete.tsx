'use client';

import { Search, X } from 'lucide-react';
import type { AddressResult } from '@/components/pesticidkort/types';
import { AddressDropdown } from '@/components/pesticidkort/AddressDropdown';
import { useAdressevaelgerSearch } from '@/components/pesticidkort/useAdressevaelgerSearch';

interface AddressAutocompleteProps {
  onSelect: (result: AddressResult) => void;
}

const LISTBOX_ID = 'address-autocomplete-listbox';

export function AddressAutocomplete({ onSelect }: AddressAutocompleteProps) {
  const search = useAdressevaelgerSearch({ onSelect });

  return (
    <div
      ref={search.containerRef}
      className="relative"
      onKeyDown={search.handleKeyDown}
    >
      <div className="relative">
        <Search className="text-muted-foreground pointer-events-none absolute top-1/2 left-4 h-5 w-5 -translate-y-1/2" />
        <input
          type="text"
          value={search.query}
          onChange={(event) => search.changeQuery(event.target.value)}
          placeholder="Indtast din adresse..."
          maxLength={73}
          data-testid="landing-address-input"
          role="combobox"
          aria-expanded={search.isOpen}
          aria-controls={LISTBOX_ID}
          aria-activedescendant={
            search.selectedIndex >= 0
              ? `address-option-${search.selectedIndex}`
              : undefined
          }
          aria-autocomplete="list"
          aria-label="Søg efter adresse"
          className="border-border bg-background text-foreground placeholder:text-muted-foreground focus:ring-primary h-14 w-full rounded-full border py-3 pr-12 pl-12 text-lg shadow-sm transition-shadow focus:shadow-md focus:ring-2 focus:outline-none"
        />
        {search.query && (
          <button
            type="button"
            onClick={() => search.changeQuery('')}
            data-testid="landing-clear-button"
            aria-label="Ryd søgefelt"
            className="text-muted-foreground hover:bg-muted absolute top-1/2 right-2 flex h-10 w-10 -translate-y-1/2 items-center justify-center rounded-full transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        )}
      </div>
      {search.isOpen && (
        <AddressDropdown
          listboxId={LISTBOX_ID}
          results={search.results}
          isLoading={search.isLoading}
          queryLength={search.query.length}
          selectedIdx={search.selectedIndex}
          onSelect={(result) => void search.selectResult(result)}
          error={search.hasError}
        />
      )}
    </div>
  );
}
