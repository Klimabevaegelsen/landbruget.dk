'use client';

import { useCallback, useEffect, useRef } from 'react';
import { MapPin, Search, X } from 'lucide-react';
import { toast } from 'sonner';
import { useLoadingToast } from '@/hooks/useLoadingToast';
import { useAdressevaelgerSearch } from '@/components/pesticidkort/useAdressevaelgerSearch';

interface SearchBarProps {
  onLocationSelect: (location: {
    lat: number;
    lng: number;
    address: string;
  }) => void;
  placeholder?: string;
  className?: string;
  onSearchStateChange?: (isOpen: boolean) => void;
}

const LISTBOX_ID = 'field-address-search-listbox';

export function SearchBar({
  onLocationSelect,
  placeholder = 'Søg efter adresse...',
  className = '',
  onSearchStateChange,
}: SearchBarProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const { showLoadingToast, hideLoadingToast } = useLoadingToast();
  const onResolveStart = useCallback(
    (address: string) =>
      showLoadingToast(
        'Finder lokation',
        `Henter koordinater for ${address}...`
      ),
    [showLoadingToast]
  );
  const onResolveError = useCallback(() => {
    toast.error('Adressen kunne ikke slås op. Prøv igen.');
  }, []);
  const search = useAdressevaelgerSearch({
    onSelect: onLocationSelect,
    onResolveStart,
    onResolveEnd: hideLoadingToast,
    onResolveError,
  });

  useEffect(() => {
    onSearchStateChange?.(search.isOpen);
  }, [onSearchStateChange, search.isOpen]);

  const clearSearch = () => {
    search.changeQuery('');
    inputRef.current?.focus();
  };

  return (
    <div
      ref={search.containerRef}
      className={`relative ${className}`}
      onKeyDown={search.handleKeyDown}
    >
      <div className="relative">
        <div className="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3">
          <Search className="text-muted-foreground h-4 w-4" />
        </div>
        <input
          ref={inputRef}
          type="text"
          value={search.query}
          onChange={(event) => search.changeQuery(event.target.value)}
          placeholder={placeholder}
          maxLength={73}
          data-testid="address-search-input"
          role="combobox"
          aria-expanded={search.isOpen}
          aria-controls={LISTBOX_ID}
          aria-activedescendant={
            search.selectedIndex >= 0
              ? `field-address-option-${search.selectedIndex}`
              : undefined
          }
          aria-autocomplete="list"
          aria-label="Søg efter adresse"
          className="border-border text-foreground placeholder:text-muted-foreground bg-background/95 focus:ring-ring block w-full rounded-lg border py-3 pr-10 pl-10 text-base shadow-lg backdrop-blur-sm transition-colors focus:border-transparent focus:ring-2 focus:outline-none lg:py-2.5 lg:text-sm"
        />
        {search.query && (
          <button
            type="button"
            onClick={clearSearch}
            data-testid="clear-search-button"
            aria-label="Ryd søgefelt"
            className="hover:text-muted-foreground text-muted-foreground absolute inset-y-0 right-0 flex items-center pr-3 transition-colors"
          >
            <X className="h-4 w-4" />
          </button>
        )}
      </div>

      {search.isOpen && (
        <div
          id={LISTBOX_ID}
          role="listbox"
          aria-label="Adresseforslag"
          className="bg-background/95 border-border absolute z-[100] mt-1 max-h-64 w-full overflow-y-auto rounded-lg border shadow-xl backdrop-blur-sm"
        >
          {search.isLoading && (
            <div className="px-4 py-3 text-center" role="status">
              <div className="text-muted-foreground inline-flex items-center space-x-2">
                <div className="border-border border-t-primary h-4 w-4 animate-spin rounded-full border-2" />
                <span className="text-sm">Søger...</span>
              </div>
            </div>
          )}

          {!search.isLoading && search.hasError && (
            <div
              className="text-muted-foreground px-4 py-3 text-center text-sm"
              role="status"
            >
              Adresseopslag kunne ikke gennemføres. Prøv igen.
            </div>
          )}

          {!search.isLoading &&
            !search.hasError &&
            search.results.length === 0 &&
            search.query.length >= 2 && (
              <div className="text-muted-foreground px-4 py-3 text-center text-sm">
                Ingen resultater fundet
              </div>
            )}

          {!search.isLoading &&
            !search.hasError &&
            search.results.map((result, index) => (
              <button
                key={result.id}
                type="button"
                role="option"
                aria-selected={index === search.selectedIndex}
                id={`field-address-option-${index}`}
                onClick={() => void search.selectResult(result)}
                data-testid={`search-result-${index}-button`}
                className={`hover:bg-muted border-border w-full border-b px-4 py-4 text-left transition-colors last:border-b-0 lg:py-3 ${
                  index === search.selectedIndex
                    ? 'bg-accent text-accent-foreground'
                    : 'text-foreground'
                }`}
              >
                <div className="flex items-center space-x-3">
                  <MapPin className="text-muted-foreground h-5 w-5 flex-shrink-0 lg:h-4 lg:w-4" />
                  <span className="truncate text-base font-medium lg:text-sm">
                    {result.titel}
                  </span>
                </div>
              </button>
            ))}
        </div>
      )}
    </div>
  );
}
