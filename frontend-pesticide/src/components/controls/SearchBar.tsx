'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { MapPin, Search, X } from 'lucide-react';
import {
  isSelectableAddress,
  resolveCoordinates,
  searchAddresses,
  type AdressevaelgerResult,
} from '@/lib/adressevaelger';

interface SearchBarProps {
  onLocationSelect?: (location: {
    lat: number;
    lng: number;
    address: string;
  }) => void;
  placeholder?: string;
  className?: string;
}

interface SearchRequest {
  id: number;
  signal: AbortSignal;
}

const LISTBOX_ID = 'pesticide-address-search-listbox';

export function SearchBar({
  onLocationSelect,
  placeholder = 'Search address...',
  className = '',
}: SearchBarProps) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<AdressevaelgerResult[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isOpen, setIsOpen] = useState(false);
  const [hasError, setHasError] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(-1);

  const searchRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const requestIdRef = useRef(0);
  const controllerRef = useRef<AbortController | null>(null);

  const beginRequest = useCallback((): SearchRequest => {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    requestIdRef.current += 1;
    return { id: requestIdRef.current, signal: controller.signal };
  }, []);

  const cancelRequest = useCallback(() => {
    controllerRef.current?.abort();
    controllerRef.current = null;
    requestIdRef.current += 1;
    setIsLoading(false);
  }, []);

  const runSearch = useCallback(
    async (searchQuery: string, request: SearchRequest) => {
      try {
        const addresses = await searchAddresses(searchQuery, request.signal);
        if (request.id !== requestIdRef.current) return;
        setResults(addresses);
        setHasError(false);
        setIsOpen(true);
      } catch (error) {
        if (error instanceof Error && error.name === 'AbortError') return;
        if (request.id !== requestIdRef.current) return;
        setResults([]);
        setHasError(true);
        setIsOpen(true);
      } finally {
        if (request.id === requestIdRef.current) setIsLoading(false);
      }
    },
    []
  );

  const changeQuery = useCallback(
    (value: string) => {
      setQuery(value);
      setResults([]);
      setHasError(false);
      setSelectedIndex(-1);
      if (debounceRef.current) clearTimeout(debounceRef.current);
      cancelRequest();

      if (value.trim().length < 2) {
        setIsOpen(false);
        return;
      }

      const request = beginRequest();
      setIsOpen(true);
      setIsLoading(true);
      debounceRef.current = setTimeout(() => {
        void runSearch(value, request);
      }, 300);
    },
    [beginRequest, cancelRequest, runSearch]
  );

  const close = useCallback(() => {
    setIsOpen(false);
    setSelectedIndex(-1);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = null;
    cancelRequest();
  }, [cancelRequest]);

  const handleSelectResult = useCallback(
    async (result: AdressevaelgerResult) => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = null;
      cancelRequest();
      setQuery(result.titel);
      setResults([]);
      setHasError(false);
      setSelectedIndex(-1);

      const request = beginRequest();
      setIsLoading(true);
      setIsOpen(true);

      if (!isSelectableAddress(result)) {
        await runSearch(result.titel, request);
        return;
      }

      try {
        const location = await resolveCoordinates(result, request.signal);
        if (request.id !== requestIdRef.current) return;
        if (!location) {
          setHasError(true);
          return;
        }
        setQuery(location.address);
        setIsOpen(false);
        onLocationSelect?.(location);
      } catch (error) {
        if (error instanceof Error && error.name === 'AbortError') return;
        if (request.id === requestIdRef.current) setHasError(true);
      } finally {
        if (request.id === requestIdRef.current) setIsLoading(false);
      }
    },
    [beginRequest, cancelRequest, onLocationSelect, runSearch]
  );

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        searchRef.current &&
        !searchRef.current.contains(event.target as Node)
      ) {
        close();
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [close]);

  useEffect(
    () => () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      controllerRef.current?.abort();
      requestIdRef.current += 1;
    },
    []
  );

  const handleKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
    if (!isOpen) return;

    switch (event.key) {
      case 'ArrowDown':
        event.preventDefault();
        setSelectedIndex((previous) =>
          previous < results.length - 1 ? previous + 1 : previous
        );
        break;
      case 'ArrowUp':
        event.preventDefault();
        setSelectedIndex((previous) => (previous > 0 ? previous - 1 : -1));
        break;
      case 'Enter':
        event.preventDefault();
        if (selectedIndex >= 0 && results[selectedIndex]) {
          void handleSelectResult(results[selectedIndex]);
        }
        break;
      case 'Escape':
        close();
        inputRef.current?.blur();
        break;
    }
  };

  const clearSearch = () => {
    changeQuery('');
    inputRef.current?.focus();
  };

  const queryLength = query.trim().length;

  return (
    <div
      ref={searchRef}
      className={`relative ${className}`}
      onKeyDown={handleKeyDown}
    >
      <div className="relative">
        <div className="absolute top-1/2 left-4 -translate-y-1/2 transform text-slate-400">
          <Search className="h-5 w-5" />
        </div>

        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={(event) => changeQuery(event.target.value)}
          onFocus={() => queryLength >= 2 && setIsOpen(true)}
          placeholder={placeholder}
          role="combobox"
          aria-expanded={isOpen}
          aria-controls={LISTBOX_ID}
          aria-activedescendant={
            selectedIndex >= 0
              ? `pesticide-address-option-${selectedIndex}`
              : undefined
          }
          aria-autocomplete="list"
          className="w-full rounded-lg border border-slate-600 bg-slate-800 py-3 pr-12 pl-12 text-base text-white placeholder-slate-400 transition-all duration-200 focus:border-blue-500 focus:ring-2 focus:ring-blue-500 focus:outline-none"
        />

        {query && (
          <button
            type="button"
            onClick={clearSearch}
            aria-label="Clear search"
            className="absolute top-1/2 right-4 -translate-y-1/2 transform text-slate-400 transition-colors hover:text-white"
          >
            <X className="h-5 w-5" />
          </button>
        )}
      </div>

      {isLoading && (
        <div className="absolute top-1/2 right-4 -translate-y-1/2 transform">
          <div className="h-5 w-5 animate-spin rounded-full border-2 border-slate-400 border-t-transparent"></div>
        </div>
      )}

      {isOpen &&
        (results.length > 0 ||
          ((hasError || !isLoading) && queryLength >= 2)) && (
          <div
            id={LISTBOX_ID}
            role="listbox"
            aria-label="Address suggestions"
            className="absolute top-full right-0 left-0 z-50 mt-1 max-h-64 overflow-y-auto rounded-lg border border-slate-600 bg-slate-800 shadow-xl"
          >
            {hasError ? (
              <div role="status" className="px-4 py-3 text-sm text-slate-400">
                Adresseopslag kunne ikke gennemføres. Prøv igen.
              </div>
            ) : results.length > 0 ? (
              results.map((result, index) => (
                <button
                  key={result.id}
                  type="button"
                  role="option"
                  aria-selected={index === selectedIndex}
                  id={`pesticide-address-option-${index}`}
                  onClick={() => void handleSelectResult(result)}
                  className={`w-full border-b border-slate-600 px-4 py-3 text-left transition-colors last:border-b-0 hover:bg-slate-700 ${
                    index === selectedIndex ? 'bg-slate-700' : ''
                  }`}
                >
                  <div className="flex items-start space-x-3">
                    <MapPin className="mt-0.5 h-4 w-4 flex-shrink-0 text-slate-400" />
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-sm font-medium text-white">
                        {result.titel}
                      </div>
                      {!isSelectableAddress(result) && (
                        <div className="mt-1 text-xs text-slate-400">
                          Vælg husnummer
                        </div>
                      )}
                    </div>
                  </div>
                </button>
              ))
            ) : (
              <div className="px-4 py-3 text-sm text-slate-400">
                No addresses found
              </div>
            )}
          </div>
        )}
    </div>
  );
}
