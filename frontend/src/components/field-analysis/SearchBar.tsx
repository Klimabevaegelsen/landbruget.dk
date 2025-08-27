'use client';

import { useState, useRef, useEffect, useCallback } from 'react';
import { Search, X, MapPin } from 'lucide-react';
import { useLoadingToast } from '@/hooks/useLoadingToast';

interface DAWAResult {
  tekst: string;
  adresse?: {
    id: string;
    href: string;
    x: number;
    y: number;
  };
}

interface SearchBarProps {
  onLocationSelect: (location: {
    lat: number;
    lng: number;
    address: string;
  }) => void;
  placeholder?: string;
  className?: string;
}

export function SearchBar({
  onLocationSelect,
  placeholder = 'Søg adresser, byer, regioner...',
  className = '',
}: SearchBarProps) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<DAWAResult[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isOpen, setIsOpen] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(-1);

  const searchRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const debounceRef = useRef<NodeJS.Timeout | undefined>(undefined);

  const { showLoadingToast, hideLoadingToast } = useLoadingToast();

  const handleSelectResult = useCallback(
    async (result: DAWAResult) => {
      setQuery(result.tekst);
      setIsOpen(false);
      setSelectedIndex(-1);

      // Show loading toast for address lookup
      showLoadingToast(
        'Finder lokation',
        `Henter koordinater for ${result.tekst}...`
      );

      let locationFound = false;

      try {
        // Try to get detailed address information including coordinates
        if (result.adresse?.href) {
          try {
            const response = await fetch(result.adresse.href);
            if (response.ok) {
              const detailData = await response.json();
              if (detailData.adgangsadresse?.koordinater) {
                const [lng, lat] = detailData.adgangsadresse.koordinater;
                onLocationSelect({
                  lat,
                  lng,
                  address: result.tekst,
                });
                locationFound = true;
              }
            }
          } catch (error) {
            console.error('Error fetching address details:', error);
          }
        }

        // Fall back to coordinates from autocomplete result if detailed fetch failed or had no coordinates
        if (!locationFound && result.adresse?.x && result.adresse?.y) {
          onLocationSelect({
            lat: result.adresse.y,
            lng: result.adresse.x,
            address: result.tekst,
          });
          locationFound = true;
        }

        // Log if no coordinates were found at all
        if (!locationFound) {
          console.warn('No coordinates found for address:', result.tekst);
        }
      } finally {
        // Always remove the loading toast
        hideLoadingToast();
      }
    },
    [onLocationSelect, showLoadingToast, hideLoadingToast]
  );

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        searchRef.current &&
        !searchRef.current.contains(event.target as Node)
      ) {
        setIsOpen(false);
        setSelectedIndex(-1);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Handle keyboard navigation
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (!isOpen) return;

      switch (event.key) {
        case 'ArrowDown':
          event.preventDefault();
          setSelectedIndex((prev) =>
            prev < results.length - 1 ? prev + 1 : prev
          );
          break;
        case 'ArrowUp':
          event.preventDefault();
          setSelectedIndex((prev) => (prev > 0 ? prev - 1 : -1));
          break;
        case 'Enter':
          event.preventDefault();
          if (selectedIndex >= 0 && results[selectedIndex]) {
            handleSelectResult(results[selectedIndex]);
          }
          break;
        case 'Escape':
          setIsOpen(false);
          setSelectedIndex(-1);
          inputRef.current?.blur();
          break;
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, results, selectedIndex, handleSelectResult]);

  const searchDAWA = async (searchQuery: string) => {
    if (searchQuery.length < 2) {
      setResults([]);
      return;
    }

    setIsLoading(true);
    try {
      // Use DAWA autocomplete API
      const response = await fetch(
        `https://api.dataforsyningen.dk/adresser/autocomplete?q=${encodeURIComponent(searchQuery)}&fuzzy=true&per_side=8`,
        {
          method: 'GET',
          headers: {
            Accept: 'application/json',
          },
        }
      );

      if (response.ok) {
        const data = await response.json();
        setResults(data || []);
      } else {
        console.error('DAWA API error:', response.status);
        setResults([]);
      }
    } catch (error) {
      console.error('Error searching DAWA:', error);
      setResults([]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setQuery(value);
    setSelectedIndex(-1);

    // Clear existing timeout
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }

    // Debounce search requests
    debounceRef.current = setTimeout(() => {
      if (value.trim()) {
        searchDAWA(value.trim());
        setIsOpen(true);
      } else {
        setResults([]);
        setIsOpen(false);
      }
    }, 300);
  };

  const clearSearch = () => {
    setQuery('');
    setResults([]);
    setIsOpen(false);
    setSelectedIndex(-1);

    // Remove any existing location toast when clearing search
    hideLoadingToast();

    inputRef.current?.focus();
  };

  return (
    <div ref={searchRef} className={`relative ${className}`}>
      {/* Search Input */}
      <div className="relative">
        <div className="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3">
          <Search className="h-4 w-4 text-gray-400" />
        </div>

        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={handleInputChange}
          placeholder={placeholder}
          className="block w-full rounded-lg border border-gray-300 bg-white py-3 pr-10 pl-10 text-base text-gray-900 placeholder-gray-500 transition-colors focus:border-transparent focus:ring-2 focus:ring-blue-500 focus:outline-none lg:py-2.5 lg:text-sm"
        />

        {/* Clear Button */}
        {query && (
          <button
            onClick={clearSearch}
            className="absolute inset-y-0 right-0 flex items-center pr-3 text-gray-400 transition-colors hover:text-gray-600"
          >
            <X className="h-4 w-4" />
          </button>
        )}
      </div>

      {/* Results Dropdown */}
      {isOpen && (
        <div className="absolute z-50 mt-1 max-h-64 w-full overflow-y-auto rounded-lg border border-gray-200 bg-white shadow-lg">
          {isLoading && (
            <div className="px-4 py-3 text-center">
              <div className="inline-flex items-center space-x-2 text-gray-500">
                <div className="h-4 w-4 animate-spin rounded-full border-2 border-gray-300 border-t-blue-500"></div>
                <span className="text-sm">Søger...</span>
              </div>
            </div>
          )}

          {!isLoading && results.length === 0 && query.length >= 2 && (
            <div className="px-4 py-3 text-center text-sm text-gray-500">
              Ingen resultater fundet
            </div>
          )}

          {!isLoading &&
            results.map((result, index) => (
              <button
                key={`${result.tekst}-${index}`}
                onClick={() => handleSelectResult(result)}
                className={`w-full border-b border-gray-100 px-4 py-4 text-left transition-colors last:border-b-0 hover:bg-gray-50 lg:py-3 ${
                  index === selectedIndex
                    ? 'bg-blue-50 text-blue-900'
                    : 'text-gray-900'
                }`}
              >
                <div className="flex items-center space-x-3">
                  <MapPin className="h-5 w-5 flex-shrink-0 text-gray-400 lg:h-4 lg:w-4" />
                  <span className="truncate text-base font-medium lg:text-sm">
                    {result.tekst}
                  </span>
                </div>
              </button>
            ))}
        </div>
      )}
    </div>
  );
}
