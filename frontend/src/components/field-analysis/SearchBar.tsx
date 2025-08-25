'use client';

import { useState, useRef, useEffect, useCallback } from 'react';
import { Search, X, MapPin } from 'lucide-react';

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
  onLocationSelect: (location: { lat: number; lng: number; address: string }) => void;
  placeholder?: string;
  className?: string;
}

export function SearchBar({
  onLocationSelect,
  placeholder = "Søg adresser, byer, regioner...",
  className = ""
}: SearchBarProps) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<DAWAResult[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isOpen, setIsOpen] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(-1);

  const searchRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const debounceRef = useRef<NodeJS.Timeout | undefined>(undefined);

  const handleSelectResult = useCallback(async (result: DAWAResult) => {
    setQuery(result.tekst);
    setIsOpen(false);
    setSelectedIndex(-1);

    let locationFound = false;

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
              address: result.tekst
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
        address: result.tekst
      });
      locationFound = true;
    }

    // Log if no coordinates were found at all
    if (!locationFound) {
      console.warn('No coordinates found for address:', result.tekst);
    }
  }, [onLocationSelect]);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (searchRef.current && !searchRef.current.contains(event.target as Node)) {
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
          setSelectedIndex(prev =>
            prev < results.length - 1 ? prev + 1 : prev
          );
          break;
        case 'ArrowUp':
          event.preventDefault();
          setSelectedIndex(prev => prev > 0 ? prev - 1 : -1);
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
            'Accept': 'application/json',
          }
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
    inputRef.current?.focus();
  };

  return (
    <div ref={searchRef} className={`relative ${className}`}>
      {/* Search Input */}
      <div className="relative">
        <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
          <Search className="h-4 w-4 text-gray-400" />
        </div>

        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={handleInputChange}
          placeholder={placeholder}
          className="block w-full pl-10 pr-10 py-3 lg:py-2.5 border border-gray-300 rounded-lg bg-white text-gray-900 placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-colors text-base lg:text-sm"
        />

        {/* Clear Button */}
        {query && (
          <button
            onClick={clearSearch}
            className="absolute inset-y-0 right-0 pr-3 flex items-center text-gray-400 hover:text-gray-600 transition-colors"
          >
            <X className="h-4 w-4" />
          </button>
        )}
      </div>

      {/* Results Dropdown */}
      {isOpen && (
        <div className="absolute z-50 w-full mt-1 bg-white border border-gray-200 rounded-lg shadow-lg max-h-64 overflow-y-auto">
          {isLoading && (
            <div className="px-4 py-3 text-center">
              <div className="inline-flex items-center space-x-2 text-gray-500">
                <div className="w-4 h-4 border-2 border-gray-300 border-t-blue-500 rounded-full animate-spin"></div>
                <span className="text-sm">Søger...</span>
              </div>
            </div>
          )}

          {!isLoading && results.length === 0 && query.length >= 2 && (
            <div className="px-4 py-3 text-center text-gray-500 text-sm">
              Ingen resultater fundet
            </div>
          )}

          {!isLoading && results.map((result, index) => (
            <button
              key={`${result.tekst}-${index}`}
              onClick={() => handleSelectResult(result)}
              className={`w-full px-4 py-4 lg:py-3 text-left hover:bg-gray-50 transition-colors border-b border-gray-100 last:border-b-0 ${
                index === selectedIndex ? 'bg-blue-50 text-blue-900' : 'text-gray-900'
              }`}
            >
              <div className="flex items-center space-x-3">
                <MapPin className="h-5 w-5 lg:h-4 lg:w-4 text-gray-400 flex-shrink-0" />
                <span className="text-base lg:text-sm font-medium truncate">{result.tekst}</span>
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
