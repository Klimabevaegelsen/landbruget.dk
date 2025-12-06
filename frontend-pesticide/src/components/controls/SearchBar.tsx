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
  onLocationSelect?: (location: { lat: number; lng: number; address: string }) => void;
  placeholder?: string;
  className?: string;
}

export function SearchBar({ 
  onLocationSelect, 
  placeholder = "Search address...",
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

    // Get detailed address information including coordinates
    if (result.adresse?.href) {
      try {
        const response = await fetch(result.adresse.href);
        if (response.ok) {
          const detailData = await response.json();
          if (detailData.adgangsadresse?.koordinater) {
            const [lng, lat] = detailData.adgangsadresse.koordinater;
            onLocationSelect?.({
              lat,
              lng,
              address: result.tekst
            });
          }
        }
      } catch (error) {
        console.error('Error fetching address details:', error);
      }
    } else if (result.adresse?.x && result.adresse?.y) {
      // Use coordinates from autocomplete result if available
      onLocationSelect?.({
        lat: result.adresse.y,
        lng: result.adresse.x,
        address: result.tekst
      });
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
    if (debounceRef.current !== undefined) {
      clearTimeout(debounceRef.current);
    }

    // Debounce search
    debounceRef.current = setTimeout(() => {
      searchDAWA(value);
    }, 300);

    setIsOpen(value.length > 0);
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
      <div className="relative">
        <div className="absolute left-4 top-1/2 transform -translate-y-1/2 text-slate-400">
          <Search className="w-5 h-5" />
        </div>
        
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={handleInputChange}
          onFocus={() => query.length > 0 && setIsOpen(true)}
          placeholder={placeholder}
          className="w-full pl-12 pr-12 py-3 bg-slate-800 border border-slate-600 rounded-lg text-white placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all duration-200 text-base"
        />
        
        {query && (
          <button
            onClick={clearSearch}
            className="absolute right-4 top-1/2 transform -translate-y-1/2 text-slate-400 hover:text-white transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        )}
      </div>

      {/* Loading indicator */}
      {isLoading && (
        <div className="absolute right-4 top-1/2 transform -translate-y-1/2">
          <div className="w-5 h-5 border-2 border-slate-400 border-t-transparent rounded-full animate-spin"></div>
        </div>
      )}

      {/* Results dropdown */}
      {isOpen && (results.length > 0 || (!isLoading && query.length >= 2)) && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-slate-800 border border-slate-600 rounded-lg shadow-xl z-50 max-h-64 overflow-y-auto">
          {results.length > 0 ? (
            results.map((result, index) => (
              <button
                key={index}
                onClick={() => handleSelectResult(result)}
                className={`w-full px-4 py-3 text-left hover:bg-slate-700 transition-colors border-b border-slate-600 last:border-b-0 ${
                  index === selectedIndex ? 'bg-slate-700' : ''
                }`}
              >
                <div className="flex items-start space-x-3">
                  <MapPin className="w-4 h-4 text-slate-400 mt-0.5 flex-shrink-0" />
                  <div className="flex-1 min-w-0">
                    <div className="text-white text-sm font-medium truncate">
                      {result.tekst}
                    </div>
                  </div>
                </div>
              </button>
            ))
          ) : (
            <div className="px-4 py-3 text-slate-400 text-sm">
              No addresses found
            </div>
          )}
        </div>
      )}
    </div>
  );
} 