"use client";
import { useEffect, useState, useCallback } from "react";
import { Input } from "./ui/input";
import { cn } from "@/lib/utils";
import { MagnifyingGlassIcon } from "@heroicons/react/24/solid";
import { motion, AnimatePresence } from "framer-motion";
import React from "react";
import Image from "next/image";
import Link from "next/link";
import { Button } from "./ui/button";
import { apiFetch } from "@/services/supabase/config";

export function GlobalSearch({
  className,
  defaultOpen,
  parentOpen,
  onClose,
  borderless,
  searchSuggestions,
}: {
  className?: string;
  defaultOpen?: boolean;
  parentOpen?: boolean;
  onClose?: () => void;
  borderless?: boolean;
  searchSuggestions?: string[];
}) {
  const [search, setSearch] = useState("");
  const [open, setOpen] = useState(defaultOpen);

  useEffect(() => {
    if (parentOpen) setOpen(parentOpen);
  }, [parentOpen]);

  return (
    <div
      className={cn(
        " relative overflow flex flex-col gap-y-4 items-center w-full ",
        className,
        parentOpen === false && "hidden"
      )}
    >
      <Input
        type="text"
        placeholder="Søg efter CVR, firmanavn eller person"
        value={search}
        onChange={(e) => {
          setSearch(e.target.value);
          setOpen(e.target.value.length > 0);
        }}
        onFocus={() => setOpen(true)}
        className="px-auto h-12"
        endIcon={<MagnifyingGlassIcon className="size-6" />}
      />
      {searchSuggestions && (
        <div className=" flex items-center justify-center gap-x-2">
          {searchSuggestions.map((suggestion) => (
            <Button
              key={suggestion}
              variant="secondary"
              className="bg-white/75 hover:bg-white/90"
              onClick={() => {
                setSearch(suggestion);
                setOpen(true);
              }}
            >
              <MagnifyingGlassIcon />
              {suggestion}
            </Button>
          ))}
        </div>
      )}
      <AnimatePresence>
        {open && (
          <SearchOverlay
            search={search}
            setSearch={setSearch}
            borderless={borderless}
            onClose={() => {
              setOpen(false);
              onClose?.();
            }}
          />
        )}
      </AnimatePresence>
    </div>
  );
}

interface SearchResult {
  name: string;
  cvr: string;
  address: string;
  type: string;
  id: string;
}

interface SearchResponse {
  results: SearchResult[];
  total: number;
  query: string;
  searchType?: string;
}

function SearchOverlay({
  search,
  setSearch,
  onClose,
  borderless,
}: {
  search: string;
  setSearch: (v: string) => void;
  onClose: () => void;
  borderless?: boolean;
}) {
  // Tabs for categories
  const tabs = ["Alle", "CVR", "Firmanavn", "Person", "Lokation"];
  const [activeTab, setActiveTab] = useState(0);
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Debounced search function
  const performSearch = useCallback(async (query: string) => {
    if (!query || query.trim().length < 2) {
      setSearchResults([]);
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      // Map tab index to search type
      const searchTypeMap: { [key: number]: string } = {
        0: 'auto', // Alle
        1: 'cvr',  // CVR
        2: 'company_name', // Firmanavn
        3: 'person', // Person (not implemented yet)
        4: 'location' // Lokation (not implemented yet)
      };

      const searchType = searchTypeMap[activeTab] || 'auto';
      const response = await apiFetch(`/functions/v1/search?q=${encodeURIComponent(query.trim())}&type=${searchType}&limit=20`);

      if (!response.ok) {
        throw new Error('Search failed');
      }

      const data: SearchResponse = await response.json();
      setSearchResults(data.results || []);
    } catch (err) {
      console.error('Search error:', err);
      setError('Søgning fejlede. Prøv igen.');
      setSearchResults([]);
    } finally {
      setIsLoading(false);
    }
  }, [activeTab]);

  // Debounce search
  useEffect(() => {
    const timeoutId = setTimeout(() => {
      performSearch(search);
    }, 300); // 300ms debounce

    return () => clearTimeout(timeoutId);
  }, [search, performSearch]);

  // Re-search when tab changes
  useEffect(() => {
    if (search && search.trim().length >= 2) {
      performSearch(search);
    }
  }, [activeTab, search, performSearch]);

  // Close on Escape
  React.useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  // Close on click outside
  const overlayRef = React.useRef<HTMLDivElement>(null);
  React.useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (
        overlayRef.current &&
        !overlayRef.current.contains(e.target as Node)
      ) {
        onClose();
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [onClose]);

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.1 }}
      className="absolute  left-0 top-0 right-0 bottom-0 z-50  flex flex-col items-center justify-start"
      ref={overlayRef}
    >
      <div
        className={cn(
          "w-full  h-auto shadow-lg  rounded-lg",
          !borderless && "border border-gray-100"
        )}
      >
        <Input
          autoFocus
          type="text"
          placeholder="Søg efter CVR, firmanavn eller person"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="h-12 rounded-b-none rounded-t-lg border-none focus:ring-0 focus-visible:ring-0"
          endIcon={<MagnifyingGlassIcon className="size-6" />}
        />
        <div className="flex gap-2   bg-primary-foreground items-stretch overflow-x-auto">
          {tabs.map((tab, i) => (
            <div
              key={tab}
              onClick={() => setActiveTab(i)}
              className={cn(
                "flex-1 px-4 py-4 text-center text-xs cursor-pointer hover:font-semibold",
                activeTab === i &&
                  "border-b-2 border-b-primary font-bold hover:font-bold"
              )}
            >
              {tab}
            </div>
          ))}
        </div>
        <div className="bg-white rounded-b-lg min-h-[200px] max-h-[400px] overflow-auto">
          {isLoading && (
            <div className="flex items-center justify-center p-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
              <span className="ml-3 text-sm text-gray-600">Søger...</span>
            </div>
          )}

          {error && (
            <div className="flex items-center justify-center p-8">
              <div className="text-sm text-red-600">{error}</div>
            </div>
          )}

          {!isLoading && !error && searchResults.length === 0 && search.trim().length >= 2 && (
            <div className="flex items-center justify-center p-8">
              <div className="text-sm text-gray-500">Ingen resultater fundet for &ldquo;{search}&rdquo;</div>
            </div>
          )}

          {!isLoading && !error && search.trim().length < 2 && (
            <div className="flex items-center justify-center p-8">
              <div className="text-sm text-gray-500">Indtast mindst 2 tegn for at søge</div>
            </div>
          )}

          {!isLoading && !error && searchResults.map((result) => (
            <SearchResultCard
              key={result.id}
              result={result}
              onClick={() => {
                onClose();
              }}
            />
          ))}
        </div>
      </div>
    </motion.div>
  );
}

function SearchResultCard({
  result,
  onClick,
}: {
  result: SearchResult;
  onClick: () => void;
}) {
  return (
    <Link
      href={`/virksomhed/${result.id}`}
      onClick={() => {
        onClick();
      }}
    >
      <div className="flex  gap-2 items-center justify-between hover:bg-gray-100 p-4 group">
        <div className="flex gap-2 items-center">
          <Image
            src={"/farm-icon.png"}
            alt={result.name}
            width={25}
            height={25}
          />

          <div className="flex-col text-left">
            <div className="text-sm font-medium group-hover:underline">
              {result.name}
            </div>
            <div className="text-xs text-muted-foreground">
              {result.address}
            </div>
          </div>
        </div>
        <div className="flex-col">
          <div className="text-xs text-muted-foreground">CVR: {result.cvr}</div>
        </div>
      </div>
    </Link>
  );
}
