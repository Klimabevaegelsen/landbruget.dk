'use client';
import { useEffect, useState } from 'react';
import { Input } from './ui/input';
import { cn } from '@/lib/utils';
import { MagnifyingGlassIcon } from '@heroicons/react/24/solid';
import { motion, AnimatePresence } from 'motion/react';
import React from 'react';
import Image from 'next/image';
import Link from 'next/link';
import { Button } from './ui/button';
import { useSearch } from '@/hooks/useSearch';
import type { SearchResult } from '@/hooks/useSearch';
import { useLoadingToast } from '@/hooks/useLoadingToast';

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
  const [search, setSearch] = useState('');
  const [open, setOpen] = useState(defaultOpen);

  useEffect(() => {
    if (parentOpen) setOpen(parentOpen);
  }, [parentOpen]);

  return (
    <div
      className={cn(
        'relative flex w-full flex-col items-center gap-y-4 overflow-hidden',
        className,
        parentOpen === false && 'hidden'
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
        data-testid="global-search-input"
      />
      {searchSuggestions && (
        <div className="hidden max-w-full flex-wrap items-center justify-center gap-2 sm:flex">
          {searchSuggestions.map((suggestion) => (
            <Button
              key={suggestion}
              variant="secondary"
              className="bg-background/75 hover:bg-background/90 max-w-full text-xs sm:text-sm"
              onClick={() => {
                setSearch(suggestion);
                setOpen(true);
              }}
              data-testid={`search-suggestion-${suggestion}`}
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
  const tabs = ['Alle', 'CVR', 'Firmanavn'];
  const [activeTab, setActiveTab] = useState(0);

  const { searchResults, isLoading, error } = useSearch(search, activeTab);

  React.useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === 'Escape') onClose();
    }
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [onClose]);

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
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [onClose]);

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.1 }}
      className="absolute top-0 right-0 bottom-0 left-0 z-50 flex flex-col items-center justify-start"
      ref={overlayRef}
    >
      <div
        className={cn(
          'h-auto w-full rounded-lg shadow-lg',
          !borderless && 'border-border border'
        )}
      >
        <Input
          autoFocus
          type="text"
          placeholder="Søg efter CVR eller firmanavn"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="h-12 rounded-t-lg rounded-b-none border-none focus:ring-0 focus-visible:ring-0"
          endIcon={<MagnifyingGlassIcon className="size-6" />}
          data-testid="search-overlay-input"
        />
        <div
          className="bg-primary-foreground flex items-stretch gap-2 overflow-x-auto"
          data-testid="search-tabs"
        >
          {tabs.map((tab, i) => (
            <div
              key={tab}
              role="tab"
              aria-selected={activeTab === i}
              onClick={() => setActiveTab(i)}
              className={cn(
                'flex-1 cursor-pointer px-4 py-4 text-center text-xs hover:font-semibold',
                activeTab === i &&
                  'border-b-primary border-b-2 font-bold hover:font-bold'
              )}
              data-testid={`search-tab-${tab.toLowerCase()}`}
            >
              {tab}
            </div>
          ))}
        </div>
        <div
          className="search-results bg-background max-h-[400px] min-h-[200px] overflow-auto rounded-b-lg"
          data-testid="search-results"
          role="listbox"
        >
          {isLoading && (
            <div
              className="flex items-center justify-center p-8"
              data-testid="search-loading"
            >
              <div className="border-muted border-t-primary h-8 w-8 animate-spin rounded-full border-2"></div>
              <span className="text-muted-foreground ml-3 text-sm">
                Søger...
              </span>
            </div>
          )}

          {error && (
            <div
              className="flex items-center justify-center p-8"
              data-testid="search-error"
            >
              <div className="text-destructive text-sm">{error}</div>
            </div>
          )}

          {!isLoading &&
            !error &&
            searchResults.length === 0 &&
            search.trim().length >= 2 && (
              <div
                className="flex items-center justify-center p-8"
                data-testid="search-no-results"
              >
                <div className="text-muted-foreground text-sm">
                  Ingen resultater fundet for &ldquo;{search}&rdquo;
                </div>
              </div>
            )}

          {!isLoading && !error && search.trim().length < 2 && (
            <div
              className="flex items-center justify-center p-8"
              data-testid="search-min-chars"
            >
              <div className="text-muted-foreground text-sm">
                Indtast mindst 2 tegn for at søge
              </div>
            </div>
          )}

          {!isLoading &&
            !error &&
            searchResults.map((result) => (
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
  const { showLoadingToast } = useLoadingToast();

  const handleClick = () => {
    showLoadingToast(
      'Indlæser virksomhed',
      `Henter data for ${result.name}...`
    );

    onClick();
  };

  return (
    <Link
      href={`/virksomhed/${result.id}`}
      onClick={handleClick}
      data-testid="search-result"
      role="option"
      aria-selected={false}
    >
      <div className="group hover:bg-muted/50 flex items-center justify-between gap-2 p-4">
        <div className="flex items-center gap-2">
          <Image
            src={'/farm-icon.png'}
            alt={result.name}
            width={25}
            height={25}
            data-testid="search-result-icon"
          />

          <div className="flex-col text-left">
            <div
              className="text-sm font-medium group-hover:underline"
              data-testid="search-result-name"
            >
              {result.name}
            </div>
            <div
              className="text-muted-foreground text-xs"
              data-testid="search-result-address"
            >
              {result.address}
            </div>
          </div>
        </div>
        <div className="flex-col">
          <div
            className="text-muted-foreground text-xs"
            data-testid="search-result-cvr"
          >
            CVR: {result.cvr}
          </div>
        </div>
      </div>
    </Link>
  );
}
