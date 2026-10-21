'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import type { AddressResult } from '@/components/pesticidkort/types';
import type { AdressevaelgerResult } from '@/components/pesticidkort/address-utils';
import { createAddressSearchActions } from '@/components/pesticidkort/address-search-actions';

interface UseAdressevaelgerSearchOptions {
  onSelect: (location: AddressResult) => void;
  onResolveStart?: (address: string) => void;
  onResolveEnd?: () => void;
  onResolveError?: () => void;
}

export function useAdressevaelgerSearch({
  onSelect,
  onResolveStart,
  onResolveEnd,
  onResolveError,
}: UseAdressevaelgerSearchOptions) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<AdressevaelgerResult[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isOpen, setIsOpen] = useState(false);
  const [hasError, setHasError] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const containerRef = useRef<HTMLDivElement>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const requestIdRef = useRef(0);
  const controllerRef = useRef<AbortController | null>(null);

  const beginRequest = useCallback(() => {
    controllerRef.current?.abort();
    controllerRef.current = new AbortController();
    requestIdRef.current += 1;
    return { id: requestIdRef.current, signal: controllerRef.current.signal };
  }, []);

  const cancelRequest = useCallback(() => {
    controllerRef.current?.abort();
    requestIdRef.current += 1;
    setIsLoading(false);
    onResolveEnd?.();
  }, [onResolveEnd]);

  const { runSearch, selectResult } = createAddressSearchActions({
    beginRequest,
    cancelRequest,
    currentRequestId: () => requestIdRef.current,
    setQuery,
    setResults,
    setIsLoading,
    setIsOpen,
    setHasError,
    setSelectedIndex,
    onSelect,
    onResolveStart,
    onResolveEnd,
    onResolveError,
  });

  const changeQuery = useCallback(
    (value: string) => {
      setQuery(value);
      setResults([]);
      setHasError(false);
      setSelectedIndex(-1);
      if (timerRef.current) clearTimeout(timerRef.current);
      cancelRequest();

      if (value.trim().length < 2) {
        setIsOpen(false);
        return;
      }

      const request = beginRequest();
      setIsOpen(true);
      setIsLoading(true);
      timerRef.current = setTimeout(() => {
        void runSearch(value, request);
      }, 300);
    },
    [beginRequest, cancelRequest, runSearch]
  );

  const close = useCallback(() => {
    setIsOpen(false);
    setSelectedIndex(-1);
    if (timerRef.current) clearTimeout(timerRef.current);
    cancelRequest();
  }, [cancelRequest]);

  const handleKeyDown = useCallback(
    (event: React.KeyboardEvent) => {
      if (!isOpen) return;
      if (event.key === 'ArrowDown') {
        event.preventDefault();
        setSelectedIndex((index) => Math.min(index + 1, results.length - 1));
      } else if (event.key === 'ArrowUp') {
        event.preventDefault();
        setSelectedIndex((index) => Math.max(index - 1, -1));
      } else if (event.key === 'Enter' && selectedIndex >= 0) {
        event.preventDefault();
        const result = results[selectedIndex];
        if (result) void selectResult(result);
      } else if (event.key === 'Escape') {
        close();
      }
    },
    [close, isOpen, results, selectedIndex, selectResult]
  );

  useEffect(() => {
    const handleOutsideClick = (event: MouseEvent) => {
      if (
        containerRef.current &&
        !containerRef.current.contains(event.target as Node)
      ) {
        close();
      }
    };
    document.addEventListener('mousedown', handleOutsideClick);
    return () => document.removeEventListener('mousedown', handleOutsideClick);
  }, [close]);

  useEffect(
    () => () => {
      if (timerRef.current) clearTimeout(timerRef.current);
      controllerRef.current?.abort();
      onResolveEnd?.();
    },
    [onResolveEnd]
  );

  return {
    containerRef,
    query,
    results,
    isLoading,
    isOpen,
    hasError,
    selectedIndex,
    setSelectedIndex,
    changeQuery,
    selectResult,
    handleKeyDown,
    close,
  };
}
