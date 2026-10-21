import {
  isSelectableAddress,
  resolveCoordinates,
  searchAddresses,
  type AdressevaelgerResult,
} from '@/components/pesticidkort/address-utils';
import type { AddressResult } from '@/components/pesticidkort/types';

export interface AddressSearchRequest {
  id: number;
  signal: AbortSignal;
}

interface AddressSearchActionsOptions {
  beginRequest: () => AddressSearchRequest;
  cancelRequest: () => void;
  currentRequestId: () => number;
  setQuery: (query: string) => void;
  setResults: (results: AdressevaelgerResult[]) => void;
  setIsLoading: (isLoading: boolean) => void;
  setIsOpen: (isOpen: boolean) => void;
  setHasError: (hasError: boolean) => void;
  setSelectedIndex: (index: number) => void;
  onSelect: (location: AddressResult) => void;
  onResolveStart?: (address: string) => void;
  onResolveEnd?: () => void;
  onResolveError?: () => void;
}

export function createAddressSearchActions({
  beginRequest,
  cancelRequest,
  currentRequestId,
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
}: AddressSearchActionsOptions) {
  const isCurrent = (request: AddressSearchRequest) =>
    request.id === currentRequestId();

  const runSearch = async (value: string, request: AddressSearchRequest) => {
    try {
      const addresses = await searchAddresses(value, request.signal);
      if (!isCurrent(request)) return;
      setResults(addresses);
      setHasError(false);
      setIsOpen(true);
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') return;
      if (!isCurrent(request)) return;
      setResults([]);
      setHasError(true);
      setIsOpen(true);
    } finally {
      if (isCurrent(request)) setIsLoading(false);
    }
  };

  const selectResult = async (result: AdressevaelgerResult) => {
    setQuery(result.titel);
    setResults([]);
    setHasError(false);
    setSelectedIndex(-1);
    cancelRequest();

    const request = beginRequest();
    setIsLoading(true);
    setIsOpen(true);
    if (!isSelectableAddress(result)) {
      await runSearch(result.titel, request);
      return;
    }

    onResolveStart?.(result.titel);
    try {
      const location = await resolveCoordinates(result, request.signal);
      if (!isCurrent(request)) return;
      if (!location) {
        setHasError(true);
        onResolveError?.();
        return;
      }
      setQuery(location.address);
      setIsOpen(false);
      onSelect(location);
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') return;
      if (isCurrent(request)) {
        setHasError(true);
        onResolveError?.();
      }
    } finally {
      if (isCurrent(request)) {
        setIsLoading(false);
        onResolveEnd?.();
      }
    }
  };

  return { runSearch, selectResult };
}
