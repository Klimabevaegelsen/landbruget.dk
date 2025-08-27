import { useState, useCallback, useRef, useEffect } from 'react';
import { useToast } from '@/components/ui/toast';

/**
 * Custom hook for managing loading toasts with automatic cleanup
 * Ensures only one loading toast is active at a time and properly removes it when done
 */
export function useLoadingToast() {
  const { addToast, removeToast } = useToast();
  const [currentToastId, setCurrentToastId] = useState<string | null>(null);
  const toastIdRef = useRef<string | null>(null);

  // Sync ref with state to ensure cleanup works in async contexts
  useEffect(() => {
    toastIdRef.current = currentToastId;
  }, [currentToastId]);

  const showLoadingToast = useCallback(
    (title: string, description?: string) => {
      // Remove any existing loading toast first
      if (toastIdRef.current) {
        removeToast(toastIdRef.current);
      }

      // Show new loading toast
      const toastId = addToast({
        title,
        description,
        variant: 'loading',
      });

      setCurrentToastId(toastId);
      toastIdRef.current = toastId;

      return toastId;
    },
    [addToast, removeToast]
  );

  const hideLoadingToast = useCallback(() => {
    if (toastIdRef.current) {
      removeToast(toastIdRef.current);
      setCurrentToastId(null);
      toastIdRef.current = null;
    }
  }, [removeToast]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (toastIdRef.current) {
        removeToast(toastIdRef.current);
      }
    };
  }, [removeToast]);

  return {
    showLoadingToast,
    hideLoadingToast,
    currentToastId,
    isLoading: currentToastId !== null,
  };
}
