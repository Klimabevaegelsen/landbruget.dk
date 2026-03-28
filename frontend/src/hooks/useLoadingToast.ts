import { useState, useCallback, useRef, useEffect } from 'react';
import { toast } from 'sonner';

/**
 * Custom hook for managing loading toasts with automatic cleanup
 * Ensures only one loading toast is active at a time and properly removes it when done
 */
export function useLoadingToast() {
  const [currentToastId, setCurrentToastId] = useState<string | number | null>(
    null
  );
  const toastIdRef = useRef<string | number | null>(null);

  // Sync ref with state to ensure cleanup works in async contexts
  useEffect(() => {
    toastIdRef.current = currentToastId;
  }, [currentToastId]);

  const showLoadingToast = useCallback(
    (title: string, description?: string) => {
      // Remove any existing loading toast first
      if (toastIdRef.current) {
        toast.dismiss(toastIdRef.current);
      }

      // Show new loading toast
      const toastId = toast.loading(title, { description });

      setCurrentToastId(toastId);
      toastIdRef.current = toastId;

      return toastId;
    },
    []
  );

  const hideLoadingToast = useCallback(() => {
    if (toastIdRef.current) {
      toast.dismiss(toastIdRef.current);
      setCurrentToastId(null);
      toastIdRef.current = null;
    }
  }, []);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (toastIdRef.current) {
        toast.dismiss(toastIdRef.current);
      }
    };
  }, []);

  return {
    showLoadingToast,
    hideLoadingToast,
    currentToastId,
    isLoading: currentToastId !== null,
  };
}
