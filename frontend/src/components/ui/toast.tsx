'use client';

import * as React from 'react';
import { cn } from '@/lib/utils';
import { X } from 'lucide-react';

const ToastProvider = React.createContext<{
  toasts: Toast[];
  addToast: (toast: Omit<Toast, 'id'>) => string;
  removeToast: (id: string) => void;
  clearLoadingToasts: () => void;
}>({
  toasts: [],
  addToast: () => '',
  removeToast: () => {},
  clearLoadingToasts: () => {},
});

export interface Toast {
  id: string;
  title?: string;
  description?: string;
  variant?: 'default' | 'success' | 'error' | 'loading';
  duration?: number;
}

export function ToastProvider_({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = React.useState<Toast[]>([]);

  const addToast = React.useCallback((toast: Omit<Toast, 'id'>) => {
    const id = Math.random().toString(36).substr(2, 9);
    const newToast = { ...toast, id };

    setToasts((prev) => [...prev, newToast]);

    // Auto-remove toast after duration (default 5s, loading toasts don't auto-remove)
    if (toast.variant !== 'loading') {
      setTimeout(() => {
        setToasts((prev) => prev.filter((t) => t.id !== id));
      }, toast.duration || 5000);
    }

    return id;
  }, []);

  const removeToast = React.useCallback((id: string) => {
    setToasts((prev) => prev.filter((toast) => toast.id !== id));
  }, []);

  const clearLoadingToasts = React.useCallback(() => {
    setToasts((prev) => prev.filter((toast) => toast.variant !== 'loading'));
  }, []);

  return (
    <ToastProvider.Provider
      value={{ toasts, addToast, removeToast, clearLoadingToasts }}
    >
      {children}
      <ToastViewport />
    </ToastProvider.Provider>
  );
}

export function useToast() {
  const context = React.useContext(ToastProvider);
  if (!context) {
    throw new Error('useToast must be used within a ToastProvider');
  }
  return context;
}

function ToastViewport() {
  const { toasts } = useToast();

  return (
    <div className="fixed top-0 z-[100] flex max-h-screen w-full flex-col-reverse p-4 sm:top-auto sm:right-0 sm:bottom-0 sm:flex-col md:max-w-[420px]">
      {toasts.map((toast) => (
        <ToastComponent key={toast.id} {...toast} />
      ))}
    </div>
  );
}

function ToastComponent({
  id,
  title,
  description,
  variant = 'default',
}: Toast) {
  const { removeToast } = useToast();

  const variantStyles = {
    default: 'border bg-background text-foreground',
    success: 'border-organic/20 bg-organic/10 text-organic',
    error: 'border-destructive/20 bg-destructive/10 text-destructive',
    loading: 'border-primary/20 bg-primary/10 text-primary',
  };

  return (
    <div
      className={cn(
        'group data-[state=open]:animate-in data-[state=closed]:animate-out data-[swipe=end]:animate-out data-[state=closed]:fade-out-80 data-[state=closed]:slide-out-to-right-full data-[state=open]:slide-in-from-top-full data-[state=open]:sm:slide-in-from-bottom-full pointer-events-auto relative flex w-full items-center justify-between space-x-4 overflow-hidden rounded-md border p-6 pr-8 shadow-lg transition-all data-[swipe=cancel]:translate-x-0 data-[swipe=end]:translate-x-[var(--radix-toast-swipe-end-x)] data-[swipe=move]:translate-x-[var(--radix-toast-swipe-move-x)] data-[swipe=move]:transition-none',
        variantStyles[variant]
      )}
    >
      <div className="flex items-center space-x-3">
        {variant === 'loading' && (
          <div className="border-primary/30 border-t-primary h-4 w-4 animate-spin rounded-full border-2" />
        )}
        <div className="grid gap-1">
          {title && <div className="text-sm font-semibold">{title}</div>}
          {description && (
            <div className="text-sm opacity-90">{description}</div>
          )}
        </div>
      </div>
      <button
        onClick={() => removeToast(id)}
        data-testid="dismiss-toast-button"
        className="text-foreground/50 hover:text-foreground absolute top-2 right-2 rounded-md p-1 opacity-0 transition-opacity group-hover:opacity-100 focus:opacity-100 focus:ring-2 focus:outline-none"
      >
        <X className="h-4 w-4" />
      </button>
    </div>
  );
}
