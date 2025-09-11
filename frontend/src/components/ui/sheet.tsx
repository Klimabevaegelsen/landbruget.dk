'use client';

import * as React from 'react';
import { X } from 'lucide-react';
import { cn } from '@/lib/utils';
import { useTouchGestures } from '@/hooks/use-touch-gestures';

interface SheetContextValue {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const SheetContext = React.createContext<SheetContextValue | null>(null);

function useSheet() {
  const context = React.useContext(SheetContext);
  if (!context) {
    throw new Error('useSheet must be used within a Sheet');
  }
  return context;
}

interface SheetProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  children: React.ReactNode;
}

const Sheet = React.forwardRef<HTMLDivElement, SheetProps>(
  ({ open, onOpenChange, children }, ref) => {
    return (
      <SheetContext.Provider value={{ open, onOpenChange }}>
        <div ref={ref}>{children}</div>
      </SheetContext.Provider>
    );
  }
);
Sheet.displayName = 'Sheet';

type SheetTriggerProps = React.ButtonHTMLAttributes<HTMLButtonElement>;

const SheetTrigger = React.forwardRef<HTMLButtonElement, SheetTriggerProps>(
  ({ className, onClick, ...props }, ref) => {
    const { onOpenChange } = useSheet();

    return (
      <button
        ref={ref}
        className={cn(
          'touch-target inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors',
          'focus-visible:ring-ring focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:outline-none',
          'disabled:pointer-events-none disabled:opacity-50',
          className
        )}
        onClick={(e) => {
          onOpenChange(true);
          onClick?.(e);
        }}
        {...props}
      />
    );
  }
);
SheetTrigger.displayName = 'SheetTrigger';

type SheetOverlayProps = React.HTMLAttributes<HTMLDivElement>;

const SheetOverlay = React.forwardRef<HTMLDivElement, SheetOverlayProps>(
  ({ className, ...props }, ref) => {
    const { open, onOpenChange } = useSheet();

    if (!open) return null;

    return (
      <div
        ref={ref}
        className={cn(
          'fixed inset-0 z-[65] bg-black/50 backdrop-blur-sm',
          'data-[state=open]:animate-in data-[state=closed]:animate-out',
          'data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0',
          className
        )}
        data-state={open ? 'open' : 'closed'}
        onClick={() => onOpenChange(false)}
        {...props}
      />
    );
  }
);
SheetOverlay.displayName = 'SheetOverlay';

interface SheetContentProps extends React.HTMLAttributes<HTMLDivElement> {
  side?: 'top' | 'right' | 'bottom' | 'left';
  title: string;
  description?: string;
}

const SheetContent = React.forwardRef<HTMLDivElement, SheetContentProps>(
  (
    { side = 'bottom', className, title, description, children, ...props },
    ref
  ) => {
    const { open, onOpenChange } = useSheet();
    const contentRef = React.useRef<HTMLDivElement>(null);

    // Add swipe gestures for mobile dismissal
    const gestureRef = useTouchGestures<HTMLDivElement>({
      onSwipeDown: side === 'bottom' ? () => onOpenChange(false) : undefined,
      onSwipeUp: side === 'top' ? () => onOpenChange(false) : undefined,
      onSwipeLeft: side === 'left' ? () => onOpenChange(false) : undefined,
      onSwipeRight: side === 'right' ? () => onOpenChange(false) : undefined,
      threshold: 100,
    });

    React.useEffect(() => {
      if (contentRef.current && gestureRef.current) {
        gestureRef.current = contentRef.current;
      }
    }, [gestureRef]);

    // Prevent body scroll when sheet is open
    React.useEffect(() => {
      if (open) {
        document.body.style.overflow = 'hidden';
        return () => {
          document.body.style.overflow = 'unset';
        };
      }
    }, [open]);

    if (!open) return null;

    const sideVariants = {
      top: 'inset-x-0 top-0 border-b rounded-b-lg',
      right: 'inset-y-0 right-0 h-full w-3/4 border-l sm:max-w-sm rounded-l-lg',
      bottom: 'inset-x-0 bottom-0 border-t rounded-t-xl',
      left: 'inset-y-0 left-0 h-full w-3/4 border-r sm:max-w-sm rounded-r-lg',
    };

    const animationVariants = {
      top: 'data-[state=closed]:slide-out-to-top data-[state=open]:slide-in-from-top',
      right:
        'data-[state=closed]:slide-out-to-right data-[state=open]:slide-in-from-right',
      bottom:
        'data-[state=closed]:slide-out-to-bottom data-[state=open]:slide-in-from-bottom',
      left: 'data-[state=closed]:slide-out-to-left data-[state=open]:slide-in-from-left',
    };

    return (
      <>
        <SheetOverlay />
        <div
          ref={(node) => {
            if (typeof ref === 'function') ref(node);
            else if (ref) ref.current = node;
            contentRef.current = node;
          }}
          className={cn(
            'bg-background fixed z-[70] gap-4 p-6 shadow-lg transition ease-in-out',
            'data-[state=open]:animate-in data-[state=closed]:animate-out',
            'data-[state=closed]:duration-300 data-[state=open]:duration-500',
            'sheet-transition',
            sideVariants[side],
            animationVariants[side],
            // Mobile-specific styles
            side === 'bottom' && 'mobile-content max-h-[85vh] overflow-y-auto',
            className
          )}
          data-state={open ? 'open' : 'closed'}
          {...props}
        >
          {/* Drag indicator for bottom sheets */}
          {side === 'bottom' && (
            <div className="bg-muted mx-auto mb-4 h-1.5 w-12 rounded-full" />
          )}

          {/* Header */}
          <div className="flex flex-col space-y-2 text-center sm:text-left">
            <div className="flex items-center justify-between">
              <div className="flex-1">
                <h2 className="text-foreground text-lg font-semibold">
                  {title}
                </h2>
                {description && (
                  <p className="text-muted-foreground text-sm">{description}</p>
                )}
              </div>
              <button
                className={cn(
                  'touch-target ring-offset-background rounded-sm opacity-70 transition-opacity',
                  'focus:ring-ring hover:opacity-100 focus:ring-2 focus:ring-offset-2 focus:outline-none',
                  'disabled:pointer-events-none'
                )}
                onClick={() => onOpenChange(false)}
                aria-label="Luk"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          </div>

          {/* Content */}
          <div className="flex-1">{children}</div>
        </div>
      </>
    );
  }
);
SheetContent.displayName = 'SheetContent';

export { Sheet, SheetTrigger, SheetContent, SheetOverlay };
