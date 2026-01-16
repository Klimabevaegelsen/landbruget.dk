'use client';

import * as React from 'react';
import { Menu, ChevronDown } from 'lucide-react';
import { cn } from '@/lib/utils';
import { Sheet, SheetContent, SheetTrigger } from './sheet';
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from './collapsible';

interface MobileMenuProps {
  children: React.ReactNode;
  title?: string;
  description?: string;
  triggerClassName?: string;
}

const MobileMenu = React.forwardRef<HTMLDivElement, MobileMenuProps>(
  ({ children, title = 'Menu', description, triggerClassName }, ref) => {
    const [open, setOpen] = React.useState(false);

    return (
      <div ref={ref} className="md:hidden">
        <Sheet open={open} onOpenChange={setOpen}>
          <SheetTrigger
            className={cn(
              'touch-target fixed z-50',
              'inline-flex items-center justify-center',
              'border-border h-10 w-10 rounded-full border shadow-lg',
              'bg-background/95 backdrop-blur-xl',
              'hover:bg-accent hover:text-accent-foreground',
              'focus:ring-ring focus:ring-2 focus:ring-offset-2 focus:outline-none',
              'transition-colors duration-200',
              triggerClassName
            )}
            style={{
              top: 'max(1rem, env(safe-area-inset-top))',
              left: 'max(1rem, env(safe-area-inset-left))',
            }}
            aria-label="Åbn menu"
          >
            <Menu size={20} />
          </SheetTrigger>

          <SheetContent
            side="left"
            title={title}
            description={description}
            className="w-[280px] rounded-none border-none p-0"
          >
            <div className="flex h-full flex-col">
              {/* Header */}
              <div className="flex items-center justify-between p-6 pb-4">
                <div className="flex flex-col">
                  <h2 className="text-lg font-semibold">{title}</h2>
                  {description && (
                    <p className="text-muted-foreground text-sm">
                      {description}
                    </p>
                  )}
                </div>
              </div>

              {/* Content */}
              <div className="flex-1 overflow-y-auto px-6 pb-6">{children}</div>
            </div>
          </SheetContent>
        </Sheet>
      </div>
    );
  }
);
MobileMenu.displayName = 'MobileMenu';

interface MobileMenuSectionProps extends React.HTMLAttributes<HTMLDivElement> {
  title?: string;
  defaultOpen?: boolean;
  collapsible?: boolean;
}

const MobileMenuSection = React.forwardRef<
  HTMLDivElement,
  MobileMenuSectionProps
>(
  (
    {
      className,
      title,
      children,
      defaultOpen = true,
      collapsible = true,
      ...props
    },
    ref
  ) => {
    if (!collapsible) {
      // Non-collapsible section (backwards compatible)
      return (
        <div ref={ref} className={cn('space-y-4', className)} {...props}>
          {title && (
            <h3 className="text-muted-foreground text-sm font-medium tracking-wider uppercase">
              {title}
            </h3>
          )}
          <div className="space-y-2">{children}</div>
        </div>
      );
    }

    return (
      <Collapsible
        defaultOpen={defaultOpen}
        className={cn('space-y-2', className)}
      >
        <div ref={ref} {...props}>
          {title && (
            <CollapsibleTrigger className="flex w-full items-center justify-between py-2 text-left">
              <h3 className="text-muted-foreground text-sm font-medium tracking-wider uppercase">
                {title}
              </h3>
              <ChevronDown className="text-muted-foreground h-4 w-4 transition-transform duration-200 [[data-state=open]_&]:rotate-180" />
            </CollapsibleTrigger>
          )}
          <CollapsibleContent className="space-y-2">
            {children}
          </CollapsibleContent>
        </div>
      </Collapsible>
    );
  }
);
MobileMenuSection.displayName = 'MobileMenuSection';

interface MobileMenuItemProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  icon?: React.ReactNode;
  label: string;
  active?: boolean;
  variant?: 'default' | 'destructive';
}

const MobileMenuItem = React.forwardRef<HTMLButtonElement, MobileMenuItemProps>(
  (
    { className, icon, label, active = false, variant = 'default', ...props },
    ref
  ) => {
    return (
      <button
        ref={ref}
        className={cn(
          'touch-target flex w-full items-center gap-3 rounded-lg px-4 py-3 text-left',
          'transition-colors duration-200',
          'focus:ring-ring focus:ring-2 focus:ring-offset-2 focus:outline-none',
          variant === 'default' && [
            'hover:bg-accent/50 hover:text-accent-foreground',
            active && 'bg-primary text-primary-foreground hover:bg-primary/90',
          ],
          variant === 'destructive' &&
            'text-destructive hover:bg-destructive/10',
          className
        )}
        {...props}
      >
        {icon && (
          <div className="flex h-5 w-5 flex-shrink-0 items-center justify-center">
            {icon}
          </div>
        )}
        <span className="text-sm font-medium">{label}</span>
      </button>
    );
  }
);
MobileMenuItem.displayName = 'MobileMenuItem';

export { MobileMenu, MobileMenuSection, MobileMenuItem };
