'use client';

import * as React from 'react';
import { Menu } from 'lucide-react';
import { cn } from '@/lib/utils';
import { Sheet, SheetContent, SheetTrigger } from './sheet';

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
              'mobile-header h-12 w-12 rounded-full shadow-lg',
              'bg-background/95 border-border border backdrop-blur-sm',
              'hover:bg-accent hover:text-accent-foreground',
              'focus:ring-ring focus:ring-2 focus:ring-offset-2 focus:outline-none',
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
            className="w-[280px] border-none p-0"
          >
            <div className="flex h-full flex-col">
              {/* Content */}
              <div className="flex-1 overflow-y-auto p-6">{children}</div>
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
}

const MobileMenuSection = React.forwardRef<
  HTMLDivElement,
  MobileMenuSectionProps
>(({ className, title, children, ...props }, ref) => {
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
});
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
            'hover:bg-accent hover:text-accent-foreground',
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
