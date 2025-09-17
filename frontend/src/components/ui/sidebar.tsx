'use client';

import * as React from 'react';
import { cn } from '@/lib/utils';
import { useMobileDetection } from '@/hooks/use-mobile-detection';

interface SidebarContextValue {
  isExpanded: boolean;
  setIsExpanded: (expanded: boolean) => void;
  isMobile: boolean;
}

const SidebarContext = React.createContext<SidebarContextValue | null>(null);

function useSidebar() {
  const context = React.useContext(SidebarContext);
  if (!context) {
    throw new Error('useSidebar must be used within a Sidebar');
  }
  return context;
}

interface SidebarProps extends React.HTMLAttributes<HTMLDivElement> {
  defaultExpanded?: boolean;
  collapsible?: boolean;
  onExpandedChange?: (expanded: boolean) => void;
}

const Sidebar = React.forwardRef<HTMLDivElement, SidebarProps>(
  (
    {
      className,
      defaultExpanded = false,
      collapsible = true,
      onExpandedChange,
      children,
      ...props
    },
    ref
  ) => {
    const [isExpanded, setIsExpanded] = React.useState(defaultExpanded);
    const { isMobile } = useMobileDetection();

    // Auto-collapse on mobile
    React.useEffect(() => {
      if (isMobile) {
        setIsExpanded(false);
      }
    }, [isMobile]);

    // Notify parent of expanded state changes
    React.useEffect(() => {
      onExpandedChange?.(isExpanded);
    }, [isExpanded, onExpandedChange]);

    return (
      <SidebarContext.Provider value={{ isExpanded, setIsExpanded, isMobile }}>
        <aside
          ref={ref}
          className={cn(
            'fixed top-[120px] z-50 h-[calc(100vh-120px)] flex-shrink-0 flex-col items-center justify-between pb-4',
            'bg-card border-border sidebar-transition border-r',
            'hidden md:flex', // Hide on mobile by default
            collapsible && [
              isExpanded ? 'w-[280px]' : 'w-[70px]',
              'transition-all duration-200 ease-[cubic-bezier(0.4,0,0.2,1)]',
            ],
            !collapsible && 'w-[280px]',
            className
          )}
          onMouseEnter={collapsible ? () => setIsExpanded(true) : undefined}
          onMouseLeave={collapsible ? () => setIsExpanded(false) : undefined}
          {...props}
        >
          {children}
        </aside>
      </SidebarContext.Provider>
    );
  }
);
Sidebar.displayName = 'Sidebar';

type SidebarHeaderProps = React.HTMLAttributes<HTMLDivElement>;

const SidebarHeader = React.forwardRef<HTMLDivElement, SidebarHeaderProps>(
  ({ className, children, ...props }, ref) => {
    return (
      <div
        ref={ref}
        className={cn(
          'border-border flex items-center gap-3 border-b p-4',
          'transition-all duration-200',
          className
        )}
        {...props}
      >
        {children}
      </div>
    );
  }
);
SidebarHeader.displayName = 'SidebarHeader';

type SidebarContentProps = React.HTMLAttributes<HTMLDivElement>;

const SidebarContent = React.forwardRef<HTMLDivElement, SidebarContentProps>(
  ({ className, children, ...props }, ref) => {
    return (
      <div
        ref={ref}
        className={cn('flex-1 overflow-y-auto p-2', className)}
        {...props}
      >
        {children}
      </div>
    );
  }
);
SidebarContent.displayName = 'SidebarContent';

type SidebarFooterProps = React.HTMLAttributes<HTMLDivElement>;

const SidebarFooter = React.forwardRef<HTMLDivElement, SidebarFooterProps>(
  ({ className, children, ...props }, ref) => {
    return (
      <div
        ref={ref}
        className={cn('border-border mt-auto border-t p-4', className)}
        {...props}
      >
        {children}
      </div>
    );
  }
);
SidebarFooter.displayName = 'SidebarFooter';

interface SidebarItemProps extends React.HTMLAttributes<HTMLDivElement> {
  icon?: React.ReactNode;
  label: string;
  active?: boolean;
  disabled?: boolean;
}

const SidebarItem = React.forwardRef<HTMLDivElement, SidebarItemProps>(
  (
    {
      className,
      icon,
      label,
      active = false,
      disabled = false,
      onClick,
      ...props
    },
    ref
  ) => {
    const { isExpanded } = useSidebar();

    return (
      <div
        ref={ref}
        className={cn(
          'touch-target flex cursor-pointer items-center gap-3 rounded-lg px-3 py-2',
          'transition-colors duration-200',
          'hover:bg-accent hover:text-accent-foreground',
          active && 'bg-primary text-primary-foreground hover:bg-primary/90',
          disabled && 'cursor-not-allowed opacity-50',
          !isExpanded && 'justify-center',
          className
        )}
        onClick={disabled ? undefined : onClick}
        {...props}
      >
        {icon && (
          <div
            className={cn(
              'flex h-5 w-5 flex-shrink-0 items-center justify-center',
              !isExpanded && 'h-6 w-6'
            )}
          >
            {icon}
          </div>
        )}
        {isExpanded && (
          <span className="truncate text-sm font-medium">{label}</span>
        )}
      </div>
    );
  }
);
SidebarItem.displayName = 'SidebarItem';

interface SidebarGroupProps extends React.HTMLAttributes<HTMLDivElement> {
  label?: string;
}

const SidebarGroup = React.forwardRef<HTMLDivElement, SidebarGroupProps>(
  ({ className, label, children, ...props }, ref) => {
    const { isExpanded } = useSidebar();

    return (
      <div ref={ref} className={cn('space-y-1', className)} {...props}>
        {label && isExpanded && (
          <div className="px-3 py-2">
            <p className="text-muted-foreground text-xs font-medium tracking-wider uppercase">
              {label}
            </p>
          </div>
        )}
        {children}
      </div>
    );
  }
);
SidebarGroup.displayName = 'SidebarGroup';

export {
  Sidebar,
  SidebarHeader,
  SidebarContent,
  SidebarFooter,
  SidebarItem,
  SidebarGroup,
  useSidebar,
};
