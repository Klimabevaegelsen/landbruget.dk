import type { ReactNode } from 'react';

interface DesktopSidebarProps {
  children: ReactNode;
  controls?: ReactNode;
}

export function DesktopSidebar({ children, controls }: DesktopSidebarProps) {
  return (
    <div className="pointer-events-none absolute top-0 right-0 bottom-[var(--pesticidkort-footer-height)] hidden w-[420px] md:block">
      <div className="bg-background/95 border-border/70 pointer-events-auto flex h-full flex-col border-l pt-14 backdrop-blur-sm">
        <div
          className="flex-1 overflow-y-auto"
          role="region"
          aria-label="Pesticidrapport"
        >
          {children}
        </div>
        {controls}
      </div>
    </div>
  );
}
