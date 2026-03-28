'use client';

import React from 'react';

interface ResponsiveDetailPanelProps {
  onClose: () => void;
  children: React.ReactNode;
}

export function ResponsiveDetailPanel({
  onClose,
  children,
}: ResponsiveDetailPanelProps) {
  return (
    <>
      {/* Desktop: Fixed sidebar */}
      <div className="bg-background relative z-10 hidden h-full w-80 shadow-lg lg:block">
        {children}
      </div>

      {/* Mobile: Bottom sheet */}
      <div className="lg:hidden">
        <div
          className="bg-foreground/50 fixed inset-0 z-40"
          onClick={onClose}
        />
        <div className="bg-background fixed inset-x-0 bottom-0 z-[70] max-h-[85vh] overflow-y-auto rounded-t-xl pb-[max(1rem,env(safe-area-inset-bottom))] shadow-2xl">
          {children}
        </div>
      </div>
    </>
  );
}
