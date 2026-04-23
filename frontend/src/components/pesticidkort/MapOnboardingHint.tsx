'use client';

import { useState, useEffect } from 'react';
import { X } from 'lucide-react';

const STORAGE_KEY = 'pesticidkort-onboarding-seen';

export function MapOnboardingHint() {
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    if (!localStorage.getItem(STORAGE_KEY)) {
      setVisible(true);
    }
  }, []);

  if (!visible) return null;

  const dismiss = () => {
    localStorage.setItem(STORAGE_KEY, '1');
    setVisible(false);
  };

  return (
    <div
      data-testid="map-onboarding-hint"
      className="bg-background/95 border-border/70 absolute top-1/2 left-1/2 z-30 w-72 -translate-x-1/2 -translate-y-1/2 rounded-xl border p-5 text-center shadow-xl backdrop-blur-sm"
    >
      <button
        onClick={dismiss}
        data-testid="map-onboarding-dismiss-button"
        className="text-muted-foreground absolute top-2 right-2 p-1"
      >
        <X className="h-4 w-4" />
      </button>
      <p className="text-foreground text-sm font-semibold">
        Farverne viser relativ pesticidbelastning
      </p>
      <div className="mt-3 flex items-center justify-center gap-3">
        <div className="flex items-center gap-1.5">
          <div className="h-3 w-3 rounded-sm bg-[var(--pesticidkort-color-burden-low)]" />
          <span className="text-muted-foreground text-[11px]">Lavest</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="h-3 w-3 rounded-sm bg-[var(--pesticidkort-color-burden-mid)]" />
          <span className="text-muted-foreground text-[11px]">Middel</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="h-3 w-3 rounded-sm bg-[var(--pesticidkort-color-burden-high)]" />
          <span className="text-muted-foreground text-[11px]">Højest</span>
        </div>
      </div>
      <p className="text-muted-foreground mt-3 text-xs">
        Tryk på en mark for at se detaljer
      </p>
      <button
        onClick={dismiss}
        data-testid="map-onboarding-ok-button"
        className="bg-primary text-primary-foreground mt-4 rounded-full px-5 py-1.5 text-sm font-medium"
      >
        Forstået
      </button>
    </div>
  );
}
