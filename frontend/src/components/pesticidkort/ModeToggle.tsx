'use client';

import { cn } from '@/lib/utils';

interface ModeToggleProps {
  mode: 'citizen' | 'expert';
  onChange: (mode: 'citizen' | 'expert') => void;
}

export function ModeToggle({ mode, onChange }: ModeToggleProps) {
  return (
    <div
      className="bg-muted inline-flex rounded-full p-1"
      data-testid="mode-toggle"
      role="radiogroup"
      aria-label="Visningstilstand"
    >
      <button
        role="radio"
        aria-checked={mode === 'citizen'}
        onClick={() => onChange('citizen')}
        data-testid="mode-citizen-button"
        className={cn(
          'rounded-full px-4 py-1.5 text-sm font-medium transition-colors',
          mode === 'citizen'
            ? 'bg-background text-foreground shadow-sm'
            : 'text-muted-foreground'
        )}
      >
        Borger
      </button>
      <button
        role="radio"
        aria-checked={mode === 'expert'}
        onClick={() => onChange('expert')}
        data-testid="mode-expert-button"
        className={cn(
          'rounded-full px-4 py-1.5 text-sm font-medium transition-colors',
          mode === 'expert'
            ? 'bg-background text-foreground shadow-sm'
            : 'text-muted-foreground'
        )}
      >
        Ekspert
      </button>
    </div>
  );
}
