'use client';

interface ReportHeaderProps {
  address: string;
  year: number;
  onBack: () => void;
}

export function ReportHeader({ address, year, onBack }: ReportHeaderProps) {
  return (
    <header className="bg-background/90 absolute top-0 right-0 left-0 z-20 flex items-center gap-3 border-b px-4 py-2.5 backdrop-blur-sm md:right-[420px]">
      <button
        onClick={onBack}
        data-testid="back-to-search-button"
        className="text-muted-foreground flex min-h-[44px] items-center text-sm hover:underline"
      >
        ← Ny søgning
      </button>
      <span className="text-muted-foreground truncate text-xs">{address}</span>
      <span className="text-foreground ml-auto text-sm font-semibold tracking-tight tabular-nums">
        {year}
      </span>
    </header>
  );
}
