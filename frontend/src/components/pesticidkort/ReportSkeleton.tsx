'use client';

export function ReportSkeleton() {
  return (
    <div
      aria-live="polite"
      className="animate-fade-slide-up space-y-4 px-5 py-5 motion-reduce:animate-none"
    >
      <p className="text-muted-foreground text-sm">
        Analyserer marker nær din adresse...
      </p>
      <div className="bg-muted h-20 animate-pulse rounded-xl" />
      <div className="bg-muted h-12 animate-pulse rounded-xl" />
      <div className="bg-muted h-24 animate-pulse rounded-xl" />
    </div>
  );
}
