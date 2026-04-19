export function ReportLoadingSkeleton() {
  return (
    <div
      aria-live="polite"
      className="animate-fade-slide-up space-y-4 px-5 py-5 motion-reduce:animate-none"
    >
      <p className="text-muted-foreground text-sm">
        Analyserer marker n&aelig;r din adresse...
      </p>
      {['h-20', 'h-12', 'h-24'].map((h) => (
        <div key={h} className={`bg-muted ${h} animate-pulse rounded-xl`} />
      ))}
    </div>
  );
}
