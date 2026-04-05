'use client';

interface ScrollyCardProps {
  title: string;
  caveat?: string;
  children: React.ReactNode;
}

export function ScrollyCard({ title, caveat, children }: ScrollyCardProps) {
  return (
    <div className="rounded-lg border p-5">
      <h4 className="font-display text-foreground mb-2 text-base font-semibold">
        {title}
      </h4>
      <div className="text-muted-foreground text-sm leading-relaxed">
        {children}
      </div>
      {caveat && (
        <p className="text-muted-foreground/60 mt-3 text-[10px] italic">
          {caveat}
        </p>
      )}
    </div>
  );
}
