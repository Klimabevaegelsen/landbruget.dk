import Link from 'next/link';

export function MethodologyHero() {
  return (
    <header className="mb-16" data-testid="methodology-hero">
      <div className="mb-6 flex items-center gap-3 text-sm">
        <Link
          href="/pesticidkort"
          data-testid="back-to-pesticidkort-link"
          className="text-primary font-medium underline-offset-4 hover:underline"
        >
          ← Pesticidkortet
        </Link>
      </div>

      <p className="text-muted-foreground mb-4 text-sm font-medium tracking-[0.14em] uppercase">
        Landbruget.dk &middot; Metode
      </p>

      <h1 className="font-display text-foreground text-[30px] leading-[1.1] font-semibold tracking-tight sm:text-[36px] lg:text-[42px]">
        Disaggregering af pesticidanvendelse
        <br />
        <span className="text-primary">fra virksomhed til mark</span>
      </h1>

      <p className="text-muted-foreground mt-6 max-w-[620px] text-lg leading-relaxed">
        Denne artikel beskriver den metodologi, vi anvender til at fordele
        virksomhedsrapporterede pesticiddata ned til individuelle
        landbrugsmarker. Vi gennemgår datakilderne, den statistiske
        sammenkøringsprocedure, resultaterne af lovlighedskontrollen samt kendte
        begrænsninger i metoden.
      </p>

      <div className="text-muted-foreground mt-4 flex gap-4 text-[13px]">
        <span>Marts 2026</span>
        <span>&middot;</span>
        <span>~12 minutters læsetid</span>
      </div>

      <hr className="border-border mt-8" />
    </header>
  );
}
