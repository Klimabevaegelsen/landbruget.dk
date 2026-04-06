import Link from 'next/link';

export function MethodologyHero() {
  return (
    <header className="mb-16" data-testid="methodology-hero">
      <div className="mb-8 flex items-center gap-3 text-sm">
        <Link
          href="/pesticidkort"
          data-testid="back-to-pesticidkort-link"
          className="text-primary font-medium underline-offset-4 hover:underline"
        >
          &larr; Pesticidkortet
        </Link>
      </div>

      <p className="text-muted-foreground mb-5 text-[11px] font-semibold tracking-[0.2em] uppercase">
        Landbruget.dk &middot; Metode
      </p>

      <h1 className="font-display text-foreground text-[30px] leading-[1.08] font-semibold tracking-tight sm:text-[36px] lg:text-[44px]">
        Fordeling af pesticidforbrug
        <br />
        <span className="text-primary">fra bedrift til mark</span>
      </h1>

      <p className="text-muted-foreground mt-7 max-w-[600px] text-[17px] leading-[1.7]">
        Denne artikel beskriver den metode, vi anvender til at fordele
        bedrifternes indberettede pesticiddata ud p&aring; de enkelte
        landbrugsmarker. Vi gennemg&aring;r datakilderne, den statistiske
        sammenkøring, lovlighedskontrollen samt metodens kendte
        begr&aelig;nsninger.
      </p>

      <div className="text-muted-foreground mt-5 flex items-center gap-3 text-[12px] tracking-wide">
        <span>Marts 2026</span>
        <span className="text-border">&bull;</span>
        <span>~12 min l&aelig;setid</span>
      </div>

      <div className="border-border mt-10 border-t" />
    </header>
  );
}
