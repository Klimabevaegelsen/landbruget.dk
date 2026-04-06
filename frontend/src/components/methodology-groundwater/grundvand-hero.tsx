import Link from 'next/link';

export function GrundvandHero() {
  return (
    <header className="mb-16" data-testid="grundvand-hero">
      <div className="mb-6 flex items-center gap-3 text-sm">
        <Link
          href="/pesticidanalyse"
          data-testid="back-to-pesticidanalyse-link"
          className="text-primary font-medium underline-offset-4 hover:underline"
        >
          &larr; Pesticidanalyse
        </Link>
        <span className="text-muted-foreground">/</span>
        <Link
          href="/pesticidanalyse/metode"
          data-testid="back-to-metode-link"
          className="text-primary font-medium underline-offset-4 hover:underline"
        >
          Metode
        </Link>
      </div>

      <p className="text-muted-foreground mb-4 text-sm font-medium tracking-[0.14em] uppercase">
        Landbruget.dk &middot; Metode
      </p>

      <h1 className="font-display text-foreground text-[36px] leading-[1.1] font-semibold tracking-tight lg:text-[42px]">
        Pesticider og grundvand
        <br />
        <span className="text-primary">Fra mark til boring</span>
      </h1>

      <p className="text-muted-foreground mt-6 max-w-[620px] text-lg leading-relaxed">
        Denne artikel beskriver, hvordan vi kobler pesticidforbrug p&aring;
        markniveau med beskyttelsen af det danske grundvand &ndash; fra
        grundvandskortl&aelig;gningens indsatsomr&aring;der og BNBO-zoner til
        GEUS&apos; nationale grundvandsoverv&aring;gning.
      </p>

      <div className="text-muted-foreground mt-4 flex gap-4 text-[13px]">
        <span>April 2026</span>
        <span>&middot;</span>
        <span>~15 minutters l&aelig;setid</span>
      </div>

      <hr className="border-border mt-8" />
    </header>
  );
}
