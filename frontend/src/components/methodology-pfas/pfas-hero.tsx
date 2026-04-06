import Link from 'next/link';

export function PfasHero() {
  return (
    <header className="mb-16" data-testid="pfas-hero">
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
        PFAS og grundvand
        <br />
        <span className="text-primary">
          fra spr&oslash;jtemiddel til evighedskemikalie
        </span>
      </h1>

      <p className="text-muted-foreground mt-6 max-w-[620px] text-lg leading-relaxed">
        Fluorholdige spr&oslash;jtemidler nedbrydes til trifluoreddikesyre (TFA)
        &ndash; et PFAS-stof, der findes i 100&nbsp;% af de overv&aring;gede
        grundvandsoplande. Problemet er blot, at kun 36&nbsp;% af oplandene
        overhovedet bliver overv&aring;get for PFAS.
      </p>

      <div className="text-muted-foreground mt-4 flex gap-4 text-[13px]">
        <span>April 2026</span>
        <span>&middot;</span>
        <span>~12 minutters l&aelig;setid</span>
      </div>

      <hr className="border-border mt-8" />
    </header>
  );
}
