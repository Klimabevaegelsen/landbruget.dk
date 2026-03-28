export function MethodologyHero() {
  return (
    <header className="mb-16" data-testid="methodology-hero">
      <p className="text-muted-foreground mb-3 text-sm tracking-wide">
        Landbruget.dk &middot; Metode
      </p>
      <h1 className="text-foreground text-[36px] leading-[1.15] font-bold tracking-tight lg:text-[42px]">
        Disaggregering af pesticidanvendelse fra virksomhed til mark
      </h1>
      <p className="text-muted-foreground mt-6 text-[18px] leading-[1.6]">
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
