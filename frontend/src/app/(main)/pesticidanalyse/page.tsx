import Link from 'next/link';
import { Metadata } from 'next';
import { PesticideAnalysisVisualization } from '@/components/pesticide-analysis/PesticideAnalysisVisualization';

export const metadata: Metadata = {
  title: 'Pesticidanalyse | Landbruget.dk',
  description:
    'Analyser pesticidanvendelse hos danske landbrugsvirksomheder med filtrering efter geografi, tid og kemikalietyper.',
  openGraph: {
    title: 'Pesticidanalyse | Landbruget.dk',
    description: 'Analyser pesticidanvendelse hos danske landbrugsvirksomheder',
    type: 'website',
  },
};

export default function PesticideAnalysisPage() {
  return (
    <div className="bg-background min-h-screen">
      <div className="container mx-auto px-4 py-8">
        <div className="border-border mb-8 border-b pb-6">
          <p className="text-muted-foreground mb-2 text-sm font-medium tracking-[0.14em] uppercase">
            Landbruget.dk &middot; Pesticidanalyse
          </p>
          <h1 className="font-display text-foreground mb-3 text-4xl font-bold tracking-tight">
            Pesticidanalyse
          </h1>
          <p className="text-muted-foreground max-w-2xl text-base">
            Analyser pesticidanvendelse hos danske landbrugsvirksomheder.
            Filtrer efter geografi, tidsperiode og kemikalietyper.
          </p>
          <div className="mt-4 flex flex-wrap gap-2">
            <Link
              href="/pesticidanalyse/metode"
              className="bg-primary/10 text-primary hover:bg-primary/20 inline-flex items-center rounded-full px-3.5 py-1.5 text-sm font-medium transition-colors"
            >
              Metode
            </Link>
            <Link
              href="/pesticidanalyse/pfas"
              className="bg-muted text-muted-foreground hover:text-foreground hover:bg-muted/80 inline-flex items-center rounded-full px-3.5 py-1.5 text-sm font-medium transition-colors"
            >
              PFAS
            </Link>
            <Link
              href="/pesticidanalyse/grundvand"
              className="bg-muted text-muted-foreground hover:text-foreground hover:bg-muted/80 inline-flex items-center rounded-full px-3.5 py-1.5 text-sm font-medium transition-colors"
            >
              Grundvand
            </Link>
          </div>
        </div>

        <PesticideAnalysisVisualization />
      </div>
    </div>
  );
}
