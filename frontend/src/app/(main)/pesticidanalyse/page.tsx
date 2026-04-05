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
          <div className="flex items-center gap-4">
            <p className="text-muted-foreground max-w-2xl text-base">
              Analyser pesticidanvendelse hos danske landbrugsvirksomheder.
              Filtrer efter geografi, tidsperiode og kemikalietyper.
            </p>
            <Link
              href="/pesticidanalyse/metode"
              className="text-primary shrink-0 text-sm font-medium underline-offset-4 hover:underline"
            >
              Metode
            </Link>
          </div>
        </div>

        <PesticideAnalysisVisualization />
      </div>
    </div>
  );
}
