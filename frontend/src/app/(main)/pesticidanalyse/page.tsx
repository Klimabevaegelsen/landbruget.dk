import { Metadata } from 'next';
import PesticideAnalysisVisualization from '@/components/pesticide-analysis/PesticideAnalysisVisualization';

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
        <div className="mb-8">
          <h1 className="text-foreground mb-4 text-4xl font-bold">
            Pesticidanalyse
          </h1>
          <p className="text-muted-foreground max-w-3xl text-lg">
            Analyser pesticidanvendelse hos danske landbrugsvirksomheder.
            Filtrer efter geografi, tidsperiode og kemikalietyper som PFAS,
            diquat og glyphosat.
          </p>
        </div>

        <PesticideAnalysisVisualization />
      </div>
    </div>
  );
}
