import { Metadata } from 'next';
import PesticideAnalysisVisualization from '@/components/pesticide-analysis/PesticideAnalysisVisualization';

export const metadata: Metadata = {
  title: 'Pesticidanalyse | Landbruget.dk',
  description: 'Analyser pesticidanvendelse hos danske landbrugsvirksomheder med filtrering efter geografi, tid og kemikalietyper.',
  openGraph: {
    title: 'Pesticidanalyse | Landbruget.dk',
    description: 'Analyser pesticidanvendelse hos danske landbrugsvirksomheder',
    type: 'website',
  },
};

export default function PesticideAnalysisPage() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-green-50 via-blue-50 to-emerald-50">
      <div className="container mx-auto px-4 py-8">
        <div className="mb-8">
          <h1 className="text-4xl font-bold text-gray-900 mb-4">
            Pesticidanalyse
          </h1>
          <p className="text-lg text-gray-700 max-w-3xl">
            Analyser pesticidanvendelse hos danske landbrugsvirksomheder.
            Filtrer efter geografi, tidsperiode og kemikalietyper som PFAS, diquat og glyphosat.
          </p>
        </div>

        <PesticideAnalysisVisualization />
      </div>
    </div>
  );
}
