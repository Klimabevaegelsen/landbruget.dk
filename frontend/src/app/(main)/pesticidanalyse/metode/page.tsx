import { Metadata } from 'next';
import { MethodologyPageContent } from '@/components/methodology/methodology-page-content';
import { getCachedPopulationExposureSummary } from '@/lib/server-cache';
import type { PopulationExposureSummary } from '@/lib/server-cache';

export const metadata: Metadata = {
  title: 'Metode: Pesticidanalyse | Landbruget.dk',
  description:
    'Gennemgang af metodologien bag Landbruget.dks pesticidanalyse: databehandlingsforløb, fordeling fra virksomhed til mark, og datakvalitet.',
  openGraph: {
    title: 'Metode: Pesticidanalyse | Landbruget.dk',
    description:
      'Hvordan vi analyserer pesticidanvendelse i Danmark – fra rå data til markniveau.',
    type: 'article',
  },
};

export default async function MethodologyPage() {
  let populationExposureSummary: PopulationExposureSummary | null = null;

  try {
    populationExposureSummary = await getCachedPopulationExposureSummary();
  } catch {
    populationExposureSummary = null;
  }

  return (
    <div className="bg-primary/[0.02] min-h-screen">
      <MethodologyPageContent
        populationExposureSummary={populationExposureSummary}
      />
    </div>
  );
}
