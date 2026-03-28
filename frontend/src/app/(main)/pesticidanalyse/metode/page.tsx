import { Metadata } from 'next';
import { MethodologyPageContent } from '@/components/methodology/methodology-page-content';

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

export default function MethodologyPage() {
  return (
    <div className="bg-background min-h-screen">
      <MethodologyPageContent />
    </div>
  );
}
