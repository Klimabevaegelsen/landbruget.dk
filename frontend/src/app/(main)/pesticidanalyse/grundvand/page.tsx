import { Metadata } from 'next';
import { GrundvandPageContent } from '@/components/methodology-groundwater/grundvand-page-content';

export const metadata: Metadata = {
  title: 'Pesticider og grundvand | Landbruget.dk',
  description:
    'Metode for kobling af pesticidanvendelse på markniveau med grundvandsoplande, boringsnære beskyttelsesområder (BNBO) og GEUS grundvandsovervågning.',
  openGraph: {
    title: 'Pesticider og grundvand | Landbruget.dk',
    description:
      'Hvordan vi kobler pesticiddata til grundvandsbeskyttelse – grundvandsoplande, BNBO og GRUMO.',
    type: 'article',
  },
};

export default function GroundwaterMethodologyPage() {
  return (
    <div className="bg-background min-h-screen">
      <GrundvandPageContent />
    </div>
  );
}
