import { Metadata } from 'next';
import { PfasPageContent } from '@/components/methodology-pfas/pfas-page-content';

export const metadata: Metadata = {
  title: 'PFAS og grundvand | Landbruget.dk',
  description:
    'Fra fluorpesticid til evighedskemikalie — hvordan fluorholdige pesticider nedbrydes til TFA i dansk grundvand, og hvorfor 64% af grundvandsoplandene mangler PFAS-overvågning.',
  openGraph: {
    title: 'PFAS og grundvand | Landbruget.dk',
    description:
      'Fluorpesticider, TFA og den blinde vinkel i dansk grundvandsovervågning.',
    type: 'article',
  },
};

export default function PfasMethodologyPage() {
  return (
    <div className="bg-background min-h-screen">
      <PfasPageContent />
    </div>
  );
}
