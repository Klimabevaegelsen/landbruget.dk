import React from 'react';
import { Metadata } from 'next';
import FertilizerAnalysisVisualization from '@/components/fertilizer-analysis/FertilizerAnalysisVisualization';

export const metadata: Metadata = {
  title: 'Gødning & Næringsstoffer | Landbruget.dk',
  description: 'Interaktiv analyse af gødningsproduktion, handelsgødning og næringsstofanvendelse på CVR-niveau i Danmark',
  keywords: [
    'gødning',
    'næringsstoffer',
    'kvælstof',
    'fosfor',
    'handelsgødning',
    'biogasproduktion',
    'husdyrgødning',
    'CVR',
    'landbrugsvirksomheder',
    'miljøregnskab',
  ],
  openGraph: {
    title: 'Gødning & Næringsstoffer - Landbruget.dk',
    description: 'Udforsk gødningsproduktion og næringsstofanvendelse i danske landbrugsvirksomheder',
    type: 'website',
  },
};

export default function GødningPage() {
  return (
    <div className="min-h-screen">
      <div className="container mx-auto px-4 py-8">
        <FertilizerAnalysisVisualization />
      </div>
    </div>
  );
}
