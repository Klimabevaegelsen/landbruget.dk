import { Metadata } from 'next';
import FieldAnalysisVisualization from '@/components/field-analysis/FieldAnalysisVisualization';

export const metadata: Metadata = {
  title: 'Markanalyse - Omfattende Landbrugsdata',
  description:
    'Interaktiv visualisering af danske landbrugsmarker med pesticidforbrug, miljøområder og nærliggende bygninger',
  keywords: [
    'landbrugsmarker',
    'pesticidforbrug',
    'BNBO',
    'lavbundsjorder',
    'tørv',
    'miljøanalyse',
    'Danmark',
  ],
  openGraph: {
    title: 'Markanalyse - Omfattende Landbrugsdata',
    description:
      'Interaktiv visualisering af danske landbrugsmarker med pesticidforbrug, miljøområder og nærliggende bygninger',
    type: 'website',
  },
};

export default function MarkanalysePage() {
  return (
    <div className="min-h-screen bg-gray-50">
      {/* Page Header */}
      <div className="border-b bg-white shadow-sm">
        <div className="mx-auto max-w-7xl px-4 py-4 sm:px-6 lg:px-8 lg:py-6">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900 lg:text-3xl">
                Markanalyse
              </h1>
              <p className="mt-2 max-w-3xl text-sm text-gray-600 lg:text-base">
                Omfattende visualisering af danske landbrugsmarker med
                pesticidforbrug, miljøområder (boringsnære beskyttelsesområder
                og lavbundsjorder), vandprojekter og bygningsnærhed.
              </p>
            </div>
            <div className="text-xs text-gray-500 lg:text-right lg:text-sm">
              617.774 marker • 2024 data
            </div>
          </div>
        </div>
      </div>

      {/* Main Visualization Area */}
      <div className="relative flex-1">
        <FieldAnalysisVisualization />
      </div>

      {/* Data Attribution Footer */}
      <div className="border-t bg-white px-4 py-3">
        <div className="mx-auto max-w-7xl">
          <div className="flex flex-col gap-2 text-xs text-gray-500 lg:flex-row lg:items-center lg:justify-between lg:gap-0">
            <div className="flex flex-col space-y-1 sm:flex-row sm:items-center sm:space-y-0 sm:space-x-6">
              <span>
                Data: Landbrugsstyrelsen, Miljøstyrelsen, Datafordeleren
              </span>
              <span>Opdateret: August 2025</span>
            </div>
            <div className="flex flex-wrap items-center gap-2 sm:gap-4">
              <span>617.774 marker</span>
              <span className="hidden sm:inline">•</span>
              <span>2.761 BNBO områder</span>
              <span className="hidden sm:inline">•</span>
              <span>768.646 lavbundsjorder</span>
              <span className="hidden sm:inline">•</span>
              <span>2.138 vandprojekter</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
