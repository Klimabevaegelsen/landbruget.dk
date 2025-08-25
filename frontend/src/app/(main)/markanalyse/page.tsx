import { Metadata } from "next";
import FieldAnalysisVisualization from "@/components/field-analysis/FieldAnalysisVisualization";

export const metadata: Metadata = {
  title: "Markanalyse - Omfattende Landbrugsdata",
  description: "Interaktiv visualisering af danske landbrugsmarker med pesticidforbrug, miljøområder og nærliggende bygninger",
  keywords: ["landbrugsmarker", "pesticidforbrug", "BNBO", "lavbundsjorder", "tørv", "miljøanalyse", "Danmark"],
  openGraph: {
    title: "Markanalyse - Omfattende Landbrugsdata",
    description: "Interaktiv visualisering af danske landbrugsmarker med pesticidforbrug, miljøområder og nærliggende bygninger",
    type: "website",
  },
};

export default function MarkanalysePage() {
  return (
    <div className="min-h-screen bg-gray-50">
      {/* Page Header */}
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4 lg:py-6">
          <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
            <div>
              <h1 className="text-2xl lg:text-3xl font-bold text-gray-900">
                Markanalyse
              </h1>
              <p className="mt-2 text-sm lg:text-base text-gray-600 max-w-3xl">
                Omfattende visualisering af danske landbrugsmarker med pesticidforbrug,
                miljøområder (boringsnære beskyttelsesområder og lavbundsjorder), vandprojekter og bygningsnærhed.
              </p>
            </div>
            <div className="text-xs lg:text-sm text-gray-500 lg:text-right">
              617.774 marker • 2024 data
            </div>
          </div>
        </div>
      </div>

      {/* Main Visualization Area */}
      <div className="flex-1 relative">
        <FieldAnalysisVisualization />
      </div>

      {/* Data Attribution Footer */}
      <div className="bg-white border-t px-4 py-3">
        <div className="max-w-7xl mx-auto">
          <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-2 lg:gap-0 text-xs text-gray-500">
            <div className="flex flex-col sm:flex-row sm:items-center sm:space-x-6 space-y-1 sm:space-y-0">
              <span>Data: Landbrugsstyrelsen, Miljøstyrelsen, Datafordeleren</span>
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
