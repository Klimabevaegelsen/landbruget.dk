import { Metadata } from "next";
import FieldAnalysisVisualization from "@/components/field-analysis/FieldAnalysisVisualization";

export const metadata: Metadata = {
  title: "Markanalyse - Omfattende Landbrugsdata",
  description: "Interaktiv visualisering af danske landbrugsmarker med pesticidforbrug, miljøområder og nærliggende bygninger",
  keywords: ["landbrugsmarker", "pesticidforbrug", "BNBO", "vådområder", "miljøanalyse", "Danmark"],
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
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">
                Markanalyse
              </h1>
              <p className="mt-2 text-gray-600 max-w-3xl">
                Omfattende visualisering af danske landbrugsmarker med pesticidforbrug,
                miljøområder (BNBO og vådområder), vandprojekter og bygningsnærhed.
              </p>
            </div>
            <div className="text-sm text-gray-500">
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
          <div className="flex flex-wrap items-center justify-between text-xs text-gray-500">
            <div className="flex items-center space-x-6">
              <span>Data: Landbrugsstyrelsen, Miljøstyrelsen, Datafordeleren</span>
              <span>Opdateret: August 2024</span>
            </div>
            <div className="flex items-center space-x-4">
              <span>617.774 marker</span>
              <span>•</span>
              <span>2.761 BNBO områder</span>
              <span>•</span>
              <span>768.646 vådområder</span>
              <span>•</span>
              <span>2.138 vandprojekter</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
