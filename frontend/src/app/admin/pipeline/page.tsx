import { H3PfasTrigger } from "@/components/pipeline/h3-pfas-trigger";

export default function PipelinePage() {
  return (
    <div className="min-h-screen bg-gray-50 py-8">
      <div className="container mx-auto px-4">
        <div className="mb-8 text-center">
          <h1 className="text-4xl font-bold text-gray-900 mb-4">
            🧪 Pipeline Administration
          </h1>
          <p className="text-lg text-gray-600 max-w-2xl mx-auto">
            Trigger and monitor data processing pipelines for agricultural and environmental analysis.
          </p>
        </div>

        <div className="max-w-4xl mx-auto space-y-8">
          {/* H3 PFAS Pipeline Section */}
          <section>
            <div className="mb-6">
              <h2 className="text-2xl font-semibold text-gray-900 mb-2">
                H3 PFAS Exposure Analysis
              </h2>
              <p className="text-gray-600">
                Process agricultural field data to analyze PFAS-containing pesticide exposure 
                at different spatial resolutions using H3 hexagonal grids and municipality boundaries.
              </p>
            </div>
            
            <H3PfasTrigger />
          </section>

          {/* Pipeline Status Section */}
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-semibold text-gray-900 mb-4">
              📊 Pipeline Status
            </h2>
            <div className="space-y-4">
              <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
                <div>
                  <h3 className="font-medium">H3 PFAS Analysis</h3>
                  <p className="text-sm text-gray-600">Last run: Check GitHub Actions</p>
                </div>
                <a 
                  href="https://github.com/Klimabevaegelsen/landbruget.dk/actions/workflows/h3-pfas-analysis.yml"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-blue-600 hover:text-blue-800 text-sm font-medium"
                >
                  View on GitHub →
                </a>
              </div>
            </div>
          </section>

          {/* Documentation Section */}
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-semibold text-gray-900 mb-4">
              📚 Documentation
            </h2>
            <div className="prose max-w-none">
              <h3>H3 PFAS Pipeline Features:</h3>
              <ul>
                <li><strong>Multi-Resolution Analysis:</strong> Supports H3 resolutions 7-10 (516ha to 1.5ha per hexagon)</li>
                <li><strong>Optimized Processing:</strong> Single process with shared data loading to eliminate redundancy</li>
                <li><strong>Parallel Execution:</strong> Up to 10 concurrent years processed simultaneously</li>
                <li><strong>Comprehensive Output:</strong> Both H3 hexagon-level and municipality-level analysis</li>
              </ul>
              
              <h3>Processing Time:</h3>
              <ul>
                <li><strong>Single year:</strong> ~10-15 minutes</li>
                <li><strong>Multiple years:</strong> Parallel processing reduces total time</li>
                <li><strong>All resolutions:</strong> Processed efficiently in single job</li>
              </ul>

              <h3>Output Locations:</h3>
              <p>Results are stored in GCS at:</p>
              <ul>
                <li><code>gs://landbrugsdata-raw-data/gold/h3_pesticide_YYYY_resN/</code></li>
                <li><code>gs://landbrugsdata-raw-data/gold/kommune_pesticide_YYYY/</code></li>
                <li><code>gs://landbrugsdata-raw-data/gold/pmtiles/h3_pfas_YYYY_resN/</code></li>
              </ul>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
} 