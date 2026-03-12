import { H3PfasTrigger } from '@/components/pipeline/h3-pfas-trigger';
import { TestTube, BarChart3, BookOpen } from 'lucide-react';

export default function PipelinePage() {
  return (
    <div className="bg-muted min-h-screen py-8">
      <div className="container mx-auto px-4">
        <div className="mb-8 text-center">
          <h1 className="text-foreground mb-4 flex items-center justify-center text-4xl font-bold">
            <TestTube className="mr-3 h-8 w-8" />
            Pipeline Administration
          </h1>
          <p className="text-muted-foreground mx-auto max-w-2xl text-lg">
            Trigger and monitor data processing pipelines for agricultural and
            environmental analysis.
          </p>
        </div>

        <div className="mx-auto max-w-4xl space-y-8">
          {/* H3 PFAS Pipeline Section */}
          <section>
            <div className="mb-6">
              <h2 className="text-foreground mb-2 text-2xl font-semibold">
                H3 PFAS Exposure Analysis
              </h2>
              <p className="text-muted-foreground">
                Process agricultural field data to analyze PFAS-containing
                pesticide exposure at different spatial resolutions using H3
                hexagonal grids and municipality boundaries.
              </p>
            </div>

            <H3PfasTrigger />
          </section>

          {/* Pipeline Status Section */}
          <section className="bg-background rounded-lg p-6 shadow-lg">
            <h2 className="text-foreground mb-4 flex items-center text-2xl font-semibold">
              <BarChart3 className="mr-2 h-6 w-6" />
              Pipeline Status
            </h2>
            <div className="space-y-4">
              <div className="bg-muted flex items-center justify-between rounded-lg p-4">
                <div>
                  <h3 className="font-medium">H3 PFAS Analysis</h3>
                  <p className="text-muted-foreground text-sm">
                    Last run: Check GitHub Actions
                  </p>
                </div>
                <a
                  href="https://github.com/Klimabevaegelsen/landbruget.dk/actions/workflows/h3-pfas-analysis.yml"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-sm font-medium text-blue-600 hover:text-blue-800"
                >
                  View on GitHub →
                </a>
              </div>
            </div>
          </section>

          {/* Documentation Section */}
          <section className="bg-background rounded-lg p-6 shadow-lg">
            <h2 className="text-foreground mb-4 flex items-center text-2xl font-semibold">
              <BookOpen className="mr-2 h-6 w-6" />
              Documentation
            </h2>
            <div className="prose max-w-none">
              <h3>H3 PFAS Pipeline Features:</h3>
              <ul>
                <li>
                  <strong>Multi-Resolution Analysis:</strong> Supports H3
                  resolutions 7-10 (516ha to 1.5ha per hexagon)
                </li>
                <li>
                  <strong>Optimized Processing:</strong> Single process with
                  shared data loading to eliminate redundancy
                </li>
                <li>
                  <strong>Parallel Execution:</strong> Up to 10 concurrent years
                  processed simultaneously
                </li>
                <li>
                  <strong>Comprehensive Output:</strong> Both H3 hexagon-level
                  and municipality-level analysis
                </li>
              </ul>

              <h3>Processing Time:</h3>
              <ul>
                <li>
                  <strong>Single year:</strong> ~10-15 minutes
                </li>
                <li>
                  <strong>Multiple years:</strong> Parallel processing reduces
                  total time
                </li>
                <li>
                  <strong>All resolutions:</strong> Processed efficiently in
                  single job
                </li>
              </ul>

              <h3>Output Locations:</h3>
              <p>Results are stored in GCS at:</p>
              <ul>
                <li>
                  <code>gs://landbruget-data/gold/h3_pesticide_YYYY_resN/</code>
                </li>
                <li>
                  <code>gs://landbruget-data/gold/kommune_pesticide_YYYY/</code>
                </li>
                <li>
                  <code>
                    gs://landbruget-data/gold/pmtiles/h3_pfas_YYYY_resN/
                  </code>
                </li>
              </ul>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}
