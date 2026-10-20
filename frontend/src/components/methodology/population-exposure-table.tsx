import type {
  PopulationBand,
  PopulationExposureDefinition,
  PopulationExposureSummary,
} from '@/lib/server-cache';
import { formatApproxPersons, formatShare } from '@/lib/population-exposure';

interface PopulationExposureTableProps {
  summary: PopulationExposureSummary;
}

const COLUMNS: Array<{
  definition: PopulationExposureDefinition;
  title: string;
}> = [
  {
    definition: 'treated_field',
    title: 'Mark med rapporteret pesticidforbrug',
  },
  { definition: 'conventional_field', title: 'Konventionel mark' },
  { definition: 'any_field', title: 'Enhver landbrugsmark' },
];

function findBand(
  summary: PopulationExposureSummary,
  definition: PopulationExposureDefinition,
  bandM: number
): PopulationBand | undefined {
  return summary.national[definition].find((band) => band.band_m === bandM);
}

export function PopulationExposureTable({
  summary,
}: PopulationExposureTableProps) {
  return (
    <div className="mt-8 overflow-x-auto">
      <table
        data-testid="population-exposure-table"
        className="w-full min-w-[680px] border-collapse text-sm"
      >
        <caption className="text-muted-foreground caption-top pb-2 text-left text-xs">
          Pesticidår {summary.pesticide_year} · marker {summary.field_year} ·
          befolkning {summary.population_source.period}
        </caption>
        <thead>
          <tr className="border-border border-b">
            <th scope="col" className="px-2 py-2 text-left font-medium">
              Afstand
            </th>
            {COLUMNS.map(({ definition, title }) => (
              <th
                key={definition}
                scope="col"
                className="px-2 py-2 text-right font-medium"
              >
                {title}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {summary.bands_m.map((bandM) => (
            <tr key={bandM} className="border-border/60 border-b">
              <th scope="row" className="px-2 py-2 text-left font-normal">
                ≤ {bandM} m
              </th>
              {COLUMNS.map(({ definition }) => {
                const band = findBand(summary, definition, bandM);
                const isHeadline =
                  bandM === 100 && definition === 'treated_field';

                return (
                  <td
                    key={definition}
                    className={`px-2 py-2 text-right tabular-nums ${isHeadline ? 'font-semibold' : ''}`}
                  >
                    {band ? (
                      <>
                        <span>{formatApproxPersons(band.persons)}</span>
                        <span className="text-muted-foreground block text-xs">
                          {formatShare(band.share_of_population)}
                        </span>
                      </>
                    ) : (
                      <span className="text-muted-foreground">—</span>
                    )}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
