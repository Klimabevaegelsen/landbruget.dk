'use client';

import { SubsectionHeader } from '@/components/methodology/article-layout';

const FIELD_DISTRIBUTION = [
  { label: '1 mark (entydig)', pct: 19.6 },
  { label: '2\u20133 marker', pct: 31.9 },
  { label: '4\u20135 marker', pct: 17.9 },
  { label: '6\u201310 marker', pct: 18.7 },
  { label: '11+ marker', pct: 11.9 },
];

export function SectionCoverageAccuracy() {
  return (
    <>
      <SubsectionHeader
        id="coverage-accuracy"
        number="3.5"
        title="Dækning versus fordel\u00ADingsnøjagtighed"
      />
      <p>
        Dækningstallene ovenfor måler <em>koblingssucces</em> &mdash; andelen af
        indberetninger, der kan knyttes til mindst &eacute;n mark. Det er ikke
        det samme som <em>fordelingsnøjagtighed</em>. Forskellen er afgørende.
      </p>

      <p>
        Af de 314.740 S1-koblede poster i 2021 modtager kun 19,6&nbsp;% en
        entydig enmmarkstildeling. De resterende 80,4&nbsp;% fordeles
        proportionalt over flere marker, baseret på en antagelse om ensartet
        behandlingsintensitet. Medianen er tre marker pr. kobling.
      </p>

      <div
        className="border-primary/20 bg-primary/[0.03] my-4 overflow-hidden rounded border"
        data-testid="field-distribution-table"
      >
        <table className="w-full text-[13px]">
          <thead>
            <tr className="border-border/50 border-b">
              <th className="text-foreground/60 px-4 py-2 text-left font-medium">
                Antal marker i kobling
              </th>
              <th className="text-foreground/60 px-4 py-2 text-right font-medium">
                Andel
              </th>
              <th className="px-4 py-2" />
            </tr>
          </thead>
          <tbody>
            {FIELD_DISTRIBUTION.map((row) => {
              const barWidth = { width: `${(row.pct / 31.9) * 100}%` };
              return (
                <tr
                  key={row.label}
                  className="border-border/30 border-b last:border-0"
                >
                  <td className="text-foreground/80 px-4 py-1.5 font-mono">
                    {row.label}
                  </td>
                  <td className="text-foreground/80 px-4 py-1.5 text-right font-mono">
                    {row.pct}&nbsp;%
                  </td>
                  <td className="w-1/3 px-4 py-1.5">
                    <div className="bg-primary/10 h-3 w-full overflow-hidden rounded-sm">
                      <div
                        className="bg-primary/40 h-full rounded-sm"
                        style={barWidth}
                      />
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <p>
        Den reelle fordelingsnøjagtighed ligger derfor i intervallet mellem
        19,6&nbsp;% (kun entydige tildelinger) og 92,1&nbsp;% (fuld
        koblingsdækning). Indsnævring af dette interval kræver validering mod
        uafhængige feltdata, som ikke aktuelt foreligger i Danmark.
      </p>

      <p className="text-muted-foreground mt-2 text-[13px]">
        Bemærk: Kortet viser en <em>nedre grænse</em> for den faktiske
        pesticidanvendelse. SJI-data underrapporterer konsekvent i forhold til
        nationale salgstal, særligt for glyphosat. 92&nbsp;% dækning af et
        datasæt, der i sig selv underrapporterer, betyder at den virkelige
        eksponering er højere end vist.
      </p>
    </>
  );
}
