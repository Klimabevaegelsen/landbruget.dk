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
        title="Dækning versus fordelingsnøjagtighed"
      />
      <p>
        Det er vigtigt at skelne mellem, hvor meget data vi kan <em>koble</em>,
        og hvor præcist det kan <em>fordeles</em>. Selvom vi kobler 92 % af
        dataene, er det kun ca. 19,6 % af indberetningerne, der kan knyttes til
        én specifik mark. De resterende 80,4 % fordeles proportionalt over flere
        marker (medianen er tre marker per kobling). Det betyder, at kortet
        viser en <em>udjævnet</em> fordeling: for nogle marker vil det viste
        forbrug være højere end det reelle, for andre lavere.
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
                    {row.pct} %
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
    </>
  );
}
