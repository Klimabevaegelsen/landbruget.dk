'use client';

import { EXAMPLE } from '@/components/methodology/scrolly-example-data';

const F = EXAMPLE.fields;
const P = EXAMPLE.pesticide;

export function DisaggResultCard() {
  return (
    <div className="rounded-lg border p-5">
      <h4 className="font-display text-foreground mb-2 text-base font-semibold">
        Proportional fordeling af {P.totalDose}&nbsp;{P.unit}
      </h4>
      <div className="text-muted-foreground text-sm leading-relaxed">
        <p className="mb-2">
          De {P.totalDose}&nbsp;{P.unit} {P.name} fordeles efter markens andel
          af det samlede areal:
        </p>
        <table className="text-foreground/80 w-full font-mono text-xs">
          <tbody>
            {F.map((f) => (
              <tr key={f.uuid} className="border-border/30 border-b">
                <td className="py-1">{f.areaHa}&nbsp;ha</td>
                <td className="py-1 text-right">
                  {((f.areaHa / P.reportedAreaHa) * 100).toFixed(1)}&nbsp;%
                </td>
                <td className="text-foreground py-1 text-right font-semibold">
                  {f.dose.toFixed(1)}&nbsp;{P.unit}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
