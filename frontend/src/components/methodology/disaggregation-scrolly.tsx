'use client';

import { ScrollySection } from '@/components/methodology/scrolly-section';
import {
  MethodologyMap,
  type MapStep,
} from '@/components/methodology/methodology-map';
import { EXAMPLE } from '@/components/methodology/scrolly-example-data';
import { ScrollyRecordOverlay } from '@/components/methodology/scrolly-record-overlay';

const F = EXAMPLE.fields;
const P = EXAMPLE.pesticide;

const STEPS = [
  {
    id: 'overview',
    content: (
      <div className="rounded-lg border p-5">
        <h4 className="font-display text-foreground mb-2 text-base font-semibold">
          &Eacute;n indberetning blandt 350.000
        </h4>
        <p className="text-muted-foreground text-sm leading-relaxed">
          I landbrugsåret {EXAMPLE.year}/{EXAMPLE.year + 1} modtog
          Miljøstyrelsen ca. 350.000 pesticidindberetninger. Vi følger &eacute;n
          af dem: <strong>SJI-post #{EXAMPLE.sjiRow}</strong> fra CVR&nbsp;
          {EXAMPLE.cvr} i {EXAMPLE.municipality} kommune.
        </p>
      </div>
    ),
  },
  {
    id: 'zoom',
    content: (
      <div className="rounded-lg border p-5">
        <h4 className="font-display text-foreground mb-2 text-base font-semibold">
          Sprøjtejournalens indhold
        </h4>
        <p className="text-muted-foreground text-sm leading-relaxed">
          Post #{EXAMPLE.sjiRow} siger: CVR&nbsp;{EXAMPLE.cvr} anvendte{' '}
          <strong>
            {P.totalDose}&nbsp;{P.unit} {P.name}
          </strong>{' '}
          (reg.&nbsp;
          {P.regNr}) på {P.reportedAreaHa}&nbsp;ha{' '}
          <em>{EXAMPLE.cropName.toLowerCase()}</em>. Men journalen nævner ikke{' '}
          <em>hvilke</em> marker. Det er hele problemet.
        </p>
      </div>
    ),
  },
  {
    id: 'fields',
    content: (
      <div className="rounded-lg border p-5">
        <h4 className="font-display text-foreground mb-2 text-base font-semibold">
          CVR-opslag i markregisteret
        </h4>
        <p className="text-muted-foreground text-sm leading-relaxed">
          Vi slår CVR&nbsp;{EXAMPLE.cvr} op i FVM {EXAMPLE.fieldYear} og finder{' '}
          <strong>3 marker</strong> med {EXAMPLE.cropName.toLowerCase()}:
          {F.map((f, i) => (
            <span key={f.uuid}>
              {i > 0 ? ',' : ''} {f.areaHa}&nbsp;ha
            </span>
          ))}
          . Samlet: {P.reportedAreaHa}&nbsp;ha.
        </p>
      </div>
    ),
  },
  {
    id: 'match',
    content: (
      <div className="rounded-lg border p-5">
        <h4 className="font-display text-foreground mb-2 text-base font-semibold">
          Arealtjek: {EXAMPLE.areaDeviationPct}&nbsp;% afvigelse
        </h4>
        <p className="text-muted-foreground text-sm leading-relaxed">
          Markernes samlede areal ({P.reportedAreaHa}&nbsp;ha) matcher
          indberetningens behandlingsareal ({P.reportedAreaHa}&nbsp;ha) perfekt.
          Afvigelse: {EXAMPLE.areaDeviationPct}&nbsp;% — langt inden for
          &plusmn;2&nbsp;%-grænsen. Pålidelighedsscore:{' '}
          <strong>{EXAMPLE.confidence}</strong>.
        </p>
      </div>
    ),
  },
  {
    id: 'result',
    content: (
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
          <p className="text-muted-foreground mt-2 text-xs">
            Massebalance: {F.reduce((s, f) => s + f.dose, 0).toFixed(1)}&nbsp;
            {P.unit} = {P.totalDose}&nbsp;{P.unit} &check;
          </p>
        </div>
      </div>
    ),
  },
];

function renderVisual(stepId: string) {
  const step = stepId as MapStep;
  const showRecord = step === 'zoom' || step === 'fields' || step === 'match';
  return (
    <div className="relative h-full w-full">
      <MethodologyMap step={step} />
      <div className="absolute top-3 right-3 left-3 z-10">
        <ScrollyRecordOverlay step={step} visible={showRecord} />
      </div>
    </div>
  );
}

export function DisaggregationScrolly() {
  return (
    <ScrollySection
      steps={STEPS}
      stickyContent={renderVisual}
      className="-mx-6 my-10 lg:-mx-32"
    />
  );
}
