'use client';

import { EXAMPLE } from '@/components/methodology/scrolly-example-data';
import { DisaggResultCard } from '@/components/methodology/disagg-result-card';

const F = EXAMPLE.fields;
const P = EXAMPLE.pesticide;

export const DISAGG_STEPS = [
  {
    id: 'context',
    content: (
      <div className="rounded-lg border p-5">
        <h4 className="font-display text-foreground mb-2 text-base font-semibold">
          Hvad er en spr&oslash;jtejournal?
        </h4>
        <p className="text-muted-foreground text-sm leading-relaxed">
          Alle landm&aelig;nd skal indberette pesticidanvendelse til
          Milj&oslash;styrelsen via <strong>SJI</strong>{' '}
          (Spr&oslash;jtejournal-Indberetning) &mdash; en digital log over hvad
          der spr&oslash;jtes, hvor meget, og p&aring; hvilken afgr&oslash;de.
          Men journalen n&aelig;vner ikke <em>hvilke marker</em>. Det hul fylder
          vi.
        </p>
      </div>
    ),
  },
  {
    id: 'location',
    content: (
      <div className="rounded-lg border p-5">
        <h4 className="font-display text-foreground mb-2 text-base font-semibold">
          Haderslev, Sydjylland
        </h4>
        <p className="text-muted-foreground text-sm leading-relaxed">
          Vi f&oslash;lger &eacute;n indberetning fra {EXAMPLE.municipality}{' '}
          kommune i Sydjylland &mdash; mellem Kolding og Aabenraa, t&aelig;t
          p&aring; den tyske gr&aelig;nse. Et typisk dansk
          landbrugsomr&aring;de.
        </p>
      </div>
    ),
  },
  {
    id: 'record',
    content: (
      <div className="rounded-lg border p-5">
        <h4 className="font-display text-foreground mb-2 text-base font-semibold">
          Hvad indberetningen indeholder
        </h4>
        <p className="text-muted-foreground text-sm leading-relaxed">
          Tabellen viser &eacute;n SJI-r&aelig;kke: <strong>Post</strong>{' '}
          (l&oslash;benummer), <strong>CVR</strong> (virksomhedsnummer),{' '}
          <strong>Afgr&oslash;de</strong>, <strong>Areal</strong> (behandlet),{' '}
          <strong>Produkt</strong> og <strong>Dosis</strong>. L&aelig;g
          m&aelig;rke til: ingen markidentifikation. Vi ved <em>hvad</em> der er
          spr&oslash;jtet, men ikke <em>hvor</em>.
        </p>
      </div>
    ),
  },
  {
    id: 'overview',
    content: (
      <div className="rounded-lg border p-5">
        <h4 className="font-display text-foreground mb-2 text-base font-semibold">
          &Eacute;n indberetning blandt 350.000
        </h4>
        <p className="text-muted-foreground text-sm leading-relaxed">
          I landbrugs&aring;ret {EXAMPLE.year}/{EXAMPLE.year + 1} modtog
          Milj&oslash;styrelsen ca. 350.000 indberetninger. Vi f&oslash;lger
          post #{EXAMPLE.sjiRow} fra CVR&nbsp;{EXAMPLE.cvr}.
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
          Vi sl&aring;r CVR&nbsp;{EXAMPLE.cvr} op i <strong>FVM</strong> (det
          offentlige register over alle danske landbrugsmarker) for{' '}
          {EXAMPLE.fieldYear} og finder <strong>3 marker</strong> med{' '}
          {EXAMPLE.cropName.toLowerCase()}:
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
          Markernes samlede areal matcher indberetningens behandlingsareal
          perfekt. Afvigelse: {EXAMPLE.areaDeviationPct}&nbsp;% &mdash; langt
          inden for &plusmn;2&nbsp;%-gr&aelig;nsen. P&aring;lidelighedsscore:{' '}
          <strong>{EXAMPLE.confidence}</strong>.
        </p>
      </div>
    ),
  },
  { id: 'result', content: <DisaggResultCard /> },
  {
    id: 'summary',
    content: (
      <div className="rounded-lg border p-5">
        <h4 className="font-display text-foreground mb-2 text-base font-semibold">
          Resultatet
        </h4>
        <p className="text-muted-foreground text-sm leading-relaxed">
          Fra &eacute;n anonym indberetning &rarr; 3 identificerede marker med
          individuelle doser. Denne proces k&oslash;rer for alle ~350.000
          indberetninger. Konfidensscoren ({EXAMPLE.confidence}) angiver hvor
          sikkert matchet er.
        </p>
      </div>
    ),
  },
];
