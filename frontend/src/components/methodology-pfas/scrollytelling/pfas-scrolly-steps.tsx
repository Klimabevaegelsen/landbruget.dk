'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import { SKRYDSTRUP_WELL, EXAMPLE_FIELD, STATS } from './scrolly-constants';

const CAVEAT =
  'Illustrativt eksempel\u00a0— viser rumlig sameksistens, ikke årsagssammenhæng';

export const PFAS_STEPS = [
  {
    id: 'field',
    content: (
      <ScrollyCard caveat={CAVEAT} title="En mark i Haderslev">
        <p>
          Vi bruger indvindingsoplandet ved I/S Skrydstrup Vandv&aelig;rk som
          illustrativt eksempel. Her dyrkes{' '}
          <strong>{EXAMPLE_FIELD.crop}</strong> p&aring; {EXAMPLE_FIELD.areaHa}
          &nbsp;ha, og marken spr&oslash;jtes med tre produkter der indeholder
          fluorholdige aktivstoffer.
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'product',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Fra produkt til aktivstof">
        <p>
          <strong>Propulse SE 250</strong> indeholder <strong>fluopyram</strong>{' '}
          &ndash; et fungicid med en CF
          <sub>3</sub>-gruppe. <strong>DFF</strong> indeholder{' '}
          <strong>diflufenican</strong>, og <strong>Mavrik 2F</strong>{' '}
          indeholder <strong>tau-fluvalinat</strong>. Alle tre er fluorholdige.
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'molecule',
    content: (
      <ScrollyCard caveat={CAVEAT} title="CF₃-gruppen gør forskellen">
        <p>
          Fluopyram, diflufenican og tau-fluvalinat har alle en{' '}
          <strong>CF₃-gruppe</strong> (trifluormethyl) i deres molekylestruktur.
          N&aring;r pesticiderne nedbrydes i milj&oslash;et, frigives denne
          CF₃-gruppe som <strong>trifluoreddikesyre (TFA)</strong>.
        </p>
        <p className="mt-2">
          I alt er <strong>{STATS.tfaFormingIngredients} aktivstoffer</strong>{' '}
          registreret i Danmark som kan danne TFA.
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'tfa',
    content: (
      <ScrollyCard caveat={CAVEAT} title="TFA — et evighedskemikalie">
        <p>
          TFA er en <strong>PFAS-forbindelse</strong> (per- og polyfluoralkyl).
          Den er ekstremt persistent &ndash; nedbrydes ikke i milj&oslash;et
          &ndash; og meget mobil i jord og grundvand. TFA kan sive ned gennem
          jordlagene til grundvandet, men ogs&aring; tilf&oslash;res via
          atmosf&aelig;risk deposition fra andre kilder.
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'detection',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Fundet i boringen">
        <p>
          I samme opland, boring DGU&nbsp;{SKRYDSTRUP_WELL.dgu},{' '}
          {SKRYDSTRUP_WELL.depthM}&nbsp;m dybde (
          {SKRYDSTRUP_WELL.detection.date.split('-').reverse().join('.')}
          ):{' '}
          <span className="text-destructive font-semibold">
            {SKRYDSTRUP_WELL.detection.conc}&nbsp;
            {SKRYDSTRUP_WELL.detection.unit} TFA
          </span>
          &nbsp;&ndash; 49&times; over gr&aelig;nsev&aelig;rdien.
        </p>
        <p className="mt-2">
          Vi kan ikke fastsl&aring; at TFA stammer fra netop disse marker
          &ndash; grundvandsstr&oslash;mme, atmosf&aelig;risk deposition og
          multiple kilder g&oslash;r det umuligt at spore &eacute;t fund til
          &eacute;n mark. Men b&aring;de fluorpesticider og TFA findes i samme
          opland: <strong>7 boringer</strong> med TFA (0,075&ndash;4,88
          &nbsp;&micro;g/L).
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'everywhere',
    content: (
      <ScrollyCard caveat={CAVEAT} title="TFA er overalt vi kigger">
        <p>
          P&aring; tv&aelig;rs af{' '}
          <strong>
            {STATS.monitored.toLocaleString('da-DK')} overv&aring;gede oplande
          </strong>{' '}
          er TFA fundet i <strong>100&nbsp;%</strong>. Ingen undtagelser.
          Medianen er 0,075&nbsp;&micro;g/L. Der er udf&oslash;rt{' '}
          <strong>
            {STATS.pfasAnalyses.toLocaleString('da-DK')} PFAS-analyser
          </strong>{' '}
          p&aring; dansk grundvand.
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'blindspot',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Men vi kigger kun her">
        <p>
          <strong>
            {STATS.unmonitored.toLocaleString('da-DK')} oplande (
            {(100 - STATS.pctMonitored).toFixed(0)}&nbsp;%)
          </strong>{' '}
          har <strong>ingen PFAS-overv&aring;gning</strong> overhovedet. Her ses
          S&oslash;nder-Felding Vandv&aelig;rk i Herning &ndash; et
          landbrugsomr&aring;de uden en eneste PFAS-m&aring;ling.
          TFA-overv&aring;gning startede f&oslash;rst i 2020.
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'conclusion',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Den blinde vinkel">
        <p>
          Dette eksempel illustrerer sameksistensen &ndash; ikke individuel
          kausalitet. Multiple kilder og grundvandsstr&oslash;mme g&oslash;r det
          umuligt at spore &eacute;t fund til &eacute;n mark. Men det
          overordnede billede er klart: TFA findes i <strong>alle</strong>{' '}
          overv&aring;gede oplande, og{' '}
          <strong>{(100 - STATS.pctMonitored).toFixed(0)}&nbsp;%</strong> har
          aldrig f&aring;et m&aring;lt. Sp&oslash;rgsm&aring;let er ikke{' '}
          <em>om</em> TFA er der &ndash; men <strong>hvor meget</strong>.
        </p>
      </ScrollyCard>
    ),
  },
];
