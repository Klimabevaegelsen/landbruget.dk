'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import { EXAMPLE_FIELD, STATS } from './scrolly-constants';
import {
  IntroCard,
  LocationCard,
  DetectionCard,
  TransitionCard,
  BlindspotCard,
} from './pfas-step-content';

const CAVEAT =
  'Illustrativt eksempel\u00a0— viser rumlig sameksistens, ikke årsagssammenhæng';

export const PFAS_STEPS = [
  { id: 'intro', content: <IntroCard /> },
  { id: 'location', content: <LocationCard /> },
  {
    id: 'field',
    content: (
      <ScrollyCard caveat={CAVEAT} title="En mark i oplandet">
        <p>
          Inde i Skrydstrup-oplandet dyrkes{' '}
          <strong>{EXAMPLE_FIELD.crop}</strong> p&aring; {EXAMPLE_FIELD.areaHa}
          &nbsp;ha. Marken spr&oslash;jtes med tre produkter der indeholder
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
          <strong>Propulse SE 250</strong> indeholder <strong>fluopyram</strong>
          , <strong>DFF</strong> indeholder <strong>diflufenican</strong>, og{' '}
          <strong>Mavrik 2F</strong> indeholder <strong>tau-fluvalinat</strong>.
          Alle tre er fluorholdige (indeholder en CF<sub>3</sub>-gruppe).
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'molecule',
    content: (
      <ScrollyCard caveat={CAVEAT} title="CF₃-gruppen gør forskellen">
        <p>
          N&aring;r pesticiderne nedbrydes, frigives CF₃-gruppen
          (trifluormethyl) som <strong>trifluoreddikesyre (TFA)</strong>. I alt
          er <strong>{STATS.tfaFormingIngredients} aktivstoffer</strong>{' '}
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
          Den er ekstremt persistent og meget mobil i jord og grundvand. TFA kan
          sive ned til grundvandet, men ogs&aring; tilf&oslash;res via
          atmosf&aelig;risk deposition.
        </p>
      </ScrollyCard>
    ),
  },
  { id: 'detection', content: <DetectionCard /> },
  { id: 'transition', content: <TransitionCard /> },
  {
    id: 'everywhere',
    content: (
      <ScrollyCard caveat={CAVEAT} title="TFA er overalt vi kigger">
        <p>
          P&aring; tv&aelig;rs af{' '}
          <strong>
            {STATS.monitored.toLocaleString('da-DK')} overv&aring;gede oplande
          </strong>{' '}
          er TFA fundet i <strong>100&nbsp;%</strong>. Ingen undtagelser. Der er
          udf&oslash;rt{' '}
          <strong>
            {STATS.pfasAnalyses.toLocaleString('da-DK')} PFAS-analyser
          </strong>{' '}
          p&aring; dansk grundvand.
        </p>
      </ScrollyCard>
    ),
  },
  { id: 'blindspot', content: <BlindspotCard /> },
  {
    id: 'conclusion',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Den blinde vinkel">
        <p>
          TFA findes i <strong>alle</strong> overv&aring;gede oplande, og{' '}
          <strong>{(100 - STATS.pctMonitored).toFixed(0)}&nbsp;%</strong> har
          aldrig f&aring;et m&aring;lt. Sp&oslash;rgsm&aring;let er ikke{' '}
          <em>om</em> TFA er der — men <strong>hvor meget</strong>.
        </p>
      </ScrollyCard>
    ),
  },
];
