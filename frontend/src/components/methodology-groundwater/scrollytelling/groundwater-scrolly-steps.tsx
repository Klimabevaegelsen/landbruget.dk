'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import {
  IntroCard,
  LocationCard,
  OplandCard,
  SoilCard,
  VadoseCard,
  DetectionCard,
  TransitionCard,
  MetaboliteCard,
} from './gw-step-content';

const CAVEAT =
  'Illustrativt eksempel\u00a0— se afsnit\u00a06.5 om begrænsninger';

export const GROUNDWATER_STEPS = [
  { id: 'intro', content: <IntroCard /> },
  { id: 'location', content: <LocationCard /> },
  { id: 'opland', content: <OplandCard /> },
  {
    id: 'fields',
    content: (
      <ScrollyCard caveat={CAVEAT} title="P&aring; marken">
        <p>
          I 2016 blev der spr&oslash;jtet med <strong>Fighter 480</strong>{' '}
          (aktivstof: bentazon) p&aring; seks marker inden for oplandet &mdash;
          silomajs og v&aring;rbyg, i alt 42,2&nbsp;ha.
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'ingredient',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Aktivstof vs. produkt">
        <p>
          Det er aktivstoffet bentazon, der potentielt siver ned &mdash; ikke
          produktnavnet <em>Fighter 480</em>. Tilsvarende g&aelig;lder for
          aktivstoffet MCPA (i produkterne U46 M og Agroxone).
        </p>
      </ScrollyCard>
    ),
  },
  { id: 'soil', content: <SoilCard /> },
  { id: 'vadose', content: <VadoseCard /> },
  { id: 'detection', content: <DetectionCard /> },
  {
    id: 'doseresponse',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Det statistiske m&oslash;nster">
        <p>
          P&aring; tv&aelig;rs af <strong>3.154 oplande</strong> ser vi et klart
          m&oslash;nster: oplande med mest bentazon-anvendelse har{' '}
          <strong>4,4&times; h&oslash;jere detektionsrate</strong> (Q4/Q1).
        </p>
      </ScrollyCard>
    ),
  },
  { id: 'transition', content: <TransitionCard /> },
  { id: 'metabolite', content: <MetaboliteCard /> },
  {
    id: 'conclusion',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Hvad det betyder">
        <p>
          Vi kan ikke bevise, at forureningen stammer fra netop disse marker, da
          grundvandstr&oslash;mme er komplekse.
        </p>
        <p className="mt-2">
          Men p&aring; tv&aelig;rs af 3.154 oplande ser vi et klart statistisk
          m&oslash;nster:{' '}
          <strong>
            jo mere der spr&oslash;jtes i et opland, des oftere finder vi
            stofferne i vandet.
          </strong>
        </p>
      </ScrollyCard>
    ),
  },
];
