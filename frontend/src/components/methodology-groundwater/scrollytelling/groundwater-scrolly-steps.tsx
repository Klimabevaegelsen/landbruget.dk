'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import {
  IntroCard,
  EspeCard,
  KildenCard,
  FundetCard,
} from '@/components/methodology-groundwater/scrollytelling/gw-step-content';
import {
  McpaCard,
  FortidslevnCard,
} from '@/components/methodology-groundwater/scrollytelling/gw-vejen-cards';

const CAVEAT =
  'Illustrativt eksempel\u00a0\u2014 se afsnit\u00a06.5 om begr\u00e6nsninger';

export const GROUNDWATER_STEPS = [
  { id: 'intro', content: <IntroCard /> },
  { id: 'espe', content: <EspeCard /> },
  { id: 'kilden', content: <KildenCard /> },
  {
    id: 'rejsen',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Den langsomme rejse">
        <p>
          Stofferne skal passere jordlagene under os &mdash; den{' '}
          <strong>um&aelig;ttede zone</strong>. Bentazon rejser hurtigt. Det
          tager kun <strong>~1,5 &aring;r</strong> at n&aring; grundvandet.
        </p>
      </ScrollyCard>
    ),
  },
  { id: 'fundet', content: <FundetCard /> },
  { id: 'mcpa', content: <McpaCard /> },
  { id: 'fortidslevn', content: <FortidslevnCard /> },
  {
    id: 'billede',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Det store billede">
        <p>
          Vi kan ikke f&aelig;lde dom over &eacute;n specifik mark. Men
          m&oslash;nsteret lyver ikke: p&aring; tv&aelig;rs af{' '}
          <strong>3.154 oplande</strong> ser vi, at jo mere der spr&oslash;jtes
          p&aring; overfladen, des oftere finder vi stofferne i vores
          drikkevand.
        </p>
      </ScrollyCard>
    ),
  },
];
