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

const CAVEAT = 'Illustrativt eksempel — se afsnit 6.5 om begrænsninger';

export const GROUNDWATER_STEPS = [
  { id: 'intro', content: <IntroCard /> },
  { id: 'espe', content: <EspeCard /> },
  { id: 'kilden', content: <KildenCard /> },
  {
    id: 'rejsen',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Den langsomme rejse">
        <p>
          Stofferne skal passere jordlagene under os — den{' '}
          <strong>umættede zone</strong>. Bentazon rejser hurtigt. Det tager kun{' '}
          <strong>~1,5 år</strong> at nå grundvandet.
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
          Vi kan ikke fælde dom over én specifik mark. Men mønsteret lyver ikke:
          på tværs af <strong>3.154 oplande</strong> ser vi, at jo mere der
          sprøjtes på overfladen, des oftere finder vi stofferne i vores
          drikkevand.
        </p>
      </ScrollyCard>
    ),
  },
];
