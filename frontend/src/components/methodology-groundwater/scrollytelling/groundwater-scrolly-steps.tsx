'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import {
  IntroCard,
  LocationCard,
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
  {
    id: 'fields',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Pesticider på marken">
        <p>
          I 2016 sprøjtes <strong>Fighter 480</strong> (aktivstof: bentazon) på
          6 marker inde i Espe-oplandet — silomajs og vårbyg, i alt
          42,2&nbsp;ha.
        </p>
        <p className="mt-2">
          Vi kan ikke vide om netop disse marker bidrager til fund i
          grundvandet, men de illustrerer sammenhængen.
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'ingredient',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Aktivstof ≠ produktnavn">
        <p>
          <strong>Bentazon</strong> er aktivstoffet i Fighter 480 og Basagran
          SG. <strong>MCPA</strong> er aktivstoffet i U46 M og Agroxone. Det er
          aktivstoffet der bestemmer hvad der potentielt kan sive ned til
          grundvandet.
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
      <ScrollyCard caveat={CAVEAT} title="Det statistiske mønster">
        <p>
          På tværs af <strong>3.154 oplande</strong> ser vi et klart
          dosis-respons-mønster: oplande med mest bentazon-anvendelse har{' '}
          <strong>4,4× højere detektionsrate</strong> (Q4/Q1). Korrelation
          r&nbsp;=&nbsp;0,213, multivariat p&nbsp;=&nbsp;0,003.
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
          Grundvandsstrømme, jordvariabilitet og multiple kilder gør det umuligt
          at spore ét fund til én mark.
        </p>
        <p className="mt-2">
          Men det overordnede billede er klart:{' '}
          <strong>
            jo mere der sprøjtes i et opland, jo oftere finder vi stofferne i
            grundvandet.
          </strong>
        </p>
      </ScrollyCard>
    ),
  },
];
