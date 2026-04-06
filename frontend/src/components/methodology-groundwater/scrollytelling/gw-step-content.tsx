'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import { ESPE_WELL } from '@/components/methodology-groundwater/scrollytelling/scrolly-constants';

const CAVEAT = 'Illustrativt eksempel — se afsnit 6.5 om begrænsninger';

export function IntroCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Fra overflade til grundvand">
      <p>
        Når vi sprøjter vores marker, forsvinder stofferne ikke bare. Nogle
        siver langsomt ned mod vores drikkevand. Vi følger to stoffer på deres
        rejse i mørket: <strong>bentazon</strong> og <strong>MCPA</strong>.
      </p>
    </ScrollyCard>
  );
}

export function EspeCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Espe, Midtfyn">
      <p>
        Vi starter ved <strong>Espe Vandværk</strong>. Landskabet over
        vandværket kaldes dets <strong>opland</strong> — det er herfra,
        regnvandet siver ned og fylder vandværkets boringer.
      </p>
    </ScrollyCard>
  );
}

export function KildenCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Kilden over jorden">
      <p>
        I 2016 sprøjtede landmændene 42,2 hektar i dette opland med produktet{' '}
        <strong>Fighter 480</strong>, som indeholder aktivstoffet{' '}
        <strong>bentazon</strong>.
      </p>
    </ScrollyCard>
  );
}

export function FundetCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Fundet i mørket">
      <p>
        Tre år senere, i maj 2019, måler Espe Vandværk{' '}
        <span className="text-destructive font-semibold">
          {ESPE_WELL.detection.conc} {ESPE_WELL.detection.unit} bentazon
        </span>{' '}
        i deres boring. Det er <strong>4.900 gange over grænseværdien</strong>.
      </p>
    </ScrollyCard>
  );
}
