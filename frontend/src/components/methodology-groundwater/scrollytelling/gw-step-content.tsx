'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import { ESPE_WELL } from '@/components/methodology-groundwater/scrollytelling/scrolly-constants';

const CAVEAT =
  'Illustrativt eksempel\u00a0— se afsnit\u00a06.5 om begrænsninger';

export function IntroCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Fra overflade til grundvand">
      <p>
        N&aring;r vi spr&oslash;jter vores marker, forsvinder stofferne ikke
        bare. Nogle siver langsomt ned mod vores drikkevand. Vi f&oslash;lger to
        stoffer p&aring; deres rejse i m&oslash;rket: <strong>bentazon</strong>{' '}
        og <strong>MCPA</strong>.
      </p>
    </ScrollyCard>
  );
}

export function EspeCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Espe, Midtfyn">
      <p>
        Vi starter ved <strong>Espe Vandv&aelig;rk</strong>. Landskabet over
        vandv&aelig;rket kaldes dets <strong>opland</strong> &mdash; det er
        herfra, regnvandet siver ned og fylder vandv&aelig;rkets boringer.
      </p>
    </ScrollyCard>
  );
}

export function KildenCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Kilden over jorden">
      <p>
        I 2016 spr&oslash;jtede landm&aelig;ndene 42,2&nbsp;hektar i dette
        opland med produktet <strong>Fighter 480</strong>, som indeholder
        aktivstoffet <strong>bentazon</strong>.
      </p>
    </ScrollyCard>
  );
}

export function FundetCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Fundet i m&oslash;rket">
      <p>
        Tre &aring;r senere, i maj 2019, m&aring;ler Espe Vandv&aelig;rk{' '}
        <span className="text-destructive font-semibold">
          {ESPE_WELL.detection.conc}&nbsp;{ESPE_WELL.detection.unit} bentazon
        </span>{' '}
        i deres boring. Det er{' '}
        <strong>4.900 gange over gr&aelig;nsev&aelig;rdien</strong>.
      </p>
    </ScrollyCard>
  );
}
