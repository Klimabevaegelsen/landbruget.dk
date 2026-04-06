'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import { SKRYDSTRUP_WELL, STATS } from './scrolly-constants';

const CAVEAT =
  'Illustrativt eksempel — viser rumlig sameksistens, ikke årsagssammenhæng';

export function SkjultCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Skjult i pesticiderne">
      <p>
        Når vi taler om PFAS — 'evighedskemikalierne' — tænker vi ofte på
        industri og brandskum. Men landbruget spreder dem også, gemt i helt
        lovlige pesticider.
      </p>
    </ScrollyCard>
  );
}

export function SkrydstrupCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Skrydstrup, Sønderjylland">
      <p>
        Tag denne hvedemark nær Skrydstrup Vandværk. Her sprøjtes med tre
        forskellige produkter.
      </p>
    </ScrollyCard>
  );
}

export function ByggeklodserCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Kemiens byggeklodser">
      <p>
        Fælles for dem er, at de indeholder fluor (en CF
        <sub>3</sub>-gruppe). I alt findes der{' '}
        <strong>{STATS.tfaFormingIngredients} af disse stoffer</strong> på det
        danske marked.
      </p>
    </ScrollyCard>
  );
}

export function GlassetCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Det lander i glasset">
      <p>
        I netop dette opland er der fundet TFA i <strong>syv boringer</strong>.
        Den ene — DGU {SKRYDSTRUP_WELL.dgu} — viste niveauer{' '}
        <span className="text-destructive font-semibold">
          49 gange over grænseværdien
        </span>
        . Er det et isoleret problem?
      </p>
    </ScrollyCard>
  );
}

export function BlindvinkelCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Den blinde vinkel">
      <p>
        Men problemet er, at vi næsten ikke leder. Hele{' '}
        <strong>{(100 - STATS.pctMonitored).toFixed(0)} %</strong> af alle
        danske oplande — inklusiv store landbrugsområder som Søndre Felding her
        — har aldrig fået målt for PFAS.
      </p>
    </ScrollyCard>
  );
}
