'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import { SKRYDSTRUP_WELL, STATS } from './scrolly-constants';

const CAVEAT =
  'Illustrativt eksempel\u00a0— viser rumlig sameksistens, ikke årsagssammenhæng';

export function SkjultCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Skjult i pesticiderne">
      <p>
        N&aring;r vi taler om PFAS &mdash; &lsquo;evighedskemikalierne&rsquo;
        &mdash; t&aelig;nker vi ofte p&aring; industri og brandskum. Men
        landbruget spreder dem ogs&aring;, gemt i helt lovlige pesticider.
      </p>
    </ScrollyCard>
  );
}

export function SkrydstrupCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Skrydstrup, Sønderjylland">
      <p>
        Tag denne hvedemark n&aelig;r Skrydstrup Vandv&aelig;rk. Her
        spr&oslash;jtes med tre forskellige produkter.
      </p>
    </ScrollyCard>
  );
}

export function ByggeklodserCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Kemiens byggeklodser">
      <p>
        F&aelig;lles for dem er, at de indeholder fluor (en CF
        <sub>3</sub>-gruppe). I alt findes der{' '}
        <strong>{STATS.tfaFormingIngredients} af disse stoffer</strong> p&aring;
        det danske marked.
      </p>
    </ScrollyCard>
  );
}

export function GlassetCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Det lander i glasset">
      <p>
        I netop dette opland er der fundet TFA i <strong>syv boringer</strong>.
        Den ene &mdash; DGU&nbsp;{SKRYDSTRUP_WELL.dgu} &mdash; viste niveauer{' '}
        <span className="text-destructive font-semibold">
          49 gange over gr&aelig;nsev&aelig;rdien
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
        Men problemet er, at vi n&aelig;sten ikke leder. Hele{' '}
        <strong>{(100 - STATS.pctMonitored).toFixed(0)}&nbsp;%</strong> af alle
        danske oplande &mdash; inklusiv store landbrugsomr&aring;der som
        S&oslash;ndre Felding her &mdash; har aldrig f&aring;et m&aring;lt for
        PFAS.
      </p>
    </ScrollyCard>
  );
}
