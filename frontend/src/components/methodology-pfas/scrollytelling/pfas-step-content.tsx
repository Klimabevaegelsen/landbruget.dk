'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import { SKRYDSTRUP_WELL, STATS } from './scrolly-constants';

const CAVEAT =
  'Illustrativt eksempel\u00a0— viser rumlig sameksistens, ikke årsagssammenhæng';

export function IntroCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Hvad er et opland?">
      <p>
        Et <strong>indvindingsopland</strong> er det geografiske område hvorfra
        regnvand siver ned og bliver til det grundvand som et vandværk pumper
        op. Danmark har{' '}
        <strong>{STATS.totalCatchments.toLocaleString('da-DK')} oplande</strong>
        . Sprøjtes der med pesticider inden for et opland, kan stofferne
        potentielt nå grundvandet.
      </p>
    </ScrollyCard>
  );
}

export function LocationCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Skrydstrup, Sønderjylland">
      <p>
        Vi starter ved <strong>I/S Skrydstrup Vandværk</strong> i Haderslev
        kommune — ca. 15 km vest for Haderslev by, i et typisk sydjysk
        landbrugsområde. Oplandet her dækker marker med vinterhvede og andre
        afgrøder.
      </p>
    </ScrollyCard>
  );
}

export function DetectionCard() {
  return (
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
        &nbsp;— 49× over grænseværdien.
      </p>
      <p className="mt-2">
        Vi kan ikke fastslå at TFA stammer fra netop disse marker — men både
        fluorpesticider og TFA findes i samme opland:{' '}
        <strong>7 boringer</strong> med TFA (0,075–4,88 µg/L).
      </p>
    </ScrollyCard>
  );
}

export function TransitionCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Fra ét opland til hele landet">
      <p>
        Skrydstrup er ét af {STATS.totalCatchments.toLocaleString('da-DK')}{' '}
        indvindingsoplande i Danmark. Er mønstret det samme andre steder? Vi
        zoomer ud til det nationale billede.
      </p>
    </ScrollyCard>
  );
}

export function BlindspotCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Men vi kigger kun her">
      <p>
        <strong>
          {STATS.unmonitored.toLocaleString('da-DK')} oplande (
          {(100 - STATS.pctMonitored).toFixed(0)}&nbsp;%)
        </strong>{' '}
        har <strong>ingen PFAS-overvågning</strong> overhovedet. Her ses
        Søndre-Felding Vandværk i Herning — et landbrugsområde uden en eneste
        PFAS-måling. TFA-overvågning startede først i 2020.
      </p>
    </ScrollyCard>
  );
}
