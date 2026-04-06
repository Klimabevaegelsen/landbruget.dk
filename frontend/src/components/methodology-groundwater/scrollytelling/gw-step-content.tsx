'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import { ESPE_WELL, VEJEN_WELL } from './scrolly-constants';

const CAVEAT =
  'Illustrativt eksempel\u00a0— se afsnit\u00a06.5 om begrænsninger';

export function IntroCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Pesticider og grundvand">
      <p>
        Når pesticider sprøjtes på marker, kan nogle sive ned gennem jorden til
        grundvandet nedenunder. Denne analyse følger to stoffer —{' '}
        <strong>bentazon</strong> og <strong>MCPA</strong> — fra mark til
        detektion i to virkelige eksempler.
      </p>
    </ScrollyCard>
  );
}

export function LocationCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Espe, Midtfyn">
      <p>
        Vi starter ved <strong>Espe Vandværk</strong> på Midtfyn, ca. 20 km
        sydvest for Odense. <strong>Oplandet</strong> (det område hvorfra
        regnvand siver ned til vandværkets grundvand) dækker et typisk dansk
        landbrugslandskab.
      </p>
    </ScrollyCard>
  );
}

export function SoilCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Ned gennem jorden">
      <p>
        Bentazon har en{' '}
        <strong>
          K<sub>oc</sub>&nbsp;~34
        </strong>{' '}
        (et mål for binding til jordpartikler — lavt tal = høj mobilitet). Det
        bindes næsten ikke og er meget mobilt.
      </p>
      <p className="mt-2">
        MCPA nedbrydes hurtigt (
        <strong>
          DT<sub>50</sub>&nbsp;~25 dage
        </strong>
        , dvs. halveringstid i jord), men metabolitten{' '}
        <strong>4-chlor-2-methylphenol</strong> er mere mobil og persistent.
      </p>
    </ScrollyCard>
  );
}

export function VadoseCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Den umættede zone">
      <p>
        <strong>Vadosezonen</strong> er jordlagene mellem markoverfladen og
        grundvandsspejlet — her sker transport og nedbrydning.
      </p>
      <p className="mt-2">
        Bentazon: estimeret transporttid <strong>1,5 år</strong> [CI: 1,0–3,0].
        MCPA-metabolit: estimeret <strong>9 år</strong> [CI: 7,5–9,0]. Dybden og
        jordens egenskaber afgør hastigheden.
      </p>
    </ScrollyCard>
  );
}

export function DetectionCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Bentazon detekteres">
      <p>
        <strong>10. maj 2019</strong>, boring DGU&nbsp;{ESPE_WELL.dgu} (DGU =
        geologisk undersøgelses boringsnummer), {ESPE_WELL.depthM}&nbsp;m dybde:{' '}
        <span className="text-destructive font-semibold">
          {ESPE_WELL.detection.conc}&nbsp;{ESPE_WELL.detection.unit} bentazon
        </span>
        &nbsp;— 4.900× over grænseværdien.
      </p>
      <p className="mt-2">
        Vi kan ikke fastslå at det stammer fra netop disse marker, men
        detektionen sker i samme opland.
      </p>
    </ScrollyCard>
  );
}

export function TransitionCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Et andet eksempel">
      <p>
        Bentazon er ét stof. Hvad med nedbrydningsprodukter? Vi flytter til{' '}
        <strong>Vejen</strong>, ca. 50 km nordvest, for at se et eksempel med
        MCPA og dens metabolit.
      </p>
    </ScrollyCard>
  );
}

export function MetaboliteCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="9 år senere: metabolitten">
      <p>
        Boring DGU&nbsp;{VEJEN_WELL.dgu}, {VEJEN_WELL.depthM}&nbsp;m dybde ved{' '}
        <strong>Vejen Forsyning</strong>. MCPA: altid &lt;0,015&nbsp;µg/L. Men
        metabolitten <strong>4-chlor-2-methylphenol</strong>:{' '}
        <span className="text-primary font-semibold">
          {VEJEN_WELL.detection.conc}&nbsp;{VEJEN_WELL.detection.unit}
        </span>{' '}
        (2021), persistent siden 1997.
      </p>
    </ScrollyCard>
  );
}
