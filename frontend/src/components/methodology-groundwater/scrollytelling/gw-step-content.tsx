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

export function OplandCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Oplandet">
      <p>
        Et <strong>opland</strong> er det areal hvorfra regnvand siver ned og
        ender i vandværkets indvindingsboringer. Det er den geografiske
        forbindelse mellem markanvendelse og drikkevand.
      </p>
      <p className="mt-2">
        Espe-oplandet dækker ca. 8 km² landbrugsjord. De marker der sprøjtes
        inden for oplandet kan potentielt bidrage til fund i grundvandet.
      </p>
    </ScrollyCard>
  );
}

export function SoilCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Nedsivning">
      <p>
        Bentazon binder sig n&aelig;sten ikke til jorden og bev&aelig;ger sig
        hurtigt (estimeret transporttid: 1,5 &aring;r).
      </p>
      <p className="mt-2">
        MCPA nedbrydes hurtigt, men efterlader et nedbrydningsprodukt
        (metabolit) &mdash; <strong>4-chlor-2-methylphenol</strong> &mdash; der
        er langt mere mobilt og persistent (estimeret transporttid: 9 &aring;r).
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
    <ScrollyCard caveat={CAVEAT} title="Fundet i boringen">
      <p>
        Tre &aring;r senere (maj 2019) m&aring;les der{' '}
        <span className="text-destructive font-semibold">
          {ESPE_WELL.detection.conc}&nbsp;{ESPE_WELL.detection.unit} bentazon
        </span>{' '}
        i en {ESPE_WELL.depthM}&nbsp;m dyb boring (DGU&nbsp;{ESPE_WELL.dgu})
        &mdash; 4.900 gange over gr&aelig;nsev&aelig;rdien p&aring;
        0,1&nbsp;&mu;g/L.
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
    <ScrollyCard caveat={CAVEAT} title="Metabolitten dukker op">
      <p>
        En anden, dybere boring ({VEJEN_WELL.depthM}&nbsp;m, DGU&nbsp;
        {VEJEN_WELL.dgu}) ved <strong>Vejen Forsyning</strong> viser i 2021
        h&oslash;je niveauer af MCPA-metabolitten{' '}
        <strong>4-chlor-2-methylphenol</strong>:{' '}
        <span className="text-primary font-semibold">
          {VEJEN_WELL.detection.conc}&nbsp;{VEJEN_WELL.detection.unit}
        </span>
        , persistent siden 1997.
      </p>
    </ScrollyCard>
  );
}
