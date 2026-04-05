'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import { ESPE_WELL, VEJEN_WELL } from './scrolly-constants';

const CAVEAT =
  'Illustrativt eksempel\u00a0— se afsnit\u00a06.5 om begrænsninger';

export const GROUNDWATER_STEPS = [
  {
    id: 'fields',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Pesticider på marken">
        <p>
          Vi bruger indvindingsoplandet ved Espe Vandværk som illustrativt
          eksempel. I 2016 sprøjtes <strong>Fighter 480</strong> (aktivstof:
          bentazon) på 6 marker inde i oplandet&nbsp;— silomajs og vårbyg, i alt
          42,2&nbsp;ha.
        </p>
        <p className="mt-2">
          Vi kan ikke vide om netop disse marker bidrager til fund i
          grundvandet, men de illustrerer sammenhængen mellem anvendelse og
          detektion.
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
          aktivstoffet&nbsp;— ikke produktnavnet&nbsp;— der bestemmer hvad der
          potentielt kan sive ned til grundvandet.
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'soil',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Ned gennem jorden">
        <p>
          Bentazon (K<sub>oc</sub>&nbsp;~34) bindes næsten ikke til
          jordpartikler og er meget mobilt. MCPA nedbrydes hurtigt (DT
          <sub>50</sub>&nbsp;~25 dage), men metabolitten{' '}
          <strong>4-chlor-2-methylphenol</strong> er mere mobil og persistent.
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'vadose',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Vadosezonen: tid til grundvandet">
        <p>
          Bentazon: estimeret transporttid <strong>1,5 år</strong> [CI:
          1,0–3,0]. MCPA-metabolit: estimeret <strong>9 år</strong> [CI:
          7,5–9,0]. Dybden til grundvandet og jordens egenskaber afgør
          hastigheden.
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'detection',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Bentazon detekteres">
        <p>
          <strong>10. maj 2019</strong>, boring DGU&nbsp;{ESPE_WELL.dgu},{' '}
          {ESPE_WELL.depthM}&nbsp;m dybde:{' '}
          <span className="text-destructive font-semibold">
            {ESPE_WELL.detection.conc}&nbsp;{ESPE_WELL.detection.unit} bentazon
          </span>
          &nbsp;— 4.900× over grænseværdien på 0,1&nbsp;µg/L.
        </p>
        <p className="mt-2">
          Vi kan ikke fastslå at det stammer fra netop disse marker, men
          detektionen sker i samme opland hvor bentazon er anvendt.
        </p>
      </ScrollyCard>
    ),
  },
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
  {
    id: 'metabolite',
    content: (
      <ScrollyCard caveat={CAVEAT} title="9 år senere: metabolitten">
        <p>
          Et andet illustrativt eksempel: boring DGU&nbsp;{VEJEN_WELL.dgu},{' '}
          {VEJEN_WELL.depthM}&nbsp;m dybde ved Vejen Forsyning. MCPA: altid
          &lt;0,015&nbsp;µg/L. Men metabolitten{' '}
          <strong>4-chlor-2-methylphenol</strong>:{' '}
          <span className="font-semibold text-amber-600">
            {VEJEN_WELL.detection.conc}&nbsp;{VEJEN_WELL.detection.unit}
          </span>{' '}
          (2021), persistent siden 1997.
        </p>
      </ScrollyCard>
    ),
  },
  {
    id: 'conclusion',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Hvad det betyder">
        <p>
          Disse eksempler illustrerer det statistiske mønster&nbsp;— ikke
          individuel kausalitet. Grundvandsstrømme, jordvariabilitet og multiple
          kilder gør det umuligt at spore ét fund til én mark.
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
