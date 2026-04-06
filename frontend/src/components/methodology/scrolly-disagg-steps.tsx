'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';

export const DISAGG_STEPS = [
  {
    id: 'context',
    content: (
      <ScrollyCard title="Det manglende led">
        Alle landmænd skal indberette deres pesticidforbrug til Miljøstyrelsen.
        Men journalen rummer en massiv blind vinkel: Den nævner ikke hvilke
        marker, der er sprøjtet. Det hul lukker vi nu.
      </ScrollyCard>
    ),
  },
  {
    id: 'location',
    content: (
      <ScrollyCard title="Haderslev, Sydjylland">
        Vi følger ét konkret spor fra Haderslev — et typisk dansk
        landbrugsområde tæt på den tyske grænse.
      </ScrollyCard>
    ),
  },
  {
    id: 'regneark',
    content: (
      <ScrollyCard title="Et anonymt regneark">
        Vi ved, at CVR 41996528 har sprøjtet 51,2 liter Roundup på 38,47 hektar
        majs. Men regnearket fortæller os ikke hvor i landskabet, de hektar
        ligger.
      </ScrollyCard>
    ),
  },
  {
    id: 'fields',
    content: (
      <ScrollyCard title="Jagten i markregisteret">
        Ved at krydstjekke med det offentlige markregister (FVM) finder vi
        gårdens majsmarker. Der er præcis tre.
      </ScrollyCard>
    ),
  },
  {
    id: 'match',
    content: (
      <ScrollyCard title="Puslespillet går op">
        Når vi lægger de tre markers areal sammen (16,24 + 14,43 + 7,80 ha),
        rammer vi præcis 38,47 hektar. Afvigelsen er 0 %. Vi har fundet de
        rigtige marker.
      </ScrollyCard>
    ),
  },
  {
    id: 'virkelighed',
    content: (
      <ScrollyCard title="Fra regneark til virkelighed">
        Nu kan vi fordele de 51,2 liter Roundup proportionalt ud på landkortet,
        dråbe for dråbe, baseret på markernes størrelse.
      </ScrollyCard>
    ),
  },
  {
    id: 'scale',
    content: (
      <ScrollyCard title="Vi gør det 350.000 gange">
        Dette er ikke bare én mark. Vi har bygget en algoritme, der udfører
        dette krydstjek for samtlige ~350.000 indberetninger hvert år.
        Resultatet? Et unikt, landsdækkende kort over pesticidbelastning.
      </ScrollyCard>
    ),
  },
];
