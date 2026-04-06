'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';

export const DISAGG_STEPS = [
  {
    id: 'context',
    content: (
      <ScrollyCard title="Det manglende led">
        Alle landm&aelig;nd skal indberette deres pesticidforbrug til
        Milj&oslash;styrelsen. Men journalen rummer en massiv blind vinkel: Den
        n&aelig;vner ikke hvilke marker, der er spr&oslash;jtet. Det hul lukker
        vi nu.
      </ScrollyCard>
    ),
  },
  {
    id: 'location',
    content: (
      <ScrollyCard title="Haderslev, Sydjylland">
        Vi f&oslash;lger &eacute;t konkret spor fra Haderslev &mdash; et typisk
        dansk landbrugsomr&aring;de t&aelig;t p&aring; den tyske gr&aelig;nse.
      </ScrollyCard>
    ),
  },
  {
    id: 'regneark',
    content: (
      <ScrollyCard title="Et anonymt regneark">
        Vi ved, at CVR&nbsp;41996528 har spr&oslash;jtet 51,2&nbsp;liter Roundup
        p&aring; 38,47&nbsp;hektar majs. Men regnearket fort&aelig;ller os ikke
        hvor i landskabet, de hektar ligger.
      </ScrollyCard>
    ),
  },
  {
    id: 'fields',
    content: (
      <ScrollyCard title="Jagten i markregisteret">
        Ved at krydstjekke med det offentlige markregister (FVM) finder vi
        g&aring;rdens majsmarker. Der er pr&aelig;cis tre.
      </ScrollyCard>
    ),
  },
  {
    id: 'match',
    content: (
      <ScrollyCard title="Puslespillet g&aring;r op">
        N&aring;r vi l&aelig;gger de tre markers areal sammen (16,24 + 14,43 +
        7,80&nbsp;ha), rammer vi pr&aelig;cis 38,47&nbsp;hektar. Afvigelsen er
        0&nbsp;%. Vi har fundet de rigtige marker.
      </ScrollyCard>
    ),
  },
  {
    id: 'virkelighed',
    content: (
      <ScrollyCard title="Fra regneark til virkelighed">
        Nu kan vi fordele de 51,2&nbsp;liter Roundup proportionalt ud p&aring;
        landkortet, dr&aring;be for dr&aring;be, baseret p&aring; markernes
        st&oslash;rrelse.
      </ScrollyCard>
    ),
  },
  {
    id: 'scale',
    content: (
      <ScrollyCard title="Vi g&oslash;r det 350.000 gange">
        Dette er ikke bare &eacute;n mark. Vi har bygget en algoritme, der
        udf&oslash;rer dette krydstjek for samtlige ~350.000 indberetninger
        hvert &aring;r. Resultatet? Et unikt, landsd&aelig;kkende kort over
        pesticidbelastning.
      </ScrollyCard>
    ),
  },
];
