'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';

const CAVEAT =
  'Illustrativt eksempel\u00a0\u2014 se afsnit\u00a06.5 om begr\u00e6nsninger';

export function McpaCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="MCPA: Den st&aelig;dige skygge">
      <p>
        Vi flytter 50&nbsp;km nordvest til <strong>Vejen</strong> for at se
        p&aring; MCPA. Selve stoffet forsvinder hurtigt, men det efterlader et
        farligt spor: et nedbrydningsprodukt, der tager{' '}
        <strong>~9 &aring;r</strong> om at sive ned.
      </p>
    </ScrollyCard>
  );
}

export function FortidslevnCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Et fortidslevn">
      <p>
        Fordi det tager n&aelig;sten et &aring;rti at n&aring; ned, er
        grundvandet et ekko af fortiden. I Vejen m&aring;les der i 2021
        h&oslash;je niveauer af dette stof &mdash;{' '}
        <strong>vedvarende siden 1997</strong>.
      </p>
    </ScrollyCard>
  );
}
