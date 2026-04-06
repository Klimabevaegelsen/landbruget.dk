'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';

const CAVEAT = 'Illustrativt eksempel — se afsnit 6.5 om begrænsninger';

export function McpaCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="MCPA: Den stædige skygge">
      <p>
        Vi flytter 50 km nordvest til <strong>Vejen</strong> for at se på MCPA.
        Selve stoffet forsvinder hurtigt, men det efterlader et farligt spor: et
        nedbrydningsprodukt, der tager <strong>~9 år</strong> om at sive ned.
      </p>
    </ScrollyCard>
  );
}

export function FortidslevnCard() {
  return (
    <ScrollyCard caveat={CAVEAT} title="Et fortidslevn">
      <p>
        Fordi det tager næsten et årti at nå ned, er grundvandet et ekko af
        fortiden. I Vejen måles der i 2021 høje niveauer af dette stof —{' '}
        <strong>vedvarende siden 1997</strong>.
      </p>
    </ScrollyCard>
  );
}
