'use client';

import { ScrollyCard } from '@/components/methodology/scrolly-card';
import {
  SkjultCard,
  SkrydstrupCard,
  ByggeklodserCard,
  GlassetCard,
  BlindvinkelCard,
} from './pfas-step-content';

const CAVEAT =
  'Illustrativt eksempel\u00a0— viser rumlig sameksistens, ikke årsagssammenhæng';

export const PFAS_STEPS = [
  { id: 'skjult', content: <SkjultCard /> },
  { id: 'skrydstrup', content: <SkrydstrupCard /> },
  { id: 'byggeklodser', content: <ByggeklodserCard /> },
  {
    id: 'evighed',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Evighedskemikaliet">
        <p>
          N&aring;r pesticiderne nedbrydes i naturen, kn&aelig;kker CF
          <sub>3</sub>-gruppen af og bliver til <strong>TFA</strong>. TFA er et
          PFAS-stof. Det er ekstremt mobilt og forsvinder stort set aldrig.
        </p>
      </ScrollyCard>
    ),
  },
  { id: 'glasset', content: <GlassetCard /> },
  {
    id: 'overalt',
    content: (
      <ScrollyCard caveat={CAVEAT} title="TFA er overalt">
        <p>
          Nej. P&aring; tv&aelig;rs af landets overv&aring;gede oplande er
          detektionsraten <strong>100&nbsp;%</strong>. Overalt hvor vi leder
          efter TFA, finder vi det. Ingen undtagelser.
        </p>
      </ScrollyCard>
    ),
  },
  { id: 'blindvinkel', content: <BlindvinkelCard /> },
  {
    id: 'spoergsmaalet',
    content: (
      <ScrollyCard caveat={CAVEAT} title="Spørgsmålet er ikke 'om'">
        <p>
          Vi mangler data for to tredjedele af Danmark. Sp&oslash;rgsm&aring;let
          er ikke l&aelig;ngere, <em>om</em> der er evighedskemikalier i det
          uoverv&aring;gede grundvand. Sp&oslash;rgsm&aring;let er kun:{' '}
          <strong>Hvor meget?</strong>
        </p>
      </ScrollyCard>
    ),
  },
];
