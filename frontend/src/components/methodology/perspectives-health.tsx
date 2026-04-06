import {
  Sidenote,
  SubsectionHeader,
} from '@/components/methodology/article-layout';

export function PerspectivesHealth() {
  return (
    <>
      <SubsectionHeader
        id="health"
        number="4.2"
        title="Sundhed og afstandsanalyser"
      />
      <p>
        Epidemiologiske studier viser konsekvent en sammenh&aelig;ng mellem
        n&aelig;rhed til spr&oslash;jtede marker og sygdomme som Parkinsons og
        b&oslash;rnekr&aelig;ft
        <Sidenote number={18}>
          Bresson et al. (2020), scoping review af 151 studier
        </Sidenote>
        <Sidenote number={19}>Brouwer et al. (2022)</Sidenote>
        <Sidenote number={20}>Baldi et al. (2021)</Sidenote>. Ved at kombinere
        vores markdata med Bygnings- og Boligregistret (BBR) kan forskere nu
        beregne den pr&aelig;cise afstand mellem spr&oslash;jtede marker og
        boliger, skoler eller daginstitutioner
        <Sidenote number={21}>Lu et al. (2012)</Sidenote>.
      </p>
    </>
  );
}
