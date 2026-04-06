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
        Epidemiologiske studier viser konsekvent en sammenhæng mellem nærhed til
        sprøjtede marker og sygdomme som Parkinsons og børnekræft
        <Sidenote number={18}>
          Bresson et al. (2020), scoping review af 151 studier
        </Sidenote>
        <Sidenote number={19}>Brouwer et al. (2022)</Sidenote>
        <Sidenote number={20}>Baldi et al. (2021)</Sidenote>. Ved at kombinere
        vores markdata med Bygnings- og Boligregistret (BBR) kan forskere nu
        beregne den præcise afstand mellem sprøjtede marker og boliger, skoler
        eller daginstitutioner
        <Sidenote number={21}>Lu et al. (2012)</Sidenote>.
      </p>
    </>
  );
}
