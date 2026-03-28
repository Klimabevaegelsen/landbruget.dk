import {
  SectionHeader,
  SubsectionHeader,
} from '@/components/methodology/article-layout';
import { LimitationDataSources } from '@/components/methodology/limitation-data-sources';
import { LimitationDetails } from '@/components/methodology/limitation-details';

export function SectionLimitations() {
  return (
    <section data-testid="section-limitations">
      <SectionHeader
        id="limitations"
        number="5"
        title="Begrænsninger og datakvalitet"
      />

      <p>
        Enhver statistisk fordeling opererer ud fra antagelser, der sætter
        grænser for konklusionernes gyldighed. Her beskrives de væsentligste
        begrænsninger og fejlkilder i metoden.
      </p>

      <SubsectionHeader
        id="lim-tolerance"
        number="5.1"
        title="Arealgrænsen er empirisk bestemt"
      />
      <p>
        Tolerancen på &plusmn;2 % mellem indberettet behandlingsareal og
        beregnet markareal er valgt erfaringsmæssigt som det punkt, der sikrer
        højest mulig datadækning (&ge; 92 %) uden at skabe åbenlyse
        fejlkoblinger. Værdien bygger ikke på en teoretisk model for usikkerhed
        ved opmåling. En strammere regel (f.eks. 1 % tolerance) koster på
        dækningen (falder til ca. 85 %), mens en løsere regel øger risikoen for
        at tildele pesticiderne til de forkerte marker.
      </p>

      <SubsectionHeader
        id="lim-non-disagg"
        number="5.2"
        title="Ikke-koblede dataposter (~8 %)"
      />
      <p>
        Cirka 8 % af pesticidindberetningerne kan ikke knyttes til en specifik
        mark. Det skyldes tre forhold: (i) virksomheden findes ikke i
        markdatasættet, (ii) landmandens indberettede behandlingsareal passer
        ikke med nogen af hans marker, eller (iii) indberetningen er mangelfuld.
        Disse data bevares i systemet uden mark-tilknytning for transparensens
        skyld. Hvis disse 8 % udgøres af bestemte typer landbrug, kan det dog
        skabe en skævvridning af de samlede resultater.
      </p>

      <LimitationDetails />

      <LimitationDataSources />
    </section>
  );
}
