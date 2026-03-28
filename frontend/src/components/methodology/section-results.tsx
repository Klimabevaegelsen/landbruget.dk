import { SectionHeader } from '@/components/methodology/article-layout';
import { PerspectivesGroundwater } from '@/components/methodology/perspectives-groundwater';
import { PerspectivesHealth } from '@/components/methodology/perspectives-health';
import { PerspectivesSurfaceWater } from '@/components/methodology/perspectives-surface-water';
import { PerspectivesBiodiversity } from '@/components/methodology/perspectives-biodiversity';
import { PerspectivesProperty } from '@/components/methodology/perspectives-property';

export function SectionResults() {
  return (
    <section data-testid="section-results">
      <SectionHeader id="perspectives" number="4" title="Perspektiver" />

      <p>
        Denne metodes unikke bidrag er rumligt: Den placerer pesticidanvendelse
        på individuelle marker i stedet for på regnearksniveau hos virksomheden.
        Denne geografiske præcision åbner for analyser, der ellers er umulige.
        Nedenfor beskrives fem forsknings- og forvaltningsområder, hvor data på
        markniveau kan bidrage med ny indsigt.
      </p>
      <p>
        Det er vigtigt at understrege, at fordelingen fungerer som et{' '}
        <em>vejledende værktøj</em>. Den estimerer, <em>hvor</em> pesticider med
        stor sandsynlighed er anvendt &ndash; den er ikke en præcis
        eksponeringsmodel. Resultaterne er udgangspunkter for videre
        undersøgelse, ikke kausale beviser.
      </p>

      <PerspectivesGroundwater />
      <PerspectivesHealth />
      <PerspectivesSurfaceWater />
      <PerspectivesBiodiversity />
      <PerspectivesProperty />
    </section>
  );
}
