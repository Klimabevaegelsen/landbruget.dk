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
        Denne metodes store styrke er geografien: Den flytter pesticiddata fra
        anonyme regneark ud p&aring; landkortet. Selvom resultaterne er
        estimater og udgangspunkter (ikke kausale beviser), &aring;bner de for
        helt nye typer af analyser.
      </p>

      <PerspectivesGroundwater />
      <PerspectivesHealth />
      <PerspectivesSurfaceWater />
      <PerspectivesBiodiversity />
      <PerspectivesProperty />
    </section>
  );
}
