import {
  SectionHeader,
  SubsectionHeader,
} from '@/components/methodology/article-layout';

export function SectionSpatialLinkage() {
  return (
    <section data-testid="section-spatial-linkage">
      <SectionHeader id="method" number="3" title="Geografisk kobling" />

      <p>
        Det metodiske gennembrud er evnen til at overlejre markernes
        pesticidforbrug med grundvandskortene. Alt beregnes præcist i meter via
        GIS-koordinater (EPSG:25832).
      </p>

      <SubsectionHeader
        id="method-grukos"
        number="3.1"
        title="Marker i grundvandsoplande"
      />
      <p>
        For hvert år beregner vi det præcise geografiske overlap mellem
        landbrugsmarker og grundvandsoplandene. Det viser den samlede
        pesticidbelastning inden for de sårbare zoner.
      </p>

      <SubsectionHeader id="method-bnbo" number="3.2" title="Marker i BNBO" />
      <p>
        Vi kortlægger, præcist hvilke sprøjtede marker der overlapper med de
        boringsnære beskyttelsesområder – et særligt vigtigt værktøj til at
        monitorere overholdelsen af sprøjteforbuddet fra 2024.
      </p>

      <SubsectionHeader
        id="method-borehole"
        number="3.3"
        title="Boringsoverlæg"
      />
      <p>
        Vi trækker en radius (typisk 1–5 km) omkring hver GRUMO-boring og
        identificerer markernes pesticidforbrug i det pågældende opland for at
        lede efter korrelationer.
      </p>
    </section>
  );
}
