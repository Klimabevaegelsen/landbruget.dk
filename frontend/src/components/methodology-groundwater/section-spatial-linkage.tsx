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
        pesticidforbrug med grundvandskortene. Alt beregnes pr&aelig;cist i
        meter via GIS-koordinater (EPSG:25832).
      </p>

      <SubsectionHeader
        id="method-grukos"
        number="3.1"
        title="Marker i grundvandsoplande"
      />
      <p>
        For hvert &aring;r beregner vi det pr&aelig;cise geografiske overlap
        mellem landbrugsmarker og grundvandsoplandene. Det viser den samlede
        pesticidbelastning inden for de s&aring;rbare zoner.
      </p>

      <SubsectionHeader id="method-bnbo" number="3.2" title="Marker i BNBO" />
      <p>
        Vi kortl&aelig;gger, pr&aelig;cist hvilke spr&oslash;jtede marker der
        overlapper med de boringsnære beskyttelsesomr&aring;der &ndash; et
        s&aelig;rligt vigtigt v&aelig;rkt&oslash;j til at monitorere
        overholdelsen af spr&oslash;jteforbuddet fra 2024.
      </p>

      <SubsectionHeader
        id="method-borehole"
        number="3.3"
        title="Boringsoverl&aelig;g"
      />
      <p>
        Vi tr&aelig;kker en radius (typisk 1&ndash;5&nbsp;km) omkring hver
        GRUMO-boring og identificerer markernes pesticidforbrug i det
        p&aring;g&aelig;ldende opland for at lede efter korrelationer.
      </p>
    </section>
  );
}
