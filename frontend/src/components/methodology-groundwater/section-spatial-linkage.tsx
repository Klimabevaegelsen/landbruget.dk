import {
  SectionHeader,
  SubsectionHeader,
} from '@/components/methodology/article-layout';

export function SectionSpatialLinkage() {
  return (
    <section data-testid="section-spatial-linkage">
      <SectionHeader id="method" number="3" title="Rumlig kobling" />

      <p>
        Det centrale metodiske bidrag er at koble pesticidanvendelse p&aring;
        markniveau med grundvandsbeskyttelsesomr&aring;der og
        overv&aring;gningsboringer. Alle rumlige operationer udf&oslash;res i
        EPSG:25832 (UTM zone 32N), s&aring; areal- og afstandsberegninger
        foreg&aring;r direkte i meter.
      </p>

      <SubsectionHeader
        id="method-grukos"
        number="3.1"
        title="Mark &times; grundvandsopland-kobling"
      />
      <p>
        For hvert &aring;r beregner vi den geometriske sk&aelig;ring (
        <code>ST_Intersection</code>) mellem samtlige landbrugsmarker og
        grundvandsoplandene. Resultatet er en tabel, der for hver mark angiver,
        hvor stor en andel der ligger inden for et indsatsomr&aring;de eller
        indvindingsopland &ndash; og om omr&aring;det er klassificeret som NFI
        eller SFI.
      </p>
      <p>
        Denne kobling muligg&oslash;r to typer analyser: (i) hvilke marker med
        dokumenteret pesticidanvendelse ligger i s&aring;rbare
        grundvandsomr&aring;der, og (ii) hvor stor den samlede
        pesticidbelastning er inden for hvert grundvandsopland.
      </p>

      <SubsectionHeader
        id="method-bnbo"
        number="3.2"
        title="Mark &times; BNBO-kobling"
      />
      <p>
        P&aring; samme m&aring;de beregner vi overlapbet mellem marker og
        BNBO-zoner. Her er analysen s&aelig;rligt relevant efter
        spr&oslash;jteforbuddets ikrafttr&aelig;den 1.&nbsp;juli 2024: vi kan
        identificere pr&aelig;cist, hvilke marker med dokumenteret
        pesticidanvendelse der ligger helt eller delvist inden for et
        BNBO-omr&aring;de.
      </p>
      <p>
        Ved at kombinere BNBO-statusdata med pesticiddata p&aring; markniveau
        kan vi desuden vurdere, om omr&aring;der med status &ldquo;indsats
        n&oslash;dvendig&rdquo; faktisk har aktiv pesticidanvendelse p&aring; de
        tilgr&aelig;nsende marker.
      </p>

      <SubsectionHeader
        id="method-borehole"
        number="3.3"
        title="Boringsoverl&aelig;g"
      />
      <p>
        GRUMO-boringernes geografiske placering overlejres med vores markkort
        for at unders&oslash;ge, om omr&aring;der med intensiv
        pesticidanvendelse korrelerer med forh&oslash;jede fund i det
        underliggende grundvand. For hver boring identificeres de marker, der
        ligger inden for et defineret opland (typisk 1&ndash;5&nbsp;km radius).
      </p>
      <p>
        Denne analyse er eksplorativ &ndash; den kan p&aring;vise geografisk
        sammenfaldenhed, men ikke kausalitet. Grundvandets transporttid fra
        markoverfladen til en boring kan v&aelig;re &aring;rtier, og mange andre
        faktorer p&aring;virker pesticidkoncentrationen (se afsnit 6).
      </p>
    </section>
  );
}
