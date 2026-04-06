import {
  SectionHeader,
  SubsectionHeader,
  Figure,
} from '@/components/methodology/article-layout';
import { PipelineDiagram } from '@/components/methodology/pipeline-diagram';
import { DeepDive } from '@/components/methodology/deep-dive';

export function SectionDataSources() {
  return (
    <section data-testid="section-data-sources">
      <SectionHeader id="data" number="1" title="Datakilder" />

      <p>
        Analysen bygger p&aring; tre prim&aelig;re datakilder fra de danske
        myndigheder, suppleret med bygningsdata til afstandsanalyser. Alle data
        hentes i maskinl&aelig;sbart format og behandles i en tretrinsproces
        (kendt som en <em>Medallion-arkitektur</em>, Figur 1).
      </p>

      <Figure
        id="fig-pipeline"
        number={1}
        caption="Medallion-arkitekturen. Rå data bevares uændret i Bronze-laget, standardiseres i Silver og fordeles til markniveau i Gold."
      >
        <PipelineDiagram />
      </Figure>

      <SubsectionHeader id="data-fvm" number="1.1" title="Markdata (FVM)" />
      <p>
        Landbrugsstyrelsens F&aelig;llesskema indeholder markblokke og
        afgr&oslash;dedata for samtlige danske landbrugsmarker. Hvert
        datas&aelig;t er versioneret per kalender&aring;r og indeholder markens
        areal (ha), afgr&oslash;dekode, CVR-nummer og geografiske koordinater
        (EPSG:25832). Vi anvender markdata fra &aring;ret <em>efter</em>{' '}
        pesticidanvendelsen (se afsnit 3.1 om tidsforskydning).
      </p>

      <SubsectionHeader
        id="data-pesticides"
        number="1.2"
        title="Pesticidindberetninger (SJI)"
      />
      <p>
        Landbrugsvirksomheder indberetter deres pesticidanvendelse til
        Milj&oslash;styrelsens Spr&oslash;jtejournalindberetning (SJI). Data
        opsummeres per CVR-nummer, afgr&oslash;dtype og behandlet areal.
        Indberetningen indeholder produktnavn, registreringsnummer, dosering
        (liter eller kg) og behandlet areal (ha) for det p&aring;g&aelig;ldende
        landbrugs&aring;r (august&ndash;juli). Bem&aelig;rk, at landmandens egen
        fysiske spr&oslash;jtejournal indeholder specifikke datoer og marknumre,
        men disse detaljer overf&oslash;res ikke til SJI-indberetningen. Det er
        netop dette tab af detaljegrad, der g&oslash;r vores fordelingsmetode
        n&oslash;dvendig (se afsnit 5.9). Bedrifter under 10 ha er fritaget for
        indberetningspligt.
      </p>

      <SubsectionHeader
        id="data-bmd"
        number="1.3"
        title="Bekæmpelsesmiddeldatabasen (BMD)"
      />
      <p>
        Milj&oslash;styrelsens BMD indeholder alle godkendte pesticider med
        tilh&oslash;rende aktivstoffer, godkendelsesstatus og
        tilbagetr&aelig;kningsdatoer. Disse data er afg&oslash;rende for
        lovlighedskontrollen (afsnit 4.1), hvor vi identificerer brugen af
        produkter efter deres officielle udl&oslash;bsdato.
      </p>

      <SubsectionHeader
        id="data-buildings"
        number="1.4"
        title="Bygnings- og Boligregistret (BBR)"
      />
      <p>
        Geodatastyrelsens bygningsregister bruges til at kortl&aelig;gge
        potentielle eksponeringer (afsnit 4.2). Vi identificerer boliger, skoler
        og institutioner inden for 100 meter af spr&oslash;jtede marker ved
        hj&aelig;lp af geografiske dataforesp&oslash;rgsler (
        <code>ST_DWithin</code>).
      </p>

      <DeepDive
        title="Vis tekniske detaljer: Filformater og datastier"
        testId="deep-dive-data-sources"
      >
        <p className="mb-2">
          Alle data lagres i Apache Parquet-format på Google Cloud Storage:
        </p>
        <ul className="list-disc space-y-2 pl-5 font-mono text-[14px]">
          <li>
            FVM marker:{' '}
            <code>gs://&hellip;/silver/fvm_marker_YYYY/data.parquet</code>
          </li>
          <li>
            Pesticider:{' '}
            <code>
              gs://&hellip;/silver/pesticides/pesticiddata_YYYY_YYYY+1.parquet
            </code>
          </li>
          <li>
            BMD: Skrabet fra <code>bmd.mst.dk</code>, konverteret til Parquet
          </li>
          <li>
            Bygninger:{' '}
            <code>gs://&hellip;/silver/bbr_buildings/data.parquet</code>
          </li>
        </ul>
        <p className="mt-2">
          Filudvælgelse sker via modificeringstidsstempler for at sikre
          konsistent versionering.
        </p>
      </DeepDive>
    </section>
  );
}
