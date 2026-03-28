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
        Analysen bygger på tre primære datakilder fra den danske
        statsforvaltning, suppleret med bygningsdata til nærhedsanalyse. Alle
        data hentes i maskinlæsbart format og processeres gennem en
        tretrins-forløb (Figur 1).
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
        Landbrugsstyrelsens Fællesskema indeholder markblokke og afgrødedata for
        samtlige danske landbrugsmarker. Hvert datasæt er versioneret per
        kalenderår og indeholder markareal (ha), afgrødekode, CVR-nummer og
        geometri i EPSG:25832. Vi anvender markdata fra året <em>efter</em>{' '}
        pesticidanvendelsen (se afsnit 3.1, Y+1-forskydningen).
      </p>

      <SubsectionHeader
        id="data-pesticides"
        number="1.2"
        title="Pesticidindberetninger"
      />
      <p>
        Virksomheder indberetter pesticidanvendelse til
        Sprøjtejournalindberetningen (SJI) opsummeret per CVR-nummer,
        afgrødetype og behandlingsareal. Indberetningen indeholder produktnavn,
        registreringsnummer, dosering (L eller kg) og behandlet areal (ha). Data
        er tilgængelige per landbrugsår (august&ndash;juli). Bemærk, at
        landmandens egen sprøjtejournal indeholder detaljer om specifikke marker
        og datoer, som ikke overføres til SJI-indberetningen &ndash; det er
        dette tab af detaljegrad, der motiverer hele vores fordelingsmetodik (se
        afsnit 5.9). Bedrifter under 10 ha er fritaget for indberetningspligt
        (se afsnit 5.8).
      </p>

      <SubsectionHeader
        id="data-bmd"
        number="1.3"
        title="Bekæmpelsesmiddeldatabasen (BMD)"
      />
      <p>
        Miljøstyrelsens BMD indeholder alle godkendte pesticidprodukter med
        tilhørende aktive stoffer, godkendelsesstatus og tilbagetrækningsdatoer.
        Disse data er afgørende for lovlighedskontrollen (afsnit 4.1), hvor vi
        identificerer anvendelse af produkter efter deres officielle
        tilbagetrækningsfrist.
      </p>

      <SubsectionHeader
        id="data-buildings"
        number="1.4"
        title="Bygnings- og Boligregistret (BBR)"
      />
      <p>
        Geodatastyrelsens bygningsregister bruges til nærhedsanalysen (afsnit
        4.2). Vi identificerer boliger, skoler og institutioner inden for 100
        meter af pesticidbehandlede marker ved hjælp af rumlige forespørgsler (
        <code>ST_DWithin</code>).
      </p>

      <DeepDive
        title="Vis tekniske detaljer: Filformater og datastier"
        testId="deep-dive-data-sources"
      >
        <p className="mb-2">
          Alle data lagres i Apache Parquet-format på Google Cloud Storage:
        </p>
        <ul className="list-disc space-y-1 pl-5 font-mono text-[13px]">
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
