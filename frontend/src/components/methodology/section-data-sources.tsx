import {
  SectionHeader,
  SubsectionHeader,
} from '@/components/methodology/article-layout';
import { DeepDive } from '@/components/methodology/deep-dive';

export function SectionDataSources() {
  return (
    <section data-testid="section-data-sources">
      <SectionHeader id="data" number="1" title="Datakilder" />

      <p>
        Analysen bygger på tre primære datakilder fra de danske myndigheder,
        suppleret med bygningsdata til afstandsanalyser. Alle data hentes i
        maskinlæsbart format, kontrolleres og sammenkøres på markniveau.
      </p>

      <SubsectionHeader id="data-fvm" number="1.1" title="Markdata (FVM)" />
      <p>
        Landbrugsstyrelsens Fællesskema indeholder markblokke og afgrødedata for
        samtlige danske landbrugsmarker. Hvert datasæt er versioneret per
        kalenderår og indeholder markens areal (ha), afgrødekode, CVR-nummer og
        geografiske koordinater (EPSG:25832). Vi anvender markdata fra året{' '}
        <em>efter</em> pesticidanvendelsen (se afsnit 3.1 om tidsforskydning).
      </p>

      <SubsectionHeader
        id="data-pesticides"
        number="1.2"
        title="Pesticidindberetninger (SJI)"
      />
      <p>
        Landbrugsvirksomheder indberetter deres pesticidanvendelse til
        Miljøstyrelsens Sprøjtejournalindberetning (SJI). Data opsummeres per
        CVR-nummer, afgrødtype og behandlet areal. Indberetningen indeholder
        produktnavn, registreringsnummer, dosering (liter eller kg) og behandlet
        areal (ha) for det pågældende landbrugsår (august–juli). Årsvariablen
        angiver landbrugsårets første kalenderår: <code>2015</code> betyder
        derfor perioden 1. august 2015 til 31. juli 2016 — ikke kalenderåret
        2015 og heller ikke året for markdeklarationen. Bemærk, at landmandens
        egen fysiske sprøjtejournal indeholder specifikke datoer og marknumre,
        men disse detaljer overføres ikke til SJI-indberetningen. Det er netop
        dette tab af detaljegrad, der gør vores fordelingsmetode nødvendig (se
        afsnit 5.9). Bedrifter under 10 ha er fritaget for indberetningspligt.
      </p>

      <SubsectionHeader
        id="data-bmd"
        number="1.3"
        title="Bekæmpelsesmiddeldatabasen (BMD)"
      />
      <p>
        Miljøstyrelsens BMD indeholder alle godkendte pesticider med tilhørende
        aktivstoffer, godkendelsesstatus og tilbagetrækningsdatoer. Disse data
        er afgørende for lovlighedskontrollen (afsnit 4.1), hvor vi
        identificerer brugen af produkter efter deres officielle udløbsdato.
      </p>

      <SubsectionHeader
        id="data-buildings"
        number="1.4"
        title="Bygnings- og Boligregistret (BBR)"
      />
      <p>
        Geodatastyrelsens bygningsregister bruges til at kortlægge potentielle
        eksponeringer (afsnit 4.2). Vi identificerer boliger, skoler og
        institutioner inden for 100 meter af sprøjtede marker ved hjælp af
        geografiske dataforespørgsler (<code>ST_DWithin</code>).
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
