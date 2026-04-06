import {
  SectionHeader,
  Sidenote,
  SubsectionHeader,
} from '@/components/methodology/article-layout';
import { LiteratureNovelty } from '@/components/methodology/literature-novelty';

export function SectionLiteratureReview() {
  return (
    <section data-testid="section-literature-review">
      <SectionHeader
        id="literature"
        number="2"
        title="Tidligere forskning og videnskabelig kontekst"
      />

      <p>
        Geografisk fordeling af pesticiddata har fået voksende forskningsmæssig
        opmærksomhed i det seneste årti, drevet af behovet for at forstå den
        lokale eksponering og miljøpåvirkning. Den opnåede præcision varierer
        betydeligt mellem de forskellige studier og afhænger fundamentalt af
        landenes datainfrastruktur.
      </p>

      <SubsectionHeader
        id="prior-work"
        number="2.1"
        title="Eksisterende tilgange"
      />

      <p>
        På global skala anvendes typisk en statistisk nedskalering af nationale
        salgsdata. PEST-CHEMGRIDS-datasættet
        <Sidenote number={1}>Maggi et al. (2019)</Sidenote> estimerer for
        eksempel forbruget af 20 aktivstoffer i et 10 km-gitter ved at kombinere
        FAO-salgsstatistikker med data for afgrødefordeling. En nyere EU-model
        <Sidenote number={2}>Maggi et al. (2025)</Sidenote> opnår en opløsning
        på 250 meter ved at kalibrere globale estimater med EUROSTAT-data, men
        den forbliver teoretisk — det faktiske forbrug kendes ikke.
      </p>
      <p>
        På regionalt niveau har forskere estimeret pesticidudledninger via
        statistisk fremskrivning
        <Sidenote number={6}>Udias et al. (2023)</Sidenote>, mens andre har
        opnået en gitteropløsning på 100 meter i Belgien ved at kombinere
        afgrødekort med estimerede standarddoser
        <Sidenote number={5}>Habran et al. (2022)</Sidenote>. Også her anvendes
        modellerede doser frem for rapporterede data.
      </p>
      <p>
        Den fineste geografiske opløsning i den publicerede forskning er hidtil
        opnået i Frankrig, hvor man har fordelt regionale salgsdata ud på
        individuelle markblokke
        <Sidenote number={3}>Martin et al. (2023)</Sidenote>
        <Sidenote number={4}>Galimberti et al. (2025)</Sidenote>. Denne tilgang
        bygger dog fortsat på salgstal og antager, at standarddoserne altid
        afspejler den faktiske praksis, hvilket introducerer en systematisk
        usikkerhed.
      </p>

      <SubsectionHeader
        id="lit-limitations"
        number="2.2"
        title="Generelle begrænsninger i litteraturen"
      />
      <p>Fire begrænsninger går igen i den eksisterende litteratur:</p>
      <ul className="list-disc space-y-3 pl-6">
        <li>
          <strong>Forskellen på salg og forbrug:</strong> Størstedelen af
          studierne bruger salgsdata som rettesnor for forbruget. Det er
          usikkert, da ikke alt solgt pesticid bruges i det samme år eller i den
          samme region.
        </li>
        <li>
          <strong>Antagelsen om ensartet forbrug:</strong> Inden for en given
          afgrødtype antages et ensartet forbrug, hvilket ignorerer variationer
          i jordtyper og lokalt skadedyrstryk.
        </li>
        <li>
          <strong>Mangel på kontrolmålinger:</strong> Faktiske registreringer på
          markniveau er sjældent tilgængelige, og modellerne har store
          statistiske afvigelser.
        </li>
        <li>
          <strong>Databeskyttelse (GDPR):</strong> Data fra individuelle
          bedrifter er beskyttet i de fleste EU-lande, hvilket gør det svært at
          validere og offentliggøre kortlægninger på markniveau.
        </li>
      </ul>

      <LiteratureNovelty />
    </section>
  );
}
