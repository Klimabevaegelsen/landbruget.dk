import {
  SectionHeader,
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
        Rumlig fordeling af pesticiddata har været genstand for voksende
        forskningsmæssig interesse i det seneste årti, drevet af behovet for at
        forstå lokale eksponeringer og miljøpåvirkninger. Den opnåede rumlige
        opløsning varierer betydeligt mellem studier og afhænger fundamentalt af
        den tilgængelige datainfrastruktur.
      </p>

      <SubsectionHeader
        id="prior-work"
        number="2.1"
        title="Eksisterende tilgange"
      />

      <p>
        På global og kontinental skala anvendes typisk statistisk nedskalering
        fra nationale salgsdata. PEST-CHEMGRIDS-datasættet [1] estimerer
        anvendelsesrater for 20 aktive stoffer på et ~10 km gitter ved at
        kombinere FAO-salgsstatistikker med afgrødefordelingsdata. En nyere
        EU-dækkende udvidelse [2] opnår 250 m opløsning ved at kalibrere globale
        estimater med EUROSTAT-data og d&apos;Andrimonts 10 m afgrødekort, men
        forbliver modelbaseret &mdash; det faktiske forbrug kendes ikke.
      </p>
      <p>
        På regionalt niveau har Udias et al. [6] estimeret pesticidemissioner på
        NUTS-3-niveau for EU via statistisk fremskrivning, mens Habran et al.
        [5] opnåede 100 m gitteropløsning for Vallonien (Belgien) ved at
        kombinere afgrødekort med estimerede anvendelsesrater. Begge tilgange
        anvender modellerede &mdash; ikke rapporterede &mdash; doser.
      </p>
      <p>
        Den fineste rumlige opløsning i publiceret forskning er opnået i
        Frankrig. Martin et al. [3] fordelte regionale salgsdata til
        individuelle markblokke via postnummerbaserede salgstal og
        afgrødegodkendelser, valideret i to testområder (830 ha og 12.007 ha).
        Galimberti et al. [4] udvidede denne tilgang til 9,5 mio. franske
        markblokke og 388 aktive stoffer. Begge studier anvender dog salgsdata
        &mdash; ikke faktisk forbrug &mdash; og antager, at godkendte doser
        afspejler praksis, hvilket introducerer en systematisk usikkerhed.
      </p>
      <p>
        I Tyskland har SYNOPS-GIS-modellen [7] opnået ægte markniveau-opløsning
        for sukkerroer, men baseret på spørgeskemadata fra et begrænset antal
        bedrifter &mdash; ikke et nationalt indberetningssystem.
      </p>

      <SubsectionHeader
        id="lit-limitations"
        number="2.2"
        title="Generelle begrænsninger i litteraturen"
      />
      <p>Fire begrænsninger går igen i den eksisterende litteratur:</p>
      <ol className="list-decimal space-y-3 pl-6">
        <li>
          <em>Salgs- vs. anvendelsesgabet:</em> Størstedelen af studierne
          anvender salgs- eller omsætningsdata som tilnærmelse for faktisk
          anvendelse. Det introducerer en fundamental usikkerhed, da ikke alle
          solgte pesticider nødvendigvis anvendes i salgsåret eller -regionen
          [10].
        </li>
        <li>
          <em>Homogenitetsantagelsen:</em> Inden for en given afgrødetype
          antages et ensartet forbrug, hvilket ignorerer variation mellem
          bedrifter, jordtyper og lokalt skadedyrstryk.
        </li>
        <li>
          <em>Mangel på kontrolmålinger:</em> Faktiske registreringer på
          markniveau er sjældent tilgængelige, og selv de bedste modeller
          rapporterer betydelige statistiske afvigelser [5].
        </li>
        <li>
          <em>Databeskyttelse:</em> Individuelle bedriftsdata er i de fleste
          EU-lande beskyttet under GDPR, hvilket begrænser muligheden for at
          validere og offentliggøre estimater på markniveau.
        </li>
      </ol>

      <LiteratureNovelty />
    </section>
  );
}
