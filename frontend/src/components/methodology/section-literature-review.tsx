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
        Geografisk fordeling af pesticiddata har f&aring;et voksende
        forskningsmæssig opmærksomhed i det seneste &aring;rti, drevet af
        behovet for at forst&aring; den lokale eksponering og
        milj&oslash;p&aring;virkning. Den opn&aring;ede pr&aelig;cision varierer
        betydeligt mellem de forskellige studier og afh&aelig;nger fundamentalt
        af landenes datainfrastruktur.
      </p>

      <SubsectionHeader
        id="prior-work"
        number="2.1"
        title="Eksisterende tilgange"
      />

      <p>
        P&aring; global skala anvendes typisk en statistisk nedskalering af
        nationale salgsdata. PEST-CHEMGRIDS-datas&aelig;ttet
        <Sidenote number={1}>Maggi et al. (2019)</Sidenote> estimerer for
        eksempel forbruget af 20 aktivstoffer i et 10&nbsp;km-gitter ved at
        kombinere FAO-salgsstatistikker med data for afgr&oslash;defordeling. En
        nyere EU-model
        <Sidenote number={2}>Maggi et al. (2025)</Sidenote> opn&aring;r en
        opl&oslash;sning p&aring; 250 meter ved at kalibrere globale estimater
        med EUROSTAT-data, men den forbliver teoretisk &mdash; det faktiske
        forbrug kendes ikke.
      </p>
      <p>
        P&aring; regionalt niveau har forskere estimeret pesticidudledninger via
        statistisk fremskrivning
        <Sidenote number={6}>Udias et al. (2023)</Sidenote>, mens andre har
        opn&aring;et en gitteropl&oslash;sning p&aring; 100 meter i Belgien ved
        at kombinere afgr&oslash;dekort med estimerede standarddoser
        <Sidenote number={5}>Habran et al. (2022)</Sidenote>. Ogs&aring; her
        anvendes modellerede doser frem for rapporterede data.
      </p>
      <p>
        Den fineste geografiske opl&oslash;sning i den publicerede forskning er
        hidtil opn&aring;et i Frankrig, hvor man har fordelt regionale salgsdata
        ud p&aring; individuelle markblokke
        <Sidenote number={3}>Martin et al. (2023)</Sidenote>
        <Sidenote number={4}>Galimberti et al. (2025)</Sidenote>. Denne tilgang
        bygger dog fortsat p&aring; salgstal og antager, at standarddoserne
        altid afspejler den faktiske praksis, hvilket introducerer en
        systematisk usikkerhed.
      </p>

      <SubsectionHeader
        id="lit-limitations"
        number="2.2"
        title="Generelle begrænsninger i litteraturen"
      />
      <p>
        Fire begr&aelig;nsninger g&aring;r igen i den eksisterende litteratur:
      </p>
      <ul className="list-disc space-y-3 pl-6">
        <li>
          <strong>Forskellen p&aring; salg og forbrug:</strong>{' '}
          St&oslash;rstedelen af studierne bruger salgsdata som rettesnor for
          forbruget. Det er usikkert, da ikke alt solgt pesticid bruges i det
          samme &aring;r eller i den samme region.
        </li>
        <li>
          <strong>Antagelsen om ensartet forbrug:</strong> Inden for en given
          afgr&oslash;dtype antages et ensartet forbrug, hvilket ignorerer
          variationer i jordtyper og lokalt skadedyrstryk.
        </li>
        <li>
          <strong>Mangel p&aring; kontrolm&aring;linger:</strong> Faktiske
          registreringer p&aring; markniveau er sj&aelig;ldent
          tilg&aelig;ngelige, og modellerne har store statistiske afvigelser.
        </li>
        <li>
          <strong>Databeskyttelse (GDPR):</strong> Data fra individuelle
          bedrifter er beskyttet i de fleste EU-lande, hvilket g&oslash;r det
          sv&aelig;rt at validere og offentligg&oslash;re kortl&aelig;gninger
          p&aring; markniveau.
        </li>
      </ul>

      <LiteratureNovelty />
    </section>
  );
}
