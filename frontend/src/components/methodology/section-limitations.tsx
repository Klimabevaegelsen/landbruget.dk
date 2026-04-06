import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionLimitations() {
  return (
    <section data-testid="section-limitations">
      <SectionHeader
        id="limitations"
        number="5"
        title="Begrænsninger og datakvalitet"
      />

      <p>
        Enhver statistisk model opererer med antagelser og begr&aelig;nsninger.
        Her er de 14 v&aelig;sentligste fejlkilder:
      </p>

      <ol className="list-decimal space-y-4 pl-6">
        <li>
          <strong>Arealgr&aelig;nsen er empirisk bestemt:</strong> Tolerancen
          p&aring; &plusmn;2&nbsp;% er valgt ud fra erfaring, fordi den sikrer
          bedst mulig datad&aelig;kning uden at skabe &aring;benlyse
          fejlkoblinger.
        </li>
        <li>
          <strong>Ikke-koblede dataposter (~8&nbsp;%):</strong> Cirka 8&nbsp;%
          af indberetningerne kan ikke placeres p&aring; et kort, enten pga.
          mangelfulde data eller forkerte indberetninger.
        </li>
        <li>
          <strong>Antagelse om j&aelig;vn fordeling:</strong> Vi fordeler
          pesticidet ligeligt ud over marken, selvom landmanden i praksis
          m&aring;ske behandler hj&oslash;rner, kanter eller pletvise
          omr&aring;der anderledes.
        </li>
        <li>
          <strong>Tidsforskydning:</strong> Vi antager, at markgr&aelig;nserne i
          &aring;r 2 er de samme som i &aring;r 1. Det holder oftest stik, men
          der kan ske matrikul&aelig;re &aelig;ndringer i l&oslash;bet af de
          mellemliggende m&aring;neder.
        </li>
        <li>
          <strong>Fejl i &oslash;kologiregistrering:</strong> Hvis en landmand
          har sat kryds i &ldquo;&oslash;kologi&rdquo; ved en fejl, eller en
          mark er under oml&aelig;gning men registreret som konventionel, kan
          algoritmen overse den.
        </li>
        <li>
          <strong>Afstandsanalyse er ren geometri:</strong> Vores 100-meter
          buffere tager ikke h&oslash;jde for vindretning, dysetyper eller
          afgr&oslash;deh&oslash;jde. Det er en teoretisk afstand, ikke en
          spredningsmodel.
        </li>
        <li>
          <strong>Kr&aelig;ver unikke registre:</strong> Modellen bygger
          p&aring; Danmarks s&aelig;rlige integration af CVR-numre, digitale
          spr&oslash;jtejournaler og GIS-markkort, hvilket g&oslash;r den
          sv&aelig;r at overf&oslash;re direkte til andre lande.
        </li>
        <li>
          <strong>Sm&aring; landbrug (under 10 ha) mangler:</strong> Bedrifter
          under 10 hektar er fritaget for at indberette til SJI. Det skaber et
          &ldquo;sort hul&rdquo; i dataene &ndash; is&aelig;r for frugt- og
          gr&oslash;ntproducenter, der typisk har et h&oslash;jt forbrug pr.
          hektar.
        </li>
        <li>
          <strong>Systemet sletter detaljer:</strong> Landmanden ved
          pr&aelig;cis, hvilken mark han spr&oslash;jter, men loven kr&aelig;ver
          kun, at han sender et overordnet sammendrag ind per afgr&oslash;de.
          Vores kode er et fors&oslash;g p&aring; at genskabe denne tabte
          information.
        </li>
        <li>
          <strong>Behandlet areal kontra bruttoareal:</strong> Landmanden
          indberetter det reelt spr&oslash;jtede areal, mens myndighedernes
          markkort viser hele marken (inklusive l&aelig;hegn og hj&oslash;rner).
          Dette giver en indbygget sk&aelig;vhed i arealvurderingerne.
        </li>
        <li>
          <strong>Registreringsfejl:</strong> Hvis en landmand s&oslash;ger
          EU-st&oslash;tte til &ldquo;vinterhvede&rdquo;, men indberetter sit
          spr&oslash;jtemiddel under &ldquo;v&aring;rbyg&rdquo;, kan systemet
          ikke koble de to data sammen.
        </li>
        <li>
          <strong>Der s&aelig;lges mere, end der indberettes:</strong> Der er et
          kendt gab (p&aring; mindst 13&nbsp;%) mellem m&aelig;ngden af solgte
          pesticider i Danmark og m&aelig;ngden af indberettede pesticider.
          Vores data er derfor et underestimeret billede af virkeligheden.
        </li>
        <li>
          <strong>Vi kender ikke datoen:</strong> Vi ved ikke, hvorn&aring;r
          p&aring; &aring;ret spr&oslash;jtningen har fundet sted, kun at det er
          sket i l&oslash;bet af det p&aring;g&aelig;ldende landbrugs&aring;r.
        </li>
        <li>
          <strong>Komplekse virksomhedskonstruktioner:</strong> Nogle
          landm&aelig;nd spr&oslash;jter i &eacute;t selskabs navn, men ejer
          jorden i et andet selskabs navn. Denne jonglering med CVR-numre
          forvirrer datakoblingen markant.
        </li>
      </ol>
    </section>
  );
}
