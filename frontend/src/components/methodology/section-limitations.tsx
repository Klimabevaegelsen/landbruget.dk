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
        Enhver statistisk model opererer med antagelser og begrænsninger. Her er
        de 14 væsentligste fejlkilder:
      </p>

      <ol className="list-decimal space-y-4 pl-6">
        <li>
          <strong>Arealgrænsen er empirisk bestemt:</strong> Tolerancen på
          &plusmn;2 % er valgt ud fra erfaring, fordi den sikrer bedst mulig
          datadækning uden at skabe åbenlyse fejlkoblinger.
        </li>
        <li>
          <strong>Ikke-koblede dataposter (~8 %):</strong> Cirka 8 % af
          indberetningerne kan ikke placeres på et kort, enten pga. mangelfulde
          data eller forkerte indberetninger.
        </li>
        <li>
          <strong>Antagelse om jævn fordeling:</strong> Vi fordeler pesticidet
          ligeligt ud over marken, selvom landmanden i praksis måske behandler
          hjørner, kanter eller pletvise områder anderledes.
        </li>
        <li>
          <strong>Tidsforskydning:</strong> Vi antager, at markgrænserne i år 2
          er de samme som i år 1. Det holder oftest stik, men der kan ske
          matrikulære ændringer i løbet af de mellemliggende måneder.
        </li>
        <li>
          <strong>Fejl i økologiregistrering:</strong> Hvis en landmand har sat
          kryds i &ldquo;økologi&rdquo; ved en fejl, eller en mark er under
          omlægning men registreret som konventionel, kan algoritmen overse den.
        </li>
        <li>
          <strong>Afstandsanalyse er ren geometri:</strong> Vores 100-meter
          buffere tager ikke højde for vindretning, dysetyper eller
          afgrødehøjde. Det er en teoretisk afstand, ikke en spredningsmodel.
        </li>
        <li>
          <strong>Kræver unikke registre:</strong> Modellen bygger på Danmarks
          særlige integration af CVR-numre, digitale sprøjtejournaler og
          GIS-markkort, hvilket gør den svær at overføre direkte til andre
          lande.
        </li>
        <li>
          <strong>Små landbrug (under 10 ha) mangler:</strong> Bedrifter under
          10 hektar er fritaget for at indberette til SJI. Det skaber et
          &ldquo;sort hul&rdquo; i dataene – især for frugt- og
          grøntproducenter, der typisk har et højt forbrug pr. hektar.
        </li>
        <li>
          <strong>Systemet sletter detaljer:</strong> Landmanden ved præcis,
          hvilken mark han sprøjter, men loven kræver kun, at han sender et
          overordnet sammendrag ind per afgrøde. Vores kode er et forsøg på at
          genskabe denne tabte information.
        </li>
        <li>
          <strong>Behandlet areal kontra bruttoareal:</strong> Landmanden
          indberetter det reelt sprøjtede areal, mens myndighedernes markkort
          viser hele marken (inklusive læhegn og hjørner). Dette giver en
          indbygget skævhed i arealvurderingerne.
        </li>
        <li>
          <strong>Registreringsfejl:</strong> Hvis en landmand søger EU-støtte
          til &ldquo;vinterhvede&rdquo;, men indberetter sit sprøjtemiddel under
          &ldquo;vårbyg&rdquo;, kan systemet ikke koble de to data sammen.
        </li>
        <li>
          <strong>Der sælges mere, end der indberettes:</strong> Der er et kendt
          gab (på mindst 13 %) mellem mængden af solgte pesticider i Danmark og
          mængden af indberettede pesticider. Vores data er derfor et
          underestimeret billede af virkeligheden.
        </li>
        <li>
          <strong>Vi kender ikke datoen:</strong> Vi ved ikke, hvornår på året
          sprøjtningen har fundet sted, kun at det er sket i løbet af det
          pågældende landbrugsår.
        </li>
        <li>
          <strong>Komplekse virksomhedskonstruktioner:</strong> Nogle landmænd
          sprøjter i ét selskabs navn, men ejer jorden i et andet selskabs navn.
          Denne jonglering med CVR-numre forvirrer datakoblingen markant.
        </li>
      </ol>
    </section>
  );
}
