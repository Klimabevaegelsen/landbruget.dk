import { SubsectionHeader } from '@/components/methodology/article-layout';

export function LimitationDetails() {
  return (
    <>
      <SubsectionHeader
        id="lim-uniform"
        number="5.3"
        title="Antagelse om ensartet fordeling på marken"
      />
      <p>
        Metoden smører pesticiddosis jævnt ud over hele markens areal. I
        virkeligheden behandles randområder, hjørner og beskyttelseszoner ofte
        anderledes end midten af marken. Denne antagelse er et standardvilkår i
        næsten al europæisk forskning [3, 4, 5], men det er vigtigt at huske,
        når man laver nøjagtige afstandsanalyser til naboer eller vandløb.
      </p>

      <SubsectionHeader
        id="lim-temporal"
        number="5.4"
        title="Tidsforskydning mellem år"
      />
      <p>
        Vi antager, at markernes grænser i år 2 kan bruges til at placere det
        pesticid, der blev brugt i år 1. Det holder stik i de fleste tilfælde.
        Det faktiske tidsinterval er dog op til 9 måneder. I denne periode kan
        en landmand have solgt jord, lagt marker sammen eller delt dem op.
        Omfanget af sådanne matrikulære ændringer er ikke kortlagt systematisk i
        denne analyse.
      </p>

      <SubsectionHeader
        id="lim-organic"
        number="5.5"
        title="Fejl i økologisk status"
      />
      <p>
        Strategi 2 (som håndterer uoverensstemmelser i arealer) stoler blindt
        på, at landmanden har sat det korrekte &ldquo;økologi&rdquo;-kryds i
        Landbrugsstyrelsens system. En konventionel mark, der ved en fejl
        registreres som økologisk, vil få vores kode til at overse den. Marker
        under omlægning til økologi kan desuden stå registreret som
        konventionelle, selvom landmanden reelt er holdt op med at sprøjte dem,
        hvilket skaber yderligere støj i arealberegningerne.
      </p>

      <SubsectionHeader
        id="lim-proximity"
        number="5.6"
        title="Nærhedsanalyse er ren geometri"
      />
      <p>
        Den 100-meter grænse, vi anvender til at identificere naboer, er en
        skrivebordsafstand &ndash; ikke en fysisk spredningsmodel. I
        virkeligheden afhænger pesticidafdrift af vind, temperatur, sprøjtedyser
        og afgrødens højde. Avancerede it-modeller kræver vejrdata og tekniske
        detaljer om sprøjten, som simpelthen ikke findes i de danske
        indberetninger. Resultaterne er derfor en rumlig kortlægning, ikke en
        formel risikovurdering.
      </p>

      <SubsectionHeader
        id="lim-generalizability"
        number="5.7"
        title="Metoden kræver unikke danske registre"
      />
      <p>
        Modellen drager fordel af Danmarks unikke infrastruktur: Den
        obligatoriske elektroniske sprøjtejournal (SJI) og de digitale markkort.
        Da denne kombination sjældent findes i resten af EU [10], kan vores kode
        ikke umiddelbart kopieres til udlandet uden omfattende modifikationer.
      </p>

      <SubsectionHeader
        id="lim-no-dates"
        number="5.13"
        title="Vi kender ikke ugedagen"
      />
      <p>
        Indberetningerne opsummerer et helt år ad gangen. Vi ved ikke, om der er
        sprøjtet en tirsdag formiddag i maj eller en fredag i september. Derfor
        kan vi ikke advare naboer eller skoler om bestemte perioder, og vi kan
        kun kontrollere, om et forbudt sprøjtemiddel er brugt &ldquo;i løbet af
        året&rdquo;, ikke om det præcis skete før eller efter datoen for
        forbuddet.
      </p>

      <SubsectionHeader
        id="lim-cvr"
        number="5.14"
        title="Virksomhedskonstruktioner forvirrer koden"
      />
      <p>
        Danske landbrug gennemgår løbende fusioner og generationsskifter. En
        landmand kan sprøjte jorden under sit driftsselskab (ét CVR-nummer), men
        eje jorden under sit holdingselskab (et andet CVR-nummer). Denne
        juridiske leg med CVR-numre forvirrer systemet og er en hovedårsag til
        de data, vi ikke kan placere på kortet.
      </p>
    </>
  );
}
