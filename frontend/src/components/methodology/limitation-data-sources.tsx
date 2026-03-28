import { SubsectionHeader } from '@/components/methodology/article-layout';

export function LimitationDataSources() {
  return (
    <>
      <SubsectionHeader
        id="lim-small-farms"
        number="5.8"
        title="Små landbrug (under 10 ha) mangler"
      />
      <p>
        Kravet om at indberette sprøjtejournaler gælder kun for landbrug over 10
        hektar. Mindre bedrifter er et sort hul i vores data. Det drejer sig om
        cirka 8.000&ndash;10.000 bedrifter &ndash; ofte frugtplantager,
        grøntsagsavlere og juletræsproducenter, der typisk bruger relativt meget
        sprøjtegift per hektar. Denne indbyggede blinde vinkel i lovgivningen
        betyder, at det samlede kemiske pres systematisk undervurderes.
      </p>

      <SubsectionHeader
        id="lim-aggregation"
        number="5.9"
        title="SJI-systemet sletter detaljerne"
      />
      <p>
        Selvom landmanden ude på traktoren noterer præcis hvilken mark han
        sprøjter, kræver loven kun, at han sender et overordnet sammendrag ind
        til Miljøstyrelsen. Detaljerne forsvinder i bureaukratiet. Hele vores
        kodearbejde er i virkeligheden et forsøg på at genskabe denne tabte
        information ved at samle puslespillet af arealer igen. Det sætter et
        naturligt loft for, hvor præcise vi kan blive.
      </p>

      <SubsectionHeader
        id="lim-treated-area"
        number="5.10"
        title="Behandlet areal versus bruttoareal"
      />
      <p>
        Når landmanden indberetter, oplyser han det faktiske areal, han har kørt
        på. Men det officielle markkort viser markens samlede bruttoareal
        inklusive små træer og vådområder i kanten. Denne systematiske skævhed
        gør, at tallene sjældent stemmer 100 % overens.
      </p>

      <SubsectionHeader
        id="lim-crop-codes"
        number="5.11"
        title="Registreringsfejl på tværs af systemer"
      />
      <p>
        Hvis en landmand har anmeldt til EU-støtte, at han dyrker
        &ldquo;vinterhvede&rdquo;, men af gammel vane indberetter sit
        pesticidforbrug under &ldquo;vårbyg&rdquo;, vil vores system ikke kunne
        parre de to ting. Omfanget af disse tastefejl mellem myndighedernes
        it-systemer kendes ikke.
      </p>

      <SubsectionHeader
        id="lim-sales-gap"
        number="5.12"
        title="Der sælges mere, end der indberettes"
      />
      <p>
        Den nationale statistik viser et hul mellem det pesticid, der bliver
        solgt i Danmark, og det, landmændene indberetter, de har brugt (en
        forskel på mindst 13 %). Det kan skyldes landmænd, der hamstrer før en
        afgiftsstigning, manglende indberetning, eller at de små landbrug ikke
        skal indberette. Det betyder endnu engang, at vores kort viser et
        underestimeret billede af virkeligheden.
      </p>
    </>
  );
}
