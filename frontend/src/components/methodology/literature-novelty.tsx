import { SubsectionHeader } from '@/components/methodology/article-layout';

export function LiteratureNovelty() {
  return (
    <>
      <SubsectionHeader
        id="novelty"
        number="2.3"
        title="Hvad er nyt i denne tilgang"
      />

      <p>
        Den metode, der præsenteres her, adskiller sig fra den eksisterende
        litteratur på flere afgørende punkter.
      </p>

      <p>
        <strong>Faktiske forbrugsdata, ikke salgsdata.</strong> Hvor samtlige
        ovennævnte studier anvender salgs- eller omsætningsdata som tilnærmelse,
        bygger vores metode på virksomhedernes faktiske pesticidindberetninger
        &mdash; rapporteret per CVR-nummer, afgrødetype og behandlingsareal via
        det danske Sprøjtejournal-system, som har været obligatorisk for
        bedrifter over 10 ha siden 2010/11 [8].
      </p>

      <p>
        <strong>National dækning med individuel markopløsning.</strong> Metoden
        opnår en nedbrydning til markniveau for samtlige danske landbrugsmarker,
        der kan sammenkøres via CVR- og afgrødedata. Dette er en finere
        opløsning end de 250 m EU-kort [2] og de franske studier [3, 4], fordi
        fordelingen er forankret i faktisk indberettet forbrug frem for
        statistisk modellerede doser.
      </p>

      <p>
        <strong>Kvantificeret pålidelighedsscore.</strong> Hver fordeling
        tildeles en pålidelighedsscore baseret på areal-overensstemmelsen mellem
        indberetning og markdata (se afsnit 3.3). Denne transparens er sjælden i
        den eksisterende litteratur, hvor usikkerheden typisk kun vurderes
        overordnet.
      </p>

      <p>
        <strong>Fuld reproducerbarhed.</strong> Hele datapipelinen er
        open-source, og alle mellemresultater er tilgængelige for uafhængig
        verifikation. Den metodologiske transparens, der muliggøres af åben
        kildekode, adresserer et centralt kritikpunkt i den nyere litteratur om
        manglende adgang til pesticiddata i EU [10].
      </p>

      <p>
        Sammenfattende udfylder denne tilgang et hul i den eksisterende
        litteratur: Ingen publiceret undersøgelse har før kombineret
        obligatoriske forbrugsdata med nationale markgrænser for at producere
        validerede markniveau-estimater i national skala.
      </p>
    </>
  );
}
