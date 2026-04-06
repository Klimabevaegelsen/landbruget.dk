import { SubsectionHeader } from '@/components/methodology/article-layout';

export function LiteratureNovelty() {
  return (
    <>
      <SubsectionHeader
        id="novelty"
        number="2.3"
        title="Hvad er nyt i denne tilgang?"
      />

      <p>
        Vores metode adskiller sig fra den eksisterende litteratur på flere
        afgørende punkter:
      </p>

      <ul className="list-disc space-y-3 pl-6">
        <li>
          <strong>Faktiske forbrugsdata:</strong> Vi bygger på virksomhedernes
          faktiske, indberettede forbrug (obligatorisk for landbrug over 10 ha i
          Danmark) frem for salgstal.
        </li>
        <li>
          <strong>National dækning med markopløsning:</strong> Metoden kobler
          data ned på markniveau for samtlige danske landbrugsmarker, hvilket er
          en langt finere og mere virkelighedsnær opløsning end de europæiske
          250-meter gitterkort.
        </li>
        <li>
          <strong>Kvantificeret pålidelighed:</strong> Hver fordeling tildeles
          en pålidelighedsscore baseret på, hvor godt indberetningen matcher
          markarealet. Denne gennemsigtighed er sjælden i branchen.
        </li>
        <li>
          <strong>Fuld reproducerbarhed:</strong> Koden er open-source, hvilket
          gør det muligt for uvildige parter at efterprøve resultaterne.
        </li>
      </ul>

      <p>
        Samlet set udfylder denne tilgang et markant hul i litteraturen:{' '}
        <mark>
          Ingen publicerede undersøgelser har tidligere kombineret obligatoriske
          forbrugsdata med nationale markgrænser for at levere validerede
          estimater på markniveau for et helt land.
        </mark>
      </p>
    </>
  );
}
