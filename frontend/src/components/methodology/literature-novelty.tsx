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
        Vores metode adskiller sig fra den eksisterende litteratur p&aring;
        flere afg&oslash;rende punkter:
      </p>

      <ul className="list-disc space-y-3 pl-6">
        <li>
          <strong>Faktiske forbrugsdata:</strong> Vi bygger p&aring;
          virksomhedernes faktiske, indberettede forbrug (obligatorisk for
          landbrug over 10 ha i Danmark) frem for salgstal.
        </li>
        <li>
          <strong>National d&aelig;kning med markopl&oslash;sning:</strong>{' '}
          Metoden kobler data ned p&aring; markniveau for samtlige danske
          landbrugsmarker, hvilket er en langt finere og mere
          virkelighedsn&aelig;r opl&oslash;sning end de europ&aelig;iske
          250-meter gitterkort.
        </li>
        <li>
          <strong>Kvantificeret p&aring;lidelighed:</strong> Hver fordeling
          tildeles en p&aring;lidelighedsscore baseret p&aring;, hvor godt
          indberetningen matcher markarealet. Denne gennemsigtighed er
          sj&aelig;lden i branchen.
        </li>
        <li>
          <strong>Fuld reproducerbarhed:</strong> Koden er open-source, hvilket
          g&oslash;r det muligt for uvildige parter at efterpr&oslash;ve
          resultaterne.
        </li>
      </ul>

      <p>
        Samlet set udfylder denne tilgang et markant hul i litteraturen:{' '}
        <mark>
          Ingen publicerede unders&oslash;gelser har tidligere kombineret
          obligatoriske forbrugsdata med nationale markgr&aelig;nser for at
          levere validerede estimater p&aring; markniveau for et helt land.
        </mark>
      </p>
    </>
  );
}
