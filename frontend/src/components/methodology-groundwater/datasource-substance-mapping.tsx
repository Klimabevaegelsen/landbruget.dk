import { SubsectionHeader } from '@/components/methodology/article-layout';

export function DatasourceSubstanceMapping() {
  return (
    <>
      <SubsectionHeader
        id="data-mapping"
        number="2.4"
        title="Stofkortl&aelig;gning"
      />
      <p>
        Pesticider overv&aring;ges og registreres under forskellige navne i
        henholdsvis GEUS&apos; grundvandsprogram og Milj&oslash;styrelsens
        spr&oslash;jtejournal (SJI). For at koble overv&aring;gningsdata med
        anvendelsesdata har vi opbygget en kortl&aelig;gning med 138
        stofkorrespondancer &ndash; 69 metabolit-til-moderstof, 66
        navnevarianter og 3 multi-variant-relationer &ndash; baseret p&aring;
        PPDB, GEUS-tekniske rapporter og Bek&aelig;mpelsesmiddeldatabasen (BMD).
      </p>
      <p>
        Tre eksempler illustrerer, hvorfor denne kortl&aelig;gning er
        afg&oslash;rende:
      </p>
      <ul className="my-4 list-disc space-y-2 pl-6">
        <li>
          <strong className="text-foreground">1,2,4-triazol</strong> dannes fra
          12 forskellige triazol-fungicider (propiconazol og tebuconazol
          udg&oslash;r ~70&nbsp;% af anvendelsesintensiteten).
        </li>
        <li>
          <strong className="text-foreground">4-chlor-2-methylphenol</strong>{' '}
          overv&aring;ges under dette navn, men er en metabolit af MCPA.
        </li>
        <li>
          <strong className="text-foreground">AMPA</strong> kr&aelig;ver
          eksplicit metabolit-til-moderstof-mapping til glyphosat.
        </li>
      </ul>
      <p>
        Uden denne infrastruktur ville korrelationen mellem metabolitter i
        grundvandet og moderstoffernes anvendelse p&aring; marker v&aelig;re
        usynlig &ndash; og de st&aelig;rkeste signaler i analysen ville blive
        overset.
      </p>
    </>
  );
}
