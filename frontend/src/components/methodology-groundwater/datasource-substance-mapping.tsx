import { SubsectionHeader } from '@/components/methodology/article-layout';

export function DatasourceSubstanceMapping() {
  return (
    <>
      <SubsectionHeader
        id="data-mapping"
        number="2.4"
        title="Stofkortlægning (Oversættelse af kemi)"
      />
      <p>
        Et pesticid skifter ofte &ldquo;identitet&rdquo;, når det lander i
        jorden. For at kunne koble landmandens sprøjtejournal med GEUS&apos;
        vandprøver har vi kortlagt 138 kemiske relationer. For eksempel:
      </p>
      <ul className="my-4 list-disc space-y-2 pl-6">
        <li>
          <strong className="text-foreground">1,2,4-triazol</strong> er et
          nedbrydningsprodukt fra 12 forskellige svampemidler.
        </li>
        <li>
          <strong className="text-foreground">AMPA</strong> er
          nedbrydningsproduktet af glyphosat (Roundup).
        </li>
      </ul>
      <p>
        Uden denne &ldquo;ordbog&rdquo; mellem moderstoffer og metabolitter
        ville de stærkeste sammenhænge i analysen forblive usynlige.
      </p>
    </>
  );
}
