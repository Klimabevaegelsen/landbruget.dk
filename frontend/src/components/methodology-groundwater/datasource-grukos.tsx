import { SubsectionHeader } from '@/components/methodology/article-layout';

export function DatasourceGrukos() {
  return (
    <>
      <SubsectionHeader
        id="data-grukos"
        number="2.1"
        title="Grundvandskortl&aelig;gning (GRUKOS)"
      />
      <p>
        Milj&oslash;styrelsens grundvandskortl&aelig;gningsdatas&aelig;t
        (GRUKOS) indeholder de omr&aring;der, som kommunerne har udpeget
        p&aring; baggrund af den statslige grundvandskortl&aelig;gning. Data
        distribueres via WFS-tjenesten p&aring; MiljoGIS og indeholder to lag:
      </p>
      <ul className="my-4 list-disc space-y-2 pl-6">
        <li>
          <strong className="text-foreground">Indsatsomr&aring;der</strong>{' '}
          &ndash; omr&aring;der med vedtagne indsatsplaner for
          grundvandsbeskyttelse.
        </li>
        <li>
          <strong className="text-foreground">Indvindingsoplande</strong>{' '}
          &ndash; de hydrologiske oplande til almene vandforsyninger.
        </li>
      </ul>
      <p>
        Begge lag kategoriseres efter s&aring;rbarhedstype: NFI
        (nitratf&oslash;lsomme indvindingsomr&aring;der) og SFI
        (sprøjtemiddelf&oslash;lsomme indvindingsomr&aring;der). SFI-zonerne er
        s&aelig;rligt relevante for vores analyse, da de markerer omr&aring;der
        med forh&oslash;jet risiko for pesticidnedsivning til grundvandet.
      </p>
    </>
  );
}
