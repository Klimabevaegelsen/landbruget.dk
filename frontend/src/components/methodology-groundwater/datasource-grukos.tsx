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
        Milj&oslash;styrelsens datas&aelig;t over de omr&aring;der, kommunerne
        har udpeget til grundvandsbeskyttelse. Datas&aelig;ttet indeholder:
      </p>
      <ul className="my-4 list-disc space-y-2 pl-6">
        <li>
          <strong className="text-foreground">Indsatsomr&aring;der</strong>{' '}
          &ndash; omr&aring;der med vedtagne politiske indsatsplaner.
        </li>
        <li>
          <strong className="text-foreground">Indvindingsoplande</strong>{' '}
          &ndash; de hydrologiske oplande, der forsyner vandv&aelig;rkerne.
        </li>
      </ul>
      <p>
        Disse kategoriseres efter s&aring;rbarhed. Vi fokuserer is&aelig;r
        p&aring; <em>SFI</em> (Spr&oslash;jtemidelf&oslash;lsomme
        indvindingsomr&aring;der), da de markerer zoner med forh&oslash;jet
        risiko for, at pesticider siver ned.
      </p>
    </>
  );
}
