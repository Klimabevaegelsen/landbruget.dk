import { SubsectionHeader } from '@/components/methodology/article-layout';

export function DatasourceGrukos() {
  return (
    <>
      <SubsectionHeader
        id="data-grukos"
        number="2.1"
        title="Grundvandskortlægning (GRUKOS)"
      />
      <p>
        Miljøstyrelsens datasæt over de områder, kommunerne har udpeget til
        grundvandsbeskyttelse. Datasættet indeholder:
      </p>
      <ul className="my-4 list-disc space-y-2 pl-6">
        <li>
          <strong className="text-foreground">Indsatsområder</strong> – områder
          med vedtagne politiske indsatsplaner.
        </li>
        <li>
          <strong className="text-foreground">Indvindingsoplande</strong> – de
          hydrologiske oplande, der forsyner vandværkerne.
        </li>
      </ul>
      <p>
        Disse kategoriseres efter sårbarhed. Vi fokuserer især på <em>SFI</em>{' '}
        (Sprøjtemidelfølsomme indvindingsområder), da de markerer zoner med
        forhøjet risiko for, at pesticider siver ned.
      </p>
    </>
  );
}
