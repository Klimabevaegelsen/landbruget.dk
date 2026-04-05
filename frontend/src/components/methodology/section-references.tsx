import { SectionHeader } from '@/components/methodology/article-layout';
import { ReferenceList } from '@/components/methodology/reference-list';

export function SectionReferences() {
  return (
    <section data-testid="section-references">
      <SectionHeader id="references" number="6" title="Referencer" />

      <p>Nummererede referencer svarende til citatmarkeringer [N] i teksten.</p>

      <ReferenceList />

      <div className="text-muted-foreground border-primary/20 mt-8 border-t pt-6 text-[13px]">
        <p>
          Landbruget.dk er et offentligt transparensprojekt. Al kildekode og
          databehandlingslogik er tilgængelig for uafhængig verifikation.
          Metodologien opdateres løbende i takt med nye datakilder og forbedrede
          data-strategier.
        </p>
      </div>
    </section>
  );
}
