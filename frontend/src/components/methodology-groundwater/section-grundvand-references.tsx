import { SectionHeader } from '@/components/methodology/article-layout';
import { GrundvandReferenceList } from '@/components/methodology-groundwater/grundvand-reference-list';

export function SectionGrundvandReferences() {
  return (
    <section data-testid="section-grundvand-references">
      <SectionHeader id="references" number="8" title="Referencer" />
      <GrundvandReferenceList />
    </section>
  );
}
