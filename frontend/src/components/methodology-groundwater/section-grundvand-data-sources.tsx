import { SectionHeader } from '@/components/methodology/article-layout';
import { DatasourceGrukos } from '@/components/methodology-groundwater/datasource-grukos';
import { DatasourceBnbo } from '@/components/methodology-groundwater/datasource-bnbo';
import { DatasourceGeus } from '@/components/methodology-groundwater/datasource-geus';
import { DatasourceSubstanceMapping } from '@/components/methodology-groundwater/datasource-substance-mapping';

export function SectionGrundvandDataSources() {
  return (
    <section data-testid="section-grundvand-data-sources">
      <SectionHeader id="data" number="2" title="Datakilder" />

      <p>
        Analysen bygger p&aring; tre offentlige datas&aelig;t. Alle data hentes
        i maskinl&aelig;sbart format og renses gennem en standardiseret
        datapipeline.
      </p>

      <DatasourceGrukos />
      <DatasourceBnbo />
      <DatasourceGeus />
      <DatasourceSubstanceMapping />
    </section>
  );
}
