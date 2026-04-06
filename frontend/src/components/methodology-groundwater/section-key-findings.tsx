import { SectionHeader } from '@/components/methodology/article-layout';
import { FindingsCorrelations } from '@/components/methodology-groundwater/findings-correlations';
import { FindingsMetabolites } from '@/components/methodology-groundwater/findings-metabolites';
import { FindingsTemporalLag } from '@/components/methodology-groundwater/findings-temporal-lag';
import { FindingsLegacy } from '@/components/methodology-groundwater/findings-legacy';

export function SectionKeyFindings() {
  return (
    <section data-testid="section-key-findings">
      <SectionHeader id="findings" number="5" title="Resultater" />

      <p>
        Vi har analyseret <mark>5.826 grundvandsoplande</mark> for at se, om der
        er sammenhæng mellem pesticidforbruget (kg/ha) på markerne og fundene i
        de <mark>4,6 millioner vandprøver</mark>. <mark>11 stoffer</mark> var
        brugt og målt ofte nok til at indgå i analysen.
      </p>

      <FindingsCorrelations />
      <FindingsMetabolites />
      <FindingsTemporalLag />
      <FindingsLegacy />
    </section>
  );
}
