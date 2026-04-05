import { SectionHeader } from '@/components/methodology/article-layout';
import { FindingsCorrelations } from '@/components/methodology-groundwater/findings-correlations';
import { FindingsDoseResponse } from '@/components/methodology-groundwater/findings-dose-response';
import { FindingsMonitoringDensity } from '@/components/methodology-groundwater/findings-monitoring-density';
import { FindingsMetabolites } from '@/components/methodology-groundwater/findings-metabolites';
import { FindingsTemporalLag } from '@/components/methodology-groundwater/findings-temporal-lag';
import { FindingsLegacy } from '@/components/methodology-groundwater/findings-legacy';

export function SectionKeyFindings() {
  return (
    <section data-testid="section-key-findings">
      <SectionHeader id="findings" number="5" title="Resultater" />

      <p>
        Vi har gennemf&oslash;rt en korrelationsanalyse p&aring; tv&aelig;rs af
        5.826 grundvandsoplande, der sammenholder markniveauets
        pesticidanvendelsesintensitet (kg aktivstof/ha) med fund i 4,6 millioner
        pr&oslash;veniveau-pesticidanalyser fra GEUS Dataverse (VP4). Af disse
        kvalificerer 11 stoffer med b&aring;de tilstr&aelig;kkelig
        detektionshyppighed og anvendelsesdata via stofkortl&aelig;gning.
      </p>

      <FindingsCorrelations />
      <FindingsDoseResponse />
      <FindingsMonitoringDensity />
      <FindingsMetabolites />
      <FindingsTemporalLag />
      <FindingsLegacy />
    </section>
  );
}
