'use client';

import {
  SectionHeader,
  SubsectionHeader,
  Figure,
} from '@/components/methodology/article-layout';
import { AllocationExplorer } from '@/components/methodology/allocation-explorer';
import { DeepDive } from '@/components/methodology/deep-dive';
import { CodeBlock } from '@/components/methodology/code-block';

export function SectionDisaggregation() {
  return (
    <section data-testid="section-disaggregation">
      <SectionHeader id="disaggregation" number="3" title="Fordelingsmetode" />

      <p>
        Pesticidindberetninger rapporteres på virksomhedsniveau (CVR), men
        analysen kræver specifik markopløsning. Vi anvender en sekventiel
        sammenkøringsprocedure med fire strategier, der tilsammen opnår en
        dækning på &ge; 92 %.
      </p>

      <SubsectionHeader id="y-plus-1" number="3.1" title="Y+1-forskydningen" />
      <p>
        Et centralt designvalg er den tidsmæssige forskydning: Pesticiddata fra
        landbrugsår <em>Y</em> køres sammen med markgrænser fra kalenderår{' '}
        <em>Y+1</em>. Baggrunden er, at markblokke først fastlægges endeligt
        efter vækstsæsonens afslutning. Eksempelvis anvender vi 2022-markgrænser
        til at placere pesticidanvendelsen i landbrugsåret 2021/22
        (august&ndash;juli).
      </p>

      <SubsectionHeader
        id="strategies"
        number="3.2"
        title="Sammenkøringsstrategier"
      />
      <p>
        For hver kombination af CVR-nummer og afgrødekode beregnes det samlede
        markareal og sammenlignes med det indberettede behandlingsareal.
        Overensstemmelsen accepteres inden for en tolerance på &plusmn;2 %.
      </p>

      <p className="border-border bg-card text-foreground/80 my-4 rounded border px-4 py-3 font-mono text-[13px] leading-relaxed">
        |AppArea &minus; &Sigma;FieldArea| &frasl; AppArea &times; 100 &le; 2,0
        %
      </p>

      <p>
        <strong>
          Strategi 1 &ndash; Bedste overensstemmelse for blandet drift.
        </strong>{' '}
        For virksomheder, der har både økologiske og konventionelle marker for
        samme afgrøde, beregnes arealoverensstemmelsen ad to veje: (a) med alle
        marker og (b) kun med de konventionelle. Den beregning, der giver den
        laveste afvigelse inden for &plusmn;2&nbsp;%-tolerancen, vælges
        automatisk. Herved får hver bedrift den mest præcise fordeling,
        uafhængigt af om driften er blandet.
      </p>

      <p>
        <strong>Strategi 2 &ndash; Hovedareal-kobling.</strong> De resterende
        rent konventionelle CVR+afgrøde-grupper behandles med proportional
        fordeling af dosis:
      </p>

      <p className="border-border bg-card text-foreground/80 my-4 rounded border px-4 py-3 font-mono text-[13px] leading-relaxed">
        Dosis<sub>mark</sub> = Dosis<sub>total</sub> &times; (Areal
        <sub>mark</sub> &frasl; Areal<sub>CVR,afgrøde</sub>)
      </p>

      <p>
        <strong>Strategi 3 &ndash; Ikke-økologisk oprydning.</strong> Opsamler
        tilfælde, som strategi 2 ikke kunne håndtere, ved at ekskludere marker
        registreret som økologiske (<code>organic_farming = true</code>).
        Fungerer som et sikkerhedsnet for de resterende poster.
      </p>
      <p>
        <strong>Strategi 4 &ndash; Delvis markdækning.</strong> Håndterer
        tilfælde, hvor et CVR-nummer kun har én mark for en given afgrøde, og
        det behandlede areal er mindre end markens fulde areal. Markeres
        særskilt i data (<code>IsPartialFieldCoverage = true</code>).
      </p>

      <SubsectionHeader
        id="confidence"
        number="3.3"
        title="Pålidelighedsscore"
      />
      <p>
        Hver tildeling får en pålidelighedsscore baseret på forskellen mellem
        indberettet og beregnet areal:
      </p>
      <p className="border-border bg-card text-foreground/80 my-4 rounded border px-4 py-3 font-mono text-[13px] leading-relaxed">
        Score = max(0, 1 &minus; |&Delta;areal%| &frasl; tolerance%)
      </p>
      <p>
        Scoren spænder fra 0,0 til 1,0, hvor 1,0 indikerer perfekt
        overensstemmelse på arealet.
      </p>

      <Figure
        id="fig-allocation"
        number={2}
        caption="Interaktiv illustration af proportional fordeling. Justér dosis og markarealer for at se beregningen."
      >
        <AllocationExplorer />
      </Figure>

      <DeepDive
        title="Vis tekniske detaljer: SQL-koblingslogik"
        testId="deep-dive-disaggregation"
      >
        <CodeBlock
          title="Arealsammenkøring med bedste overensstemmelse (forenklet)"
          language="sql"
          code={`-- For blandet drift: beregn afvigelse ad begge veje
SELECT cvr, crop_code,
  ABS(p.AcreageSize - SUM(f.area_ha))
    / p.AcreageSize * 100          AS afvigelse_alle_pct,
  ABS(p.AcreageSize - SUM(
    CASE WHEN NOT f.organic THEN f.area_ha ELSE 0 END))
    / p.AcreageSize * 100          AS afvigelse_konv_pct
FROM fields f
JOIN pesticides p USING (cvr, crop_code)
GROUP BY cvr, crop_code
-- Vælg den beregning med lavest afvigelse ≤ 2 %`}
        />
        <p className="text-muted-foreground mt-2 text-[13px]">
          Hver registrering forsynes med koblingsmetode og pålidelighedsscore
          for at sikre fuld sporbarhed.
        </p>
      </DeepDive>
    </section>
  );
}
