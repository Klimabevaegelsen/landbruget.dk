'use client';

import {
  SectionHeader,
  SubsectionHeader,
  Figure,
} from '@/components/methodology/article-layout';
import { AllocationExplorer } from '@/components/methodology/allocation-explorer';
import { DeepDive } from '@/components/methodology/deep-dive';
import { CodeBlock } from '@/components/methodology/code-block';
import { DisaggregationValidation } from '@/components/methodology/disaggregation-validation';
import { SectionCoverageAccuracy } from '@/components/methodology/section-coverage-accuracy';
import { DisaggregationScrolly } from '@/components/methodology/disaggregation-scrolly';

export function SectionDisaggregation() {
  return (
    <section data-testid="section-disaggregation">
      <SectionHeader id="disaggregation" number="3" title="Fordelingsmetode" />

      <p>
        Pesticidindberetninger rapporteres p&aring; virksomhedsniveau (CVR), men
        analysen kr&aelig;ver data p&aring; markniveau. Vi anvender en trinvis
        sammenkøring med fire strategier, der samlet set opn&aring;r en
        d&aelig;kning p&aring; mindst <mark>92 %</mark>.
      </p>

      <DisaggregationScrolly />

      <SubsectionHeader
        id="y-plus-1"
        number="3.1"
        title="Tidsforskydningen (&Aring;r+1)"
      />
      <p>
        Et centralt designvalg er vores tidsforskydning: Pesticiddata fra
        landbrugs&aring;r <em>Y</em> sammenkøres med markgr&aelig;nser fra
        kalender&aring;r <em>Y+1</em>. Grunden er, at de endelige markblokke
        f&oslash;rst fastl&aelig;gges <em>efter</em> v&aelig;ksts&aelig;sonen.
        Eksempelvis bruges markgr&aelig;nserne fra 2022 til at placere
        pesticider spr&oslash;jtet i landbrugs&aring;ret 2021/22. Uden denne
        forskydning falder d&aelig;kningen fra over 90&nbsp;% til blot
        6&ndash;9&nbsp;%.
      </p>

      <SubsectionHeader
        id="strategies"
        number="3.2"
        title="Sammenkøringsstrategier"
      />
      <p>
        For hver kombination af CVR-nummer og afgr&oslash;dekode sammenlignes
        markarealet med det indberettede areal. Vi accepterer et match, hvis
        forskellen ligger inden for en tolerance p&aring; &plusmn;2,0&nbsp;%:
      </p>

      <p className="border-primary/20 bg-primary/[0.03] text-foreground/80 my-6 rounded-md border px-5 py-4 font-mono text-[15px] leading-relaxed">
        |Indberettet areal &minus; &Sigma;Markareal| &frasl; Indberettet areal
        &times; 100 &le; 2,0 %
      </p>

      <p>Vi anvender fire strategier:</p>
      <ol className="list-decimal space-y-3 pl-6">
        <li>
          <strong>Bedste match (blandet drift):</strong> For bedrifter med
          b&aring;de &oslash;kologiske og konventionelle marker afpr&oslash;ver
          systemet kombinationer (med og uden &oslash;kologi) for at finde den
          laveste afvigelse.
        </li>
        <li>
          <strong>Proportional fordeling:</strong> For rent konventionelle
          bedrifter fordeles m&aelig;ngden ud fra markens andel af det samlede
          areal:
        </li>
      </ol>

      <p className="border-primary/20 bg-primary/[0.03] text-foreground/80 my-6 rounded-md border px-5 py-4 font-mono text-[15px] leading-relaxed">
        Dosis<sub>mark</sub> = Dosis<sub>total</sub> &times; (Areal
        <sub>mark</sub> &frasl; Areal<sub>CVR, afgr&oslash;de</sub>)
      </p>

      <ol className="list-decimal space-y-3 pl-6" start={3}>
        <li>
          <strong>Filtrering af &oslash;kologi:</strong> Et sikkerhedsnet for
          resterende poster, der tvinger systemet til at ignorere marker
          registreret som &oslash;kologiske (<code>organic_farming = true</code>
          ).
        </li>
        <li>
          <strong>Delvis markbehandling:</strong> H&aring;ndterer de
          tilf&aelig;lde, hvor bedriften kun har &eacute;n mark med
          afgr&oslash;den, men har indberettet et behandlet areal, der er{' '}
          <em>mindre</em> end markens samlede areal.
        </li>
      </ol>

      <SubsectionHeader
        id="confidence"
        number="3.3"
        title="Pålidelighedsscore"
      />
      <p>
        Hver fordeling udl&oslash;ser en score mellem 0,0 og 1,0 (hvor 1,0 er
        perfekt), baseret p&aring; formlen:
      </p>
      <p className="border-primary/20 bg-primary/[0.03] text-foreground/80 my-6 rounded-md border px-5 py-4 font-mono text-[15px] leading-relaxed">
        Score = max(0, 1 &minus; |&Delta;areal%| &frasl; tolerance%)
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

      <DisaggregationValidation />

      <SectionCoverageAccuracy />
    </section>
  );
}
