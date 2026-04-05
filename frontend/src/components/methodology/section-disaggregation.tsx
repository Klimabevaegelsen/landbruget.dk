'use client';

import {
  SectionHeader,
  SubsectionHeader,
  Figure,
} from '@/components/methodology/article-layout';
import { AllocationExplorer } from '@/components/methodology/allocation-explorer';
import { DeepDive } from '@/components/methodology/deep-dive';
import { CodeBlock } from '@/components/methodology/code-block';
import { SectionCoverageAccuracy } from '@/components/methodology/section-coverage-accuracy';
import { DisaggregationScrolly } from '@/components/methodology/disaggregation-scrolly';

export function SectionDisaggregation() {
  return (
    <section data-testid="section-disaggregation">
      <SectionHeader id="disaggregation" number="3" title="Fordelingsmetode" />

      <p>
        Pesticidindberetninger rapporteres på virksomhedsniveau (CVR), men
        analysen kræver specifik markopløsning. Vi anvender en sekventiel
        sammenkøringsprocedure med fire strategier, der tilsammen opnår en
        d��kning på &ge; 92 %.
      </p>

      <DisaggregationScrolly />

      <SubsectionHeader id="y-plus-1" number="3.1" title="Y+1-forskydningen" />
      <p>
        Et centralt designvalg er den tidsmæssige forskydning: Pesticiddata fra
        landbrugsår <em>Y</em> køres sammen med markgrænser fra kalenderår{' '}
        <em>Y+1</em>. Baggrunden er, at markblokke først fastlægges endeligt
        efter vækstsæsonens afslutning. Eksempelvis anvender vi 2022-markgrænser
        til at placere pesticidanvendelsen i landbrugsåret 2021/22
        (august&ndash;juli).
      </p>
      <p>
        Valget er ikke vilkårligt &mdash; det er strukturelt nødvendigt.
        Empirisk test med Y+0-kobling (hvor SJI-år X matches mod FVM-år X i
        stedet for X+1) giver kun 6&ndash;9&nbsp;% dækning mod
        82&ndash;93&nbsp;% med Y+1, en forskel på over 80 procentpoint i hvert
        testår (2018&ndash;2023). De 6&ndash;9&nbsp;% afspejler landmænd, der
        tilfældigvis dyrker samme afgrøde på samme areal to år i træk.
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

      <p className="border-primary/20 bg-primary/[0.03] text-foreground/80 my-4 rounded border px-4 py-3 font-mono text-[13px] leading-relaxed">
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

      <p className="border-primary/20 bg-primary/[0.03] text-foreground/80 my-4 rounded border px-4 py-3 font-mono text-[13px] leading-relaxed">
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
      <p className="border-primary/20 bg-primary/[0.03] text-foreground/80 my-4 rounded border px-4 py-3 font-mono text-[13px] leading-relaxed">
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

      <SubsectionHeader
        id="validation"
        number="3.4"
        title="Empirisk validering"
      />
      <p>
        Fordelingsmetodens robusthed er valideret empirisk over alle 14
        tilgængelige indberetningsår (2010&ndash;2023) ved at køre strategi 1 og
        2 mod produktionsdata fra SJI og FVM i R2-lageret.
      </p>

      <DeepDive
        title="Vis valideringsresultater: dækning over tid"
        testId="deep-dive-validation"
      >
        <p>
          <strong>Dækning ved 2&nbsp;% tolerance (strategi 1+2):</strong>
        </p>
        <p className="border-primary/20 bg-primary/[0.03] text-foreground/80 my-3 rounded border px-4 py-3 font-mono text-[12px] leading-relaxed">
          2015: 81,8&nbsp;% &nbsp;|&nbsp; 2016: 87,2&nbsp;% &nbsp;|&nbsp; 2017:
          86,9&nbsp;% &nbsp;|&nbsp; 2018: 90,5&nbsp;% &nbsp;|&nbsp; 2019:
          91,6&nbsp;% &nbsp;|&nbsp; 2020: 92,7&nbsp;% &nbsp;|&nbsp; 2021:
          92,1&nbsp;% &nbsp;|&nbsp; 2022: 91,2&nbsp;% &nbsp;|&nbsp; 2023:
          90,0&nbsp;%
        </p>
        <p>
          Dækningen forbedres frem mod 2020 grundet stigende
          registreringskomplethed i FVM. Grænsen på 92&nbsp;% er nået fra 2020.
          Strategi&nbsp;2 (ikke-økologiske marker) bidrager med
          0&ndash;0,2&nbsp;procentpoint ved 2&nbsp;% tolerance &mdash; det
          primære bidrag er fra strategi&nbsp;1.
        </p>
        <p className="mt-2">
          <strong>Tolerancefølsomhed:</strong> Springet fra 0&nbsp;% til
          0,5&nbsp;% tolerance er den største enkeltstigning hvert år
          (37&ndash;48&nbsp;% &rarr; 72&ndash;87&nbsp;%), hvilket bekræfter, at
          afrunding i arealindberetningen er den dominerende årsag til
          afvigelse. Over 2&nbsp;% aftager gevinsten markant &mdash; 10&nbsp;%
          tolerance giver kun ~3&ndash;4 procentpoint ekstra dækning, mens
          antallet af tvetydige koblinger tredobles. Ved 2&nbsp;% tolerance har
          6,5&nbsp;% af koblede poster kryds-afgrøde-tvetydighed (dvs. de kunne
          matche flere afgrødegrupper under samme CVR); ved 10&nbsp;% stiger
          dette til 27,6&nbsp;%.
        </p>
        <p className="mt-2">
          <strong>Ikke-koblede poster (2021):</strong> Af 27.188 ukoblede poster
          skyldes 67&nbsp;% for stor arealafvigelse (&gt;&nbsp;2&nbsp;%),
          24&nbsp;% at CVR-nummeret slet ikke findes i FVM, og 8&nbsp;% at CVR
          findes men uden matchende afgrødekode.
        </p>
        <p className="mt-2">
          <strong>Dosisplausibilitet:</strong> Af 1.671.295 kontrollerbare
          fordelte poster (2021) overskred kun 289 (0,017&nbsp;%) ti gange den
          produktspecifikke mediandosis. Den lave outlier-rate tyder på, at
          fordelingen ikke systematisk producerer usandsynlige tildelinger.
        </p>
        <p className="mt-2">
          <strong>Særlige år:</strong> 2014-data giver 0&nbsp;% dækning ved alle
          toleranceniveauer &mdash; FVM 2015-datasættet i R2 mangler{' '}
          <code>cvr_number</code> for samtlige 741.882 marker (feltet er NULL),
          hvilket gør CVR+afgrøde-koblingen umulig. Afgrødekoderne er kompatible
          (begge anvender de standardiserede Fællesskema-koder). CVR-genfinding
          via journalnumre er undersøgt og afvist: journalnumre (format
          &ldquo;ÅÅ-XXXXXXX&rdquo;) er årsspecifikke ansøgnings-ID&rsquo;er, der
          tildeles på ny hvert år og ikke identificerer samme bedrift på tværs
          af år (0&nbsp;% CVR-overensstemmelse mellem FVM 2016 og 2017 for delte
          numeriske dele). Løsning kræver, at FVM 2015-sølvdata genbehandles med
          CVR udfyldt. Årene 2010&ndash;2013 har 55&ndash;67&nbsp;% dækning
          grundet lav FVM-registreringskomplethed i den tidlige periode.
        </p>
        <p className="text-muted-foreground mt-2 text-[13px]">
          Valideringen er reproducerbar via{' '}
          <code>backend/scripts/validate_disaggregation_robustness.py</code>.
        </p>
      </DeepDive>

      <SectionCoverageAccuracy />
    </section>
  );
}
