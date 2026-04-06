'use client';

import { DeepDive } from '@/components/methodology/deep-dive';

export function DisaggregationValidation() {
  return (
    <DeepDive
      title="Vis valideringsresultater: dækning over tid"
      testId="deep-dive-validation"
    >
      <p>
        <strong>Dækning ved 2&nbsp;% tolerance (strategi 1+2):</strong>
      </p>
      <p className="border-primary/20 bg-primary/[0.03] text-foreground/80 my-4 rounded-md border px-5 py-4 font-mono text-[14px] leading-relaxed">
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
        0&ndash;0,2&nbsp;procentpoint ved 2&nbsp;% tolerance &mdash; det primære
        bidrag er fra strategi&nbsp;1.
      </p>
      <p className="mt-2">
        <strong>Tolerancefølsomhed:</strong> Springet fra 0&nbsp;% til
        0,5&nbsp;% tolerance er den største enkeltstigning hvert år
        (37&ndash;48&nbsp;% &rarr; 72&ndash;87&nbsp;%), hvilket bekræfter, at
        afrunding i arealindberetningen er den dominerende årsag til afvigelse.
        Over 2&nbsp;% aftager gevinsten markant &mdash; 10&nbsp;% tolerance
        giver kun ~3&ndash;4 procentpoint ekstra dækning, mens antallet af
        tvetydige koblinger tredobles. Ved 2&nbsp;% tolerance har 6,5&nbsp;% af
        koblede poster kryds-afgrøde-tvetydighed (dvs. de kunne matche flere
        afgrødegrupper under samme CVR); ved 10&nbsp;% stiger dette til
        27,6&nbsp;%.
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
        tildeles på ny hvert år og ikke identificerer samme bedrift på tværs af
        år (0&nbsp;% CVR-overensstemmelse mellem FVM 2016 og 2017 for delte
        numeriske dele). Løsning kræver, at FVM 2015-sølvdata genbehandles med
        CVR udfyldt. Årene 2010&ndash;2013 har 55&ndash;67&nbsp;% dækning
        grundet lav FVM-registreringskomplethed i den tidlige periode.
      </p>
      <p className="text-muted-foreground mt-2 text-[13px]">
        Valideringen er reproducerbar via{' '}
        <code>backend/scripts/validate_disaggregation_robustness.py</code>.
      </p>
    </DeepDive>
  );
}
