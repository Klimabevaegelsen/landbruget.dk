'use client';

import { DeepDive } from '@/components/methodology/deep-dive';

export function DisaggregationValidation() {
  return (
    <DeepDive
      title="Vis valideringsresultater: dækning over tid"
      testId="deep-dive-validation"
    >
      <p>
        <strong>Dækning ved 2 % tolerance (strategi 1+2):</strong>
      </p>
      <p className="border-primary/20 bg-primary/[0.03] text-foreground/80 my-4 rounded-md border px-5 py-4 font-mono text-[14px] leading-relaxed">
        2015: 81,8 %  |  2016: 87,2 %  |  2017: 86,9 %  |  2018: 90,5 %  | 
        2019: 91,6 %  |  2020: 92,7 %  |  2021: 92,1 %  |  2022: 91,2 %  | 
        2023: 90,0 %
      </p>
      <p>
        Dækningen forbedres frem mod 2020 grundet stigende
        registreringskomplethed i FVM. Grænsen på 92 % er nået fra 2020.
        Strategi 2 (ikke-økologiske marker) bidrager med 0–0,2 procentpoint ved
        2 % tolerance — det primære bidrag er fra strategi 1.
      </p>
      <p className="mt-2">
        <strong>Tolerancefølsomhed:</strong> Springet fra 0 % til 0,5 %
        tolerance er den største enkeltstigning hvert år (37–48 % &rarr;
        72–87 %), hvilket bekræfter, at afrunding i arealindberetningen er den
        dominerende årsag til afvigelse. Over 2 % aftager gevinsten markant —
        10 % tolerance giver kun ~3–4 procentpoint ekstra dækning, mens antallet
        af tvetydige koblinger tredobles. Ved 2 % tolerance har 6,5 % af koblede
        poster kryds-afgrøde-tvetydighed (dvs. de kunne matche flere
        afgrødegrupper under samme CVR); ved 10 % stiger dette til 27,6 %.
      </p>
      <p className="mt-2">
        <strong>Ikke-koblede poster (2021):</strong> Af 27.188 ukoblede poster
        skyldes 67 % for stor arealafvigelse (&gt; 2 %), 24 % at CVR-nummeret
        slet ikke findes i FVM, og 8 % at CVR findes men uden matchende
        afgrødekode.
      </p>
      <p className="mt-2">
        <strong>Dosisplausibilitet:</strong> Af 1.671.295 kontrollerbare
        fordelte poster (2021) overskred kun 289 (0,017 %) ti gange den
        produktspecifikke mediandosis. Den lave outlier-rate tyder på, at
        fordelingen ikke systematisk producerer usandsynlige tildelinger.
      </p>
      <p className="mt-2">
        <strong>Særlige år:</strong> 2014-data giver 0 % dækning ved alle
        toleranceniveauer — FVM 2015-datasættet i R2 mangler{' '}
        <code>cvr_number</code> for samtlige 741.882 marker (feltet er NULL),
        hvilket gør CVR+afgrøde-koblingen umulig. Afgrødekoderne er kompatible
        (begge anvender de standardiserede Fællesskema-koder). CVR-genfinding
        via journalnumre er undersøgt og afvist: journalnumre (format
        &ldquo;ÅÅ-XXXXXXX&rdquo;) er årsspecifikke ansøgnings-ID'er, der
        tildeles på ny hvert år og ikke identificerer samme bedrift på tværs af
        år (0 % CVR-overensstemmelse mellem FVM 2016 og 2017 for delte numeriske
        dele). Løsning kræver, at FVM 2015-sølvdata genbehandles med CVR
        udfyldt. Årene 2010–2013 har 55–67 % dækning grundet lav
        FVM-registreringskomplethed i den tidlige periode.
      </p>
      <p className="text-muted-foreground mt-2 text-[13px]">
        Valideringen er reproducerbar via{' '}
        <code>backend/scripts/validate_disaggregation_robustness.py</code>.
      </p>
    </DeepDive>
  );
}
