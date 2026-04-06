import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionPfasIntroduction() {
  return (
    <section data-testid="section-pfas-introduction">
      <SectionHeader id="pfas-introduction" number="1" title="Introduktion" />

      <p>
        PFAS (per- og polyfluoralkylstoffer) kaldes ofte for
        &ldquo;evighedskemikalier&rdquo;, fordi de er ekstremt svære at nedbryde
        i naturen. De mest kendte kilder til PFAS er brandslukningsskum og
        industriel produktion. Men en ofte overset kilde er{' '}
        <strong>fluorholdige pesticider</strong>, der nedbrydes til stoffet
        trifluoreddikesyre (TFA) i jorden og grundvandet.
      </p>

      <p>
        I Danmark har vi{' '}
        <mark>
          <strong>52 godkendte aktivstoffer</strong>
        </mark>
        , der indeholder fluor. Af dem kan{' '}
        <mark>
          <strong>35 nedbrydes til TFA</strong>
        </mark>
        . De bruges på titusindvis af marker hvert år. TFA er ekstremt mobilt i
        grundvandet og detekteres i{' '}
        <mark>
          <strong>100 %</strong>
        </mark>{' '}
        af de grundvandsoplande, hvor man leder efter det.
      </p>

      <p>
        Denne artikel følger kæden fra mark til boring – fra det produkt, der
        sprøjtes med, til det aktivstof det indeholder, og endelig til det
        nedbrydningsprodukt, der dukker op i drikkevandet. Derefter zoomer vi ud
        og kigger på den store <strong>blinde vinkel</strong>:{' '}
        <mark>64 % af Danmarks grundvandsoplande</mark> har aldrig fået målt for
        PFAS.
      </p>
    </section>
  );
}
