import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionPfasIntroduction() {
  return (
    <section data-testid="section-pfas-introduction">
      <SectionHeader id="pfas-introduction" number="1" title="Introduktion" />

      <p>
        PFAS (per- og polyfluoralkylstoffer) kaldes ofte for
        &ldquo;evighedskemikalier&rdquo;, fordi de er ekstremt sv&aelig;re at
        nedbryde i naturen. De mest kendte kilder til PFAS er brandslukningsskum
        og industriel produktion. Men en ofte overset kilde er{' '}
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
        . De bruges p&aring; titusindvis af marker hvert &aring;r. TFA er
        ekstremt mobilt i grundvandet og detekteres i{' '}
        <mark>
          <strong>100&nbsp;%</strong>
        </mark>{' '}
        af de grundvandsoplande, hvor man leder efter det.
      </p>

      <p>
        Denne artikel f&oslash;lger k&aelig;den fra mark til boring &ndash; fra
        det produkt, der spr&oslash;jtes med, til det aktivstof det indeholder,
        og endelig til det nedbrydningsprodukt, der dukker op i drikkevandet.
        Derefter zoomer vi ud og kigger p&aring; den store{' '}
        <strong>blinde vinkel</strong>:{' '}
        <mark>64&nbsp;% af Danmarks grundvandsoplande</mark> har aldrig
        f&aring;et m&aring;lt for PFAS.
      </p>
    </section>
  );
}
