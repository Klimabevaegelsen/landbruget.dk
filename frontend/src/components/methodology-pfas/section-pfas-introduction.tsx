import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionPfasIntroduction() {
  return (
    <section data-testid="section-pfas-introduction">
      <SectionHeader id="pfas-introduction" number="1" title="Introduktion" />

      <p>
        PFAS &ndash; per- og polyfluoralkylstoffer &ndash; kaldes
        &ldquo;evighedskemikalier&rdquo; fordi de er ekstremt persistente i
        milj&oslash;et. De mest kendte PFAS-kilder er brandslukningsskum og
        industrielle processer. Men en ofte overset kilde er{' '}
        <strong>fluorholdige pesticider</strong>, der nedbrydes til
        trifluoreddikesyre (TFA) i jord og grundvand.
      </p>

      <p>
        I Danmark er <strong>52 godkendte aktivstoffer</strong> fluorholdige,
        heraf <strong>35 der kan danne TFA</strong>. De bruges p&aring;
        titusindvis af marker hvert &aring;r. TFA er ekstremt mobilt i grundvand
        og detekteres i <strong>100&nbsp;%</strong> af de overv&aring;gede
        grundvandsoplande.
      </p>

      <p>
        Denne artikel f&oslash;lger k&aelig;den fra mark til boring &ndash; fra
        produktet der spr&oslash;jtes, til aktivstoffet det indeholder, til
        metabolitten der dukker op i grundvandet. Derefter zoomer vi ud og viser
        den <strong>blinde vinkel</strong>: 64&nbsp;% af Danmarks
        grundvandsoplande har aldrig f&aring;et m&aring;lt for PFAS.
      </p>
    </section>
  );
}
