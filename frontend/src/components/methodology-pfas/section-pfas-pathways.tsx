import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionPfasPathways() {
  return (
    <section data-testid="section-pfas-pathways">
      <SectionHeader
        id="pfas-pathways"
        number="4"
        title="Fire PFAS-veje til grundvandet"
      />

      <p className="mb-4">
        Landbruget og det omgivende samfund bidrager prim&aelig;rt til PFAS i
        grundvandet via fire mekanismer med meget forskellige tidshorisonter:
      </p>

      <ol className="list-decimal space-y-3 pl-6">
        <li>
          <strong>Fluorholdige pesticider (TFA):</strong> Aktivstofferne
          nedbrydes i milj&oslash;et til TFA. Dette er hoved&aring;rsagen til,
          at TFA findes overalt. Det kan n&aring; grundvandet inden for f&aring;
          &aring;r.
        </li>
        <li>
          <strong>Spildevandsslam:</strong> Slam, der spredes som g&oslash;dning
          p&aring; markerne, kan indeholde PFAS (bl.a. PFOS og PFOA), som
          langsomt vaskes ned i grundvandet over &aring;rtier.
        </li>
        <li>
          <strong>Hj&aelig;lpestoffer i spr&oslash;jtemidler:</strong> Visse
          pesticidblandinger indeholder fluorholdige hj&aelig;lpestoffer
          (overfladeaktive stoffer), som kan introducere traditionelle PFAS,
          uafh&aelig;ngigt af selve aktivstoffet.
        </li>
        <li>
          <strong>Atmosf&aelig;risk nedfald:</strong> Nedbrydning af
          fluorholdige k&oslash;lemidler (HFC&apos;er) i atmosf&aelig;ren danner
          TFA, som falder ned med regnen. Dette bidrager konstant til
          baggrundsniveauet af TFA, uafh&aelig;ngigt af det lokale landbrug.
        </li>
      </ol>
    </section>
  );
}
