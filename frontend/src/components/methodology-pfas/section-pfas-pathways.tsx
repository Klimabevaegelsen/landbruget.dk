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
        Landbruget og det omgivende samfund bidrager primært til PFAS i
        grundvandet via fire mekanismer med meget forskellige tidshorisonter:
      </p>

      <ol className="list-decimal space-y-3 pl-6">
        <li>
          <strong>Fluorholdige pesticider (TFA):</strong> Aktivstofferne
          nedbrydes i miljøet til TFA. Dette er hovedårsagen til, at TFA findes
          overalt. Det kan nå grundvandet inden for få år.
        </li>
        <li>
          <strong>Spildevandsslam:</strong> Slam, der spredes som gødning på
          markerne, kan indeholde PFAS (bl.a. PFOS og PFOA), som langsomt vaskes
          ned i grundvandet over årtier.
        </li>
        <li>
          <strong>Hjælpestoffer i sprøjtemidler:</strong> Visse
          pesticidblandinger indeholder fluorholdige hjælpestoffer
          (overfladeaktive stoffer), som kan introducere traditionelle PFAS,
          uafhængigt af selve aktivstoffet.
        </li>
        <li>
          <strong>Atmosfærisk nedfald:</strong> Nedbrydning af fluorholdige
          kølemidler (HFC&apos;er) i atmosfæren danner TFA, som falder ned med
          regnen. Dette bidrager konstant til baggrundsniveauet af TFA,
          uafhængigt af det lokale landbrug.
        </li>
      </ol>
    </section>
  );
}
