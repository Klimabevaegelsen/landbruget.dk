import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionPfasLimitations() {
  return (
    <section data-testid="section-pfas-limitations">
      <SectionHeader
        id="pfas-limitations"
        number="5"
        title="Begr&aelig;nsninger"
      />

      <p className="mb-4">
        Vores analyse har en r&aelig;kke v&aelig;sentlige forbehold:
      </p>

      <ul className="list-disc space-y-3 pl-6">
        <li>
          <strong>Massiv blind vinkel i overv&aring;gningen:</strong> Da kun
          36&nbsp;% af oplandene unders&oslash;ges for PFAS, og disse ikke er
          udvalgt tilf&aelig;ldigt, kan vi ikke uden videre overf&oslash;re
          resultaterne til resten af landet.
        </li>
        <li>
          <strong>Korte tidsserier:</strong> Den systematiske overv&aring;gning
          af TFA startede f&oslash;rst i 2020. Vi kan derfor ikke tegne
          langsigtede tendenser p&aring; baggrund af kun 4&ndash;5 &aring;rs
          data.
        </li>
        <li>
          <strong>Sv&aelig;rt at isolere kilderne:</strong> Vi kan ikke skelne
          matematisk mellem PFAS fra landbruget og PFAS fra f.eks. brandskum,
          industri eller regnvand. Den massive tilstedev&aelig;relse af TFA er
          h&oslash;jst sandsynligt et resultat af flere kilder p&aring;
          &eacute;n gang.
        </li>
        <li>
          <strong>Kausalitet vs. korrelation:</strong> Eksemplet fra Skrydstrup
          viser, at tingene findes samme sted &ndash; ikke at der
          n&oslash;dvendigvis er en direkte &aring;rsagssammenh&aelig;ng mellem
          den specifikke mark og boringen.
        </li>
        <li>
          <strong>Geografisk klyngedannelse:</strong> Som n&aelig;vnt
          optr&aelig;der PFAS ofte i klynger, hvilket g&oslash;r statistiske
          beregninger komplekse og kr&aelig;ver avanceret justering for at
          undg&aring; kunstigt forst&aelig;rkede resultater.
        </li>
      </ul>
    </section>
  );
}
