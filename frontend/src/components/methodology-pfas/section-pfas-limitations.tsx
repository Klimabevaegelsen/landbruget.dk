import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionPfasLimitations() {
  return (
    <section data-testid="section-pfas-limitations">
      <SectionHeader id="pfas-limitations" number="5" title="Begrænsninger" />

      <p className="mb-4">Vores analyse har en række væsentlige forbehold:</p>

      <ul className="list-disc space-y-3 pl-6">
        <li>
          <strong>Massiv blind vinkel i overvågningen:</strong> Da kun 36 % af
          oplandene undersøges for PFAS, og disse ikke er udvalgt tilfældigt,
          kan vi ikke uden videre overføre resultaterne til resten af landet.
        </li>
        <li>
          <strong>Korte tidsserier:</strong> Den systematiske overvågning af TFA
          startede først i 2020. Vi kan derfor ikke tegne langsigtede tendenser
          på baggrund af kun 4–5 års data.
        </li>
        <li>
          <strong>Svært at isolere kilderne:</strong> Vi kan ikke skelne
          matematisk mellem PFAS fra landbruget og PFAS fra f.eks. brandskum,
          industri eller regnvand. Den massive tilstedeværelse af TFA er højst
          sandsynligt et resultat af flere kilder på én gang.
        </li>
        <li>
          <strong>Kausalitet vs. korrelation:</strong> Eksemplet fra Skrydstrup
          viser, at tingene findes samme sted – ikke at der nødvendigvis er en
          direkte årsagssammenhæng mellem den specifikke mark og boringen.
        </li>
        <li>
          <strong>Geografisk klyngedannelse:</strong> Som nævnt optræder PFAS
          ofte i klynger, hvilket gør statistiske beregninger komplekse og
          kræver avanceret justering for at undgå kunstigt forstærkede
          resultater.
        </li>
      </ul>
    </section>
  );
}
