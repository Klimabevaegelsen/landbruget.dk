import {
  SectionHeader,
  SubsectionHeader,
} from '@/components/methodology/article-layout';

export function SectionStatisticalMethod() {
  return (
    <section data-testid="section-statistical-method">
      <SectionHeader id="statistics" number="4" title="Statistisk metode" />

      <p>
        For at undgå, at vi drager konklusioner på baggrund af statistiske
        tilfældigheder, skal alle data overleve en streng firetrins-validering:
      </p>

      <SubsectionHeader
        id="stats-pipeline"
        number="4.1"
        title="Valideringspipeline"
      />
      <ol className="my-4 list-decimal space-y-3 pl-6">
        <li>
          <strong className="text-foreground">
            FDR-korrektion (False Discovery Rate)
          </strong>{' '}
          – Filtrerer falske positiver fra ved hjælp af
          Benjamini–Hochberg-metoden.
        </li>
        <li>
          <strong className="text-foreground">Logistisk regression</strong> –
          Bekræfter sammenhængen via grundlæggende statistiske modeller.
        </li>
        <li>
          <strong className="text-foreground">Multivariat justering</strong> –
          Renser data for &ldquo;forstyrrende faktorer&rdquo; (confounders) som
          jordtype, boringens dybde og tætheden af overvågningsboringer i
          området.
        </li>
        <li>
          <strong className="text-foreground">Geografisk modellering</strong> –
          Justerer for såkaldt geografisk autokorrelation, så afsmitning fra
          nabooplande ikke forvrænger billedet.
        </li>
      </ol>

      <SubsectionHeader
        id="stats-controls"
        number="4.2"
        title="Negativ kontrol"
      />
      <p>
        Som en stresstest af modellen testede vi fem stoffer, som binder sig så
        hårdt til jorden, at de <em>ikke</em> burde kunne nå grundvandet.
        Resultatet var præcis som forventet:
      </p>
      <div className="border-border bg-card my-6 rounded border p-5">
        <p className="text-foreground text-sm font-semibold">
          Ingen af de fem stoffer viste nogen statistisk sammenhæng.
        </p>
        <p className="text-muted-foreground mt-2 text-sm">
          Modellen fanger altså kun reelle nedsivninger.
        </p>
      </div>
    </section>
  );
}
