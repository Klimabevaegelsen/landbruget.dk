import {
  SectionHeader,
  SubsectionHeader,
} from '@/components/methodology/article-layout';

export function SectionStatisticalMethod() {
  return (
    <section data-testid="section-statistical-method">
      <SectionHeader id="statistics" number="4" title="Statistisk metode" />

      <p>
        For at undg&aring;, at vi drager konklusioner p&aring; baggrund af
        statistiske tilf&aelig;ldigheder, skal alle data overleve en streng
        firetrins-validering:
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
          &ndash; Filtrerer falske positiver fra ved hj&aelig;lp af
          Benjamini&ndash;Hochberg-metoden.
        </li>
        <li>
          <strong className="text-foreground">Logistisk regression</strong>{' '}
          &ndash; Bekr&aelig;fter sammenh&aelig;ngen via grundl&aelig;ggende
          statistiske modeller.
        </li>
        <li>
          <strong className="text-foreground">Multivariat justering</strong>{' '}
          &ndash; Renser data for &ldquo;forstyrrende faktorer&rdquo;
          (confounders) som jordtype, boringens dybde og t&aelig;theden af
          overv&aring;gningsboringer i omr&aring;det.
        </li>
        <li>
          <strong className="text-foreground">Geografisk modellering</strong>{' '}
          &ndash; Justerer for s&aring;kaldt geografisk autokorrelation,
          s&aring; afsmitning fra nabooplande ikke forvr&aelig;nger billedet.
        </li>
      </ol>

      <SubsectionHeader
        id="stats-controls"
        number="4.2"
        title="Negativ kontrol"
      />
      <p>
        Som en stresstest af modellen testede vi fem stoffer, som binder sig
        s&aring; h&aring;rdt til jorden, at de <em>ikke</em> burde kunne
        n&aring; grundvandet. Resultatet var pr&aelig;cis som forventet:
      </p>
      <div className="border-border bg-card my-6 rounded border p-5">
        <p className="text-foreground text-sm font-semibold">
          Ingen af de fem stoffer viste nogen statistisk sammenh&aelig;ng.
        </p>
        <p className="text-muted-foreground mt-2 text-sm">
          Modellen fanger alts&aring; kun reelle nedsivninger.
        </p>
      </div>
    </section>
  );
}
