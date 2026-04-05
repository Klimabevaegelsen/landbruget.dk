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
        Analysen har flere v&aelig;sentlige begr&aelig;nsninger:
      </p>

      <ul className="list-disc space-y-3 pl-6">
        <li>
          <strong>Overv&aring;gningsbias</strong> &ndash; Kun 36&nbsp;% af
          oplandene har PFAS-m&aring;linger. De overv&aring;gede oplande er ikke
          tilf&aelig;ldigt udvalgt, s&aring; resultaterne kan ikke direkte
          generaliseres til de uoverv&aring;gede.
        </li>
        <li>
          <strong>Temporal begr&aelig;nsning</strong> &ndash; Systematisk
          TFA-overv&aring;gning startede f&oslash;rst i 2020. Vi kan ikke
          vurdere langsigtede trends p&aring; baggrund af 4&ndash;5 &aring;rs
          data.
        </li>
        <li>
          <strong>Ingen punktkildeadskillelse</strong> &ndash; Vi kan ikke
          adskille PFAS fra landbrugspesticider fra andre kilder (brandskum,
          industri, spildevand, deposition). TFA&apos;s universelle
          tilstedev&aelig;relse afspejler sandsynligvis flere kilder.
        </li>
        <li>
          <strong>Kausalitet vs. korrelation</strong> &ndash; De illustrative
          eksempler (mark &rarr; produkt &rarr; boring) viser rumlig
          n&aelig;rhed, ikke &aring;rsagssammenh&aelig;ng.
          Grundvandsstr&oslash;mme og multiple kilder g&oslash;r det umuligt at
          spore &eacute;t fund til &eacute;n mark.
        </li>
        <li>
          <strong>Rumlig autokorrelation</strong> &ndash; PFAS-fund er
          st&aelig;rkt rumligt klyngede. Naive statistiske tests overvurderer
          signifikansen markant (294&ndash;448&times; inflation af effektive
          frihedsgrader).
        </li>
      </ul>
    </section>
  );
}
