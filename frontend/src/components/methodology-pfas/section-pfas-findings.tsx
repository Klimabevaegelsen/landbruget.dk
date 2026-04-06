import {
  SectionHeader,
  SubsectionHeader,
} from '@/components/methodology/article-layout';

export function SectionPfasFindings() {
  return (
    <section data-testid="section-pfas-findings">
      <SectionHeader id="pfas-findings" number="3" title="Resultater" />

      <SubsectionHeader
        id="pfas-tfa-ubiquity"
        number="3.1"
        title="TFA: 100&nbsp;% detektionsrate"
      />
      <p>
        TFA er detekteret i{' '}
        <mark>
          <strong>samtlige 2.113 overv&aring;gede grundvandsoplande</strong>
        </mark>
        . Medianen er 0,075&nbsp;&micro;g/L, men koncentrationer op til
        4,88&nbsp;&micro;g/L er registreret. Den universelle
        tilstedev&aelig;relse afspejler b&aring;de fluorpesticidbrug og
        atmosf&aelig;risk deposition fra nedbrydning af fluorholdige
        k&oslash;lemidler (HFC&apos;er).
      </p>

      <SubsectionHeader
        id="pfas-monitoring-density"
        number="3.2"
        title="Overv&aring;gningstætheden dominerer"
      />
      <p>
        I alle 19 statistiske modeller er{' '}
        <strong>antallet af boringer per opland</strong> den st&aelig;rkeste
        pr&aelig;diktor for om PFAS detekteres. Det afspejler et
        grundl&aelig;ggende{' '}
        <mark>
          <strong>observationsbias</strong>
        </mark>
        : jo mere vi kigger, jo mere finder vi. Kun <mark>36&nbsp;%</mark> af
        oplandene har nogen PFAS-overv&aring;gning overhovedet.
      </p>

      <SubsectionHeader
        id="pfas-dose-response"
        number="3.3"
        title="Beskedent dosis-respons"
      />
      <p>
        For traditionelle PFAS (PFOS, PFOA) ses en beskeden sammenhæng med
        pesticidintensitet: oplande i den h&oslash;jeste kvartil har{' '}
        <strong>1,7&times; h&oslash;jere PFOS-detektionsrate</strong> (Q4/Q1).
        For PFOA er forholdet 1,3&times;. Korrelationerne er dog svage og kan
        ikke adskilles fra co-lokalisering med andre PFAS-kilder
        (industriomr&aring;der, beredskab).
      </p>

      <SubsectionHeader
        id="pfas-spatial"
        number="3.4"
        title="Rumlig autokorrelation"
      />
      <p>
        PFAS-fund er st&aelig;rkt rumligt autokorrelerede: Moran&apos;s
        I-v&aelig;rdierne (0,25&ndash;0,38) giver{' '}
        <mark>
          <strong>294&ndash;448&times; inflation</strong>
        </mark>{' '}
        af effektive frihedsgrader sammenlignet med uafh&aelig;ngige
        observationer. Det betyder, at naive statistiske tests vil overvurdere
        signifikansen af rumlige m&oslash;nstre markant.
      </p>
    </section>
  );
}
