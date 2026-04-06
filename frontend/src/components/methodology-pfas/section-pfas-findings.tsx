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
        title="TFA: Fundet i 100&nbsp;% af pr&oslash;verne"
      />
      <p>
        TFA er detekteret i{' '}
        <mark>
          <strong>samtlige 2.113 overv&aring;gede grundvandsoplande</strong>
        </mark>
        . Medianen er 0,075&nbsp;&micro;g/L, men der er registreret
        koncentrationer helt op til 4,88&nbsp;&micro;g/L. Denne massive
        tilstedev&aelig;relse afspejler b&aring;de brugen af fluorholdige
        pesticider og atmosf&aelig;risk nedfald fra nedbrudte k&oslash;lemidler.
      </p>

      <SubsectionHeader
        id="pfas-monitoring-density"
        number="3.2"
        title="Overv&aring;gningssk&aelig;vhed: Vi finder kun det, vi leder efter"
      />
      <p>
        I alle vores statistiske modeller er{' '}
        <strong>antallet af boringer i et opland</strong> den st&aelig;rkeste
        indikator for, om der findes PFAS. Det er et klassisk udtryk for{' '}
        <mark>
          <strong>overv&aring;gningssk&aelig;vhed</strong>
        </mark>{' '}
        (<em>observationsbias</em>): Jo mere vi leder, jo mere finder vi.
        Problemet er, at hele <mark>64&nbsp;%</mark> af oplandene overhovedet
        ikke overv&aring;ges for PFAS.
      </p>

      <SubsectionHeader
        id="pfas-dose-response"
        number="3.3"
        title="Svag sammenh&aelig;ng for traditionelle PFAS"
      />
      <p>
        For de &aelig;ldre og mere kendte PFAS-stoffer (som PFOS og PFOA) ser vi
        kun en beskeden sammenh&aelig;ng med landbrugets pesticidforbrug.
        Oplande med det h&oslash;jeste pesticidforbrug har{' '}
        <strong>1,7 gange h&oslash;jere fund af PFOS</strong> sammenlignet med
        oplande med det laveste forbrug. Sammenh&aelig;ngen er dog for svag til
        entydigt at pege p&aring; landbruget, da stofferne ogs&aring; kan stamme
        fra n&aelig;rliggende industri eller brand&oslash;velsespladser.
      </p>

      <SubsectionHeader
        id="pfas-spatial"
        number="3.4"
        title="Geografisk afsmitning (Rumlig autokorrelation)"
      />
      <p>
        PFAS-forurening optr&aelig;der ofte i store, sammenh&aelig;ngende
        klynger p&aring; landkortet. Rent statistisk betyder det, at et fund i
        &eacute;n boring smitter af p&aring; naboboringen. Hvis man ikke
        justerer for dette i sine modeller, vil man f&aring; et st&aelig;rkt
        overvurderet billede af, hvor pr&aelig;cise de geografiske
        m&oslash;nstre er.
      </p>
    </section>
  );
}
