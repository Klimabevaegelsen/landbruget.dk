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
        title="TFA: Fundet i 100 % af prøverne"
      />
      <p>
        TFA er detekteret i{' '}
        <mark>
          <strong>samtlige 2.113 overvågede grundvandsoplande</strong>
        </mark>
        . Medianen er 0,075 µg/L, men der er registreret koncentrationer helt op
        til 4,88 µg/L. Denne massive tilstedeværelse afspejler både brugen af
        fluorholdige pesticider og atmosfærisk nedfald fra nedbrudte kølemidler.
      </p>

      <SubsectionHeader
        id="pfas-monitoring-density"
        number="3.2"
        title="Overvågningsskævhed: Vi finder kun det, vi leder efter"
      />
      <p>
        I alle vores statistiske modeller er{' '}
        <strong>antallet af boringer i et opland</strong> den stærkeste
        indikator for, om der findes PFAS. Det er et klassisk udtryk for{' '}
        <mark>
          <strong>overvågningsskævhed</strong>
        </mark>{' '}
        (<em>observationsbias</em>): Jo mere vi leder, jo mere finder vi.
        Problemet er, at hele <mark>64 %</mark> af oplandene overhovedet ikke
        overvåges for PFAS.
      </p>

      <SubsectionHeader
        id="pfas-dose-response"
        number="3.3"
        title="Svag sammenhæng for traditionelle PFAS"
      />
      <p>
        For de ældre og mere kendte PFAS-stoffer (som PFOS og PFOA) ser vi kun
        en beskeden sammenhæng med landbrugets pesticidforbrug. Oplande med det
        højeste pesticidforbrug har{' '}
        <strong>1,7 gange højere fund af PFOS</strong> sammenlignet med oplande
        med det laveste forbrug. Sammenhængen er dog for svag til entydigt at
        pege på landbruget, da stofferne også kan stamme fra nærliggende
        industri eller brandøvelsespladser.
      </p>

      <SubsectionHeader
        id="pfas-spatial"
        number="3.4"
        title="Geografisk afsmitning (Rumlig autokorrelation)"
      />
      <p>
        PFAS-forurening optræder ofte i store, sammenhængende klynger på
        landkortet. Rent statistisk betyder det, at et fund i én boring smitter
        af på naboboringen. Hvis man ikke justerer for dette i sine modeller,
        vil man få et stærkt overvurderet billede af, hvor præcise de
        geografiske mønstre er.
      </p>
    </section>
  );
}
