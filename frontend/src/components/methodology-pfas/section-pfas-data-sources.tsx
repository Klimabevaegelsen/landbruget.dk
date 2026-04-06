import {
  SectionHeader,
  SubsectionHeader,
} from '@/components/methodology/article-layout';

export function SectionPfasDataSources() {
  return (
    <section data-testid="section-pfas-data-sources">
      <SectionHeader id="pfas-data" number="2" title="Datakilder" />

      <SubsectionHeader
        id="pfas-geus"
        number="2.1"
        title="GEUS Jupiter (PFAS-analyser)"
      />
      <p>
        GEUS&apos; nationale grundvandsdatabase indeholder{' '}
        <strong>441.589 analyser</strong> for 26 forskellige PFAS-stoffer i
        danske boringer. TFA er det hyppigst fundne stof med målinger fra{' '}
        <strong>4.789 boringer</strong>. Den systematiske overvågning af TFA
        startede først i 2020.
      </p>

      <SubsectionHeader
        id="pfas-grukos"
        number="2.2"
        title="GRUKOS (Grundvandsoplande)"
      />
      <p>
        Miljøstyrelsens grundvandskortlægning afgrænser{' '}
        <strong>5.826 indvindingsoplande</strong>. Et opland er det geografiske
        område, hvorfra regnvand siver ned og ender i vandværkets boringer. Det
        er altså her, landbrugets pesticidforbrug direkte kan påvirke
        drikkevandet.
      </p>

      <SubsectionHeader
        id="pfas-bmd"
        number="2.3"
        title="Bekæmpelsesmiddeldatabasen (BMD)"
      />
      <p>
        Miljøstyrelsens database over godkendte sprøjtemidler. Vi har kortlagt{' '}
        <strong>132 fluorholdige produkter</strong> fordelt på{' '}
        <strong>52 aktivstoffer</strong>. Af disse kan{' '}
        <strong>35 aktivstoffer danne TFA</strong>, når de nedbrydes.
      </p>

      <SubsectionHeader
        id="pfas-disagg"
        number="2.4"
        title="Fordeling af pesticidforbrug på markniveau"
      />
      <p>
        Vores metode fordeler bedrifternes indberettede pesticidforbrug ud på de
        enkelte marker (se{' '}
        <a
          href="/pesticidanalyse/metode"
          className="text-primary underline-offset-4 hover:underline"
        >
          metoden for fordeling af pesticidforbrug
        </a>
        ). Dette gør det muligt at koble specifikke sprøjtemidler til
        landbrugsarealerne inden for hvert grundvandsopland.
      </p>
    </section>
  );
}
