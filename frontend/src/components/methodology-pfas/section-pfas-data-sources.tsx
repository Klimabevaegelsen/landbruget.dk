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
        danske boringer. TFA er det hyppigst fundne stof med m&aring;linger fra{' '}
        <strong>4.789 boringer</strong>. Den systematiske overv&aring;gning af
        TFA startede f&oslash;rst i 2020.
      </p>

      <SubsectionHeader
        id="pfas-grukos"
        number="2.2"
        title="GRUKOS (Grundvandsoplande)"
      />
      <p>
        Milj&oslash;styrelsens grundvandskortl&aelig;gning afgr&aelig;nser{' '}
        <strong>5.826 indvindingsoplande</strong>. Et opland er det geografiske
        omr&aring;de, hvorfra regnvand siver ned og ender i vandv&aelig;rkets
        boringer. Det er alts&aring; her, landbrugets pesticidforbrug direkte
        kan p&aring;virke drikkevandet.
      </p>

      <SubsectionHeader
        id="pfas-bmd"
        number="2.3"
        title="Bek&aelig;mpelsesmiddeldatabasen (BMD)"
      />
      <p>
        Milj&oslash;styrelsens database over godkendte spr&oslash;jtemidler. Vi
        har kortlagt <strong>132 fluorholdige produkter</strong> fordelt
        p&aring; <strong>52 aktivstoffer</strong>. Af disse kan{' '}
        <strong>35 aktivstoffer danne TFA</strong>, n&aring;r de nedbrydes.
      </p>

      <SubsectionHeader
        id="pfas-disagg"
        number="2.4"
        title="Fordeling af pesticidforbrug p&aring; markniveau"
      />
      <p>
        Vores metode fordeler bedrifternes indberettede pesticidforbrug ud
        p&aring; de enkelte marker (se{' '}
        <a
          href="/pesticidanalyse/metode"
          className="text-primary underline-offset-4 hover:underline"
        >
          metoden for fordeling af pesticidforbrug
        </a>
        ). Dette g&oslash;r det muligt at koble specifikke spr&oslash;jtemidler
        til landbrugsarealerne inden for hvert grundvandsopland.
      </p>
    </section>
  );
}
