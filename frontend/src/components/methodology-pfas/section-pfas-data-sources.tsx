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
        title="GEUS Jupiter — PFAS-analyser"
      />
      <p>
        GEUS&apos; nationale grundvandsdatabase (Jupiter) indeholder{' '}
        <strong>441.589 PFAS-analyser</strong> fra danske boringer,
        d&aelig;kkende 26 PFAS-stoffer. Trifluoreddikesyre (TFA) er det hyppigst
        fundne stof med m&aring;linger fra <strong>4.789 boringer</strong>.
        Systematisk TFA-overv&aring;gning startede f&oslash;rst i 2020.
      </p>

      <SubsectionHeader
        id="pfas-grukos"
        number="2.2"
        title="GRUKOS — grundvandsoplande"
      />
      <p>
        Milj&oslash;styrelsens grundvandskortl&aelig;gning (GRUKOS) afgrænser{' '}
        <strong>5.826 indvindingsoplande</strong> for danske vandv&aelig;rker.
        Hvert opland er det areal, hvorfra nedb&oslash;r kan n&aring;
        vandv&aelig;rkets boringer &ndash; og dermed det omr&aring;de, hvor
        pesticidanvendelse potentielt kan p&aring;virke drikkevandet.
      </p>

      <SubsectionHeader
        id="pfas-bmd"
        number="2.3"
        title="Bek&aelig;mpelsesmiddeldatabasen (BMD)"
      />
      <p>
        Milj&oslash;styrelsens BMD indeholder registreringer af alle godkendte
        pesticidprodukter og deres aktivstoffer. Vi har kortlagt{' '}
        <strong>132 fluorholdige produkter</strong> med{' '}
        <strong>52 fluorholdige aktivstoffer</strong>, heraf{' '}
        <strong>35 der kan danne TFA</strong> via metabolisk nedbrydning af
        CF₃-gruppen.
      </p>

      <SubsectionHeader
        id="pfas-disagg"
        number="2.4"
        title="Pesticid-disaggregering"
      />
      <p>
        Vores disaggregeringsmetode fordeler virksomhedsrapporterede
        pesticiddata ned p&aring; individuelle marker (se{' '}
        <a
          href="/pesticidanalyse/metode"
          className="text-primary underline-offset-4 hover:underline"
        >
          metoden for disaggregering
        </a>
        ). Det g&oslash;r det muligt at koble konkrete produkter og aktivstoffer
        til specifikke landbrugsarealer inden for hvert grundvandsopland.
      </p>
    </section>
  );
}
