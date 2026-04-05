import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionIntroduction() {
  return (
    <section data-testid="section-grundvand-introduction">
      <SectionHeader id="introduction" number="1" title="Introduktion" />

      <p>
        Danmark er et af de eneste lande i Europa, hvor 100&nbsp;% af
        drikkevandet stammer fra grundvand. Det g&oslash;r beskyttelsen af
        grundvandsressourcen til et centralt milj&oslash;politisk
        indsatsomr&aring;de &ndash; og g&oslash;r det s&aelig;rligt relevant at
        forst&aring;, hvordan pesticidanvendelsen p&aring; landbrugsmarker
        p&aring;virker det underliggende grundvand.
      </p>

      <p>
        Vores pesticidanalyse fordeler virksomhedsrapporterede pesticiddata ned
        til individuelle marker (se{' '}
        <a
          href="/pesticidanalyse/metode"
          className="text-primary underline-offset-4 hover:underline"
        >
          metoden for disaggregering
        </a>
        ). Denne geografiske pr&aelig;cision &aring;bner for en ny type analyse:
        en direkte rumlig kobling mellem marker med dokumenteret
        pesticidanvendelse og de omr&aring;der, der er udpeget til beskyttelse
        af grundvandet.
      </p>

      <p>
        Artiklen gennemg&aring;r tre centrale datas&aelig;t &ndash;
        Milj&oslash;styrelsens grundvandskortl&aelig;gning (GRUKOS), boringsnære
        beskyttelsesomr&aring;der (BNBO) og GEUS&apos; nationale
        grundvandsoverv&aring;gningsprogram (GRUMO) &ndash; samt den metode, vi
        anvender til at koble dem med pesticiddata p&aring; markniveau. Vi
        beskriver ogs&aring; den statistiske valideringspipeline og de vigtigste
        resultater fra korrelationsanalysen af 11 stoffer p&aring; tv&aelig;rs
        af 5.826 grundvandsoplande.
      </p>
    </section>
  );
}
