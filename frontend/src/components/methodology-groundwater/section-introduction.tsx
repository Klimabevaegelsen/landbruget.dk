import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionIntroduction() {
  return (
    <section data-testid="section-grundvand-introduction">
      <SectionHeader id="introduction" number="1" title="Introduktion" />

      <p>
        Danmark er et af de f&aring; lande i Europa, hvor{' '}
        <mark>100&nbsp;% af drikkevandet stammer urenset fra grundvandet</mark>.
        Det g&oslash;r beskyttelsen af vores grundvandsressourcer til en af de
        allervigtigste milj&oslash;politiske indsatser &ndash; og g&oslash;r det
        afg&oslash;rende at forst&aring;, pr&aelig;cis hvordan landbrugets
        pesticidforbrug p&aring;virker de underliggende vandmagasiner.
      </p>

      <p>
        Vores pesticidanalyse fordeler bedrifternes indberettede forbrug ud
        p&aring; individuelle marker (se{' '}
        <a
          href="/pesticidanalyse/metode"
          className="text-primary underline-offset-4 hover:underline"
        >
          metoden for fordeling af pesticidforbrug
        </a>
        ). Denne geografiske pr&aelig;cision &aring;bner for en helt ny type
        analyse: en direkte geografisk kobling mellem marker med et dokumenteret
        pesticidforbrug og de zoner, der er udpeget til at beskytte
        drikkevandet.
      </p>

      <p>
        Artiklen gennemg&aring;r tre centrale datas&aelig;t fra myndighederne
        &ndash; Milj&oslash;styrelsens grundvandskortl&aelig;gning (GRUKOS), de
        boringsnære beskyttelsesomr&aring;der (BNBO) og GEUS&apos; nationale
        grundvandsoverv&aring;gningsprogram (GRUMO) &ndash; samt vores metode
        til at koble dem med pesticiddata. Vi beskriver desuden vores
        statistiske validering og de vigtigste resultater fra analysen af 11
        stoffer p&aring; tv&aelig;rs af 5.826 grundvandsoplande.
      </p>
    </section>
  );
}
