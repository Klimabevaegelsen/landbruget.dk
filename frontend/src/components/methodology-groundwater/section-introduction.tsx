import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionIntroduction() {
  return (
    <section data-testid="section-grundvand-introduction">
      <SectionHeader id="introduction" number="1" title="Introduktion" />

      <p>
        Danmark er et af de få lande i Europa, hvor{' '}
        <mark>100 % af drikkevandet stammer urenset fra grundvandet</mark>. Det
        gør beskyttelsen af vores grundvandsressourcer til en af de
        allervigtigste miljøpolitiske indsatser – og gør det afgørende at
        forstå, præcis hvordan landbrugets pesticidforbrug påvirker de
        underliggende vandmagasiner.
      </p>

      <p>
        Vores pesticidanalyse fordeler bedrifternes indberettede forbrug ud på
        individuelle marker (se{' '}
        <a
          href="/pesticidanalyse/metode"
          className="text-primary underline-offset-4 hover:underline"
        >
          metoden for fordeling af pesticidforbrug
        </a>
        ). Denne geografiske præcision åbner for en helt ny type analyse: en
        direkte geografisk kobling mellem marker med et dokumenteret
        pesticidforbrug og de zoner, der er udpeget til at beskytte
        drikkevandet.
      </p>

      <p>
        Artiklen gennemgår tre centrale datasæt fra myndighederne –
        Miljøstyrelsens grundvandskortlægning (GRUKOS), de boringsnære
        beskyttelsesområder (BNBO) og GEUS&apos; nationale
        grundvandsovervågningsprogram (GRUMO) – samt vores metode til at koble
        dem med pesticiddata. Vi beskriver desuden vores statistiske validering
        og de vigtigste resultater fra analysen af 11 stoffer på tværs af 5.826
        grundvandsoplande.
      </p>
    </section>
  );
}
