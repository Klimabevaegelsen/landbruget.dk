import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionEmergingSubstances() {
  return (
    <section data-testid="section-emerging-substances">
      <SectionHeader
        id="emerging"
        number="7"
        title="Fremtiden: De nye trusler"
      />

      <p>
        Flere nedbrydningsprodukter fra <em>aktuelt</em> godkendte pesticider
        viser stigende tendens i vandprøverne. Eksempelvis vokser fundene af
        svampemidlet <em>metazachlor OA</em> med 28 % årligt og forventes at
        indgå i vores korrelationsanalyse omkring 2027–2028. Dette stof har
        allerede overskredet EU&apos;s grænseværdier i mindst én dansk boring.
        Vi overvåger udviklingen tæt og opdaterer analyserne, i takt med at
        dataene modnes.
      </p>
    </section>
  );
}
