import {
  SectionHeader,
  Sidenote,
  SubsectionHeader,
} from '@/components/methodology/article-layout';
import { PopulationExposureTable } from '@/components/methodology/population-exposure-table';
import type { PopulationExposureSummary } from '@/lib/server-cache';

interface SectionPopulationExposureProps {
  summary: PopulationExposureSummary | null;
}

export function SectionPopulationExposure({
  summary,
}: SectionPopulationExposureProps) {
  return (
    <section data-testid="section-population-exposure">
      <SectionHeader
        id="befolkning"
        number="5"
        title="Hvor mange bor tæt på marker?"
      />

      <p>
        Vi fordeler kommunernes befolkning (Danmarks Statistik, FOLK1A
        {summary ? `, ${summary.population_source.period}` : ''}) på boligerne i
        BBR og måler afstanden fra hver beboelsesbygning til nærmeste mark.
        Tallene beskriver potentiel eksponering ud fra afstand, ikke målt optag.
      </p>

      <p>
        Studier, hvor man har målt pesticider i husstøv, luft og urin, finder de
        tydeligste forskelle inden for ca. 50–250 m. Det gælder OBO i Holland
        (2019)
        <Sidenote number={27}>
          Figueiredo et al. (2019), OBO-rapport, RIVM
        </Sidenote>{' '}
        og PestiRiv i Frankrig (2025)
        <Sidenote number={28}>
          Santé publique France &amp; ANSES (2025), PestiRiv
        </Sidenote>
        ; 1 km beskriver mest, hvor mange der bor i landbrugslandskabet.
      </p>

      {summary ? (
        <PopulationExposureTable summary={summary} />
      ) : (
        <p>Tallene vises, når analysen er publiceret.</p>
      )}

      <SubsectionHeader
        id="population-forbehold"
        number="5.1"
        title="Forbehold"
      />
      <ul className="list-disc space-y-3 pl-6">
        <li>
          Sprøjtedata er en modelleret fordeling af indberettet forbrug på
          marker, ikke registrerede sprøjtninger.
        </li>
        <li>
          Kolonnen med rapporteret pesticidforbrug er et nedre skøn, fordi
          forbrug, der ikke kan fordeles på marker, ikke indgår. Konventionel
          mark er et øvre skøn, fordi den også omfatter marker, der ikke
          sprøjtes.
        </li>
        <li>
          Rigsrevisionen (2026) fandt mangelfuld indberetning, så forbruget er
          snarere undervurderet.
        </li>
        <li>
          BBR er et øjebliksbillede, og landmandsfamilier på egne marker tæller
          med.
        </li>
        <li>
          Ingen danske målinger har endnu valideret afstand mod faktisk optag.
        </li>
      </ul>
    </section>
  );
}
