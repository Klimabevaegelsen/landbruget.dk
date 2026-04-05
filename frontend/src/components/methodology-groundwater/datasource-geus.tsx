import { SubsectionHeader } from '@/components/methodology/article-layout';

export function DatasourceGeus() {
  return (
    <>
      <SubsectionHeader
        id="data-geus"
        number="2.3"
        title="GEUS Dataverse &ndash; GRUMO grundvandsoverv&aring;gning"
      />
      <p>
        De Nationale Geologiske Unders&oslash;gelser for Danmark og
        Gr&oslash;nland (GEUS) driver det nationale
        grundvandsoverv&aring;gningsprogram (GRUMO), der &aring;rligt analyserer
        for ca. 53 pesticider og nedbrydningsprodukter i danske
        grundvandsboringer [1].
      </p>
      <p>
        GEUS stiller data til r&aring;dighed via Dataverse med DOI
        10.22008/FK2/IHVDXL. Datas&aelig;ttet indeholder:
      </p>
      <ul className="my-4 list-disc space-y-2 pl-6">
        <li>
          <strong className="text-foreground">633 pesticidstoffer</strong> og 26
          PFAS-forbindelser
        </li>
        <li>
          Over <strong className="text-foreground">4,6 mio. analyser</strong>{' '}
          fordelt p&aring; perioden 1981&ndash;2025
        </li>
        <li>Boringernes geografiske placering fra Jupiter-databasen</li>
      </ul>
      <p>
        Disse data giver et historisk og geografisk billede af pesticidindholdet
        i dansk grundvand &ndash; og muligg&oslash;r en sammenligning med den
        pesticidanvendelse, vi har kortlagt p&aring; de omkringliggende marker.
      </p>
    </>
  );
}
