import {
  SectionHeader,
  SubsectionHeader,
} from '@/components/methodology/article-layout';

export function SectionStatisticalMethod() {
  return (
    <section data-testid="section-statistical-method">
      <SectionHeader id="statistics" number="4" title="Statistisk metode" />

      <p>
        For at sikre, at fundne sammenh&aelig;nge ikke er tilfældige eller
        drevet af systematiske fejlkilder, anvender vi en firetrins-validering.
        Kun stoffer, der overlever alle fire lag, betragtes som robuste.
      </p>

      <SubsectionHeader
        id="stats-pipeline"
        number="4.1"
        title="Valideringspipeline"
      />
      <ol className="my-4 list-decimal space-y-3 pl-6">
        <li>
          <strong className="text-foreground">FDR-korrektion</strong> &ndash;
          Benjamini&ndash;Hochberg-korrektion p&aring; tv&aelig;rs af 11
          samtidige test (q&lt;0,05) fjerner falske positiver, der opst&aring;r
          ved multipel testning.
        </li>
        <li>
          <strong className="text-foreground">
            Bivariat logistisk regression
          </strong>{' '}
          &ndash; Bekr&aelig;fter hver korrelation med modeldiagnostik (AUC,
          Hosmer&ndash;Lemeshow-test).
        </li>
        <li>
          <strong className="text-foreground">Multivariat justering</strong>{' '}
          &ndash; Kontrollerer for tre konfounders: jordtype (p&aring;virker
          b&aring;de afgrødevalg og pesticidtransport), indtagsdybde (dybere
          magasiner er mindre forurenede), og
          overv&aring;gningsbrøndt&aelig;thed (flere boringer &oslash;ger
          sandsynligheden for fund).
        </li>
        <li>
          <strong className="text-foreground">
            Rumlig modellering (SAR-probit)
          </strong>{' '}
          &ndash; Justerer for spatial autokorrelation, s&aring; naboliggende
          oplandes p&aring;virkning ikke forvr&aelig;nger resultaterne.
        </li>
      </ol>

      <SubsectionHeader
        id="stats-controls"
        number="4.2"
        title="Negativ kontrol"
      />
      <p>
        For at bekr&aelig;fte, at metoden ikke producerer falske positiver,
        testede vi fem stoffer med h&oslash;j jordbinding (K<sub>oc</sub>{' '}
        772&ndash;3.400), som ikke forventes at n&aring; grundvandet:
        diflufenican, prosulfocarb, propiconazol, epoxiconazol og boscalid.
      </p>
      <div className="border-border bg-card my-6 rounded border p-5">
        <p className="text-foreground text-sm font-semibold">
          Resultat: Alle fem negativ-kontrol-stoffer viste ikke-signifikante
          korrelationer (q=1,00).
        </p>
        <p className="text-muted-foreground mt-2 text-sm">
          Metoden fanger alts&aring; kun stoffer, der faktisk kan transporteres
          til grundvand &ndash; ikke tilf&aelig;ldige rumlige m&oslash;nstre.
        </p>
      </div>
    </section>
  );
}
