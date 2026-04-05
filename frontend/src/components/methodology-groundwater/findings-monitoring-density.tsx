import { SubsectionHeader } from '@/components/methodology/article-layout';

export function FindingsMonitoringDensity() {
  return (
    <>
      <SubsectionHeader
        id="findings-monitoring-density"
        number="5.3"
        title="Overv&aring;gningst&aelig;thedens betydning"
      />

      <div className="border-primary/20 bg-primary/5 my-6 rounded-lg border-l-4 p-5">
        <p className="text-foreground text-sm font-semibold">
          Vigtigste forbehold
        </p>
        <p className="text-muted-foreground mt-2 text-sm">
          Alle tre statistisk robuste korrelationer er{' '}
          <strong className="text-foreground">
            kun p&aring;viselige i oplande med h&oslash;j
            overv&aring;gningst&aelig;thed
          </strong>{' '}
          (&gt;5 boringer). I oplande med f&aelig;rre boringer er
          korrelationerne t&aelig;t p&aring; nul.
        </p>
      </div>

      <div className="border-border bg-card my-6 overflow-x-auto rounded border">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-border border-b">
              <th className="text-muted-foreground px-4 py-2.5 text-left font-medium">
                Stof
              </th>
              <th className="text-muted-foreground px-4 py-2.5 text-right font-medium">
                &le;2 boringer
              </th>
              <th className="text-muted-foreground px-4 py-2.5 text-right font-medium">
                3&ndash;5 boringer
              </th>
              <th className="text-muted-foreground px-4 py-2.5 text-right font-medium">
                &gt;5 boringer
              </th>
            </tr>
          </thead>
          <tbody className="text-muted-foreground">
            <tr className="border-border border-b">
              <td className="text-foreground px-4 py-2.5 font-medium">
                1,2,4-Triazol
              </td>
              <td className="px-4 py-2.5 text-right">&minus;0,012</td>
              <td className="px-4 py-2.5 text-right">0,027</td>
              <td className="text-foreground px-4 py-2.5 text-right font-medium">
                0,186***
              </td>
            </tr>
            <tr className="border-border border-b">
              <td className="text-foreground px-4 py-2.5 font-medium">
                Bentazon
              </td>
              <td className="px-4 py-2.5 text-right">0,012</td>
              <td className="px-4 py-2.5 text-right">&minus;0,019</td>
              <td className="text-foreground px-4 py-2.5 text-right font-medium">
                0,215***
              </td>
            </tr>
            <tr>
              <td className="text-foreground px-4 py-2.5 font-medium">
                4-Chlor-2-methylphenol
              </td>
              <td className="px-4 py-2.5 text-right">0,000</td>
              <td className="px-4 py-2.5 text-right">0,184</td>
              <td className="text-foreground px-4 py-2.5 text-right font-medium">
                0,202***
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <p className="text-muted-foreground text-xs">
        *** p&lt;0,001. Korrelationer (point-biserial r) stratificeret efter
        antal overv&aring;gningsboringer pr. opland.
      </p>

      <p className="mt-4">
        To forklaringer er mulige: enten er den statistiske styrke
        utilstr&aelig;kkelig i tyndt overv&aring;gede oplande, eller ogs&aring;
        er overv&aring;gningsintensiteten en mere kompleks konfounder end en
        enkelt variabel kan fange. Resultaterne kan derfor ikke generaliseres
        til oplande med f&aring; boringer.
      </p>
    </>
  );
}
