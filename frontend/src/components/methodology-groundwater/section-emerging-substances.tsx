import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionEmergingSubstances() {
  return (
    <section data-testid="section-emerging-substances">
      <SectionHeader
        id="emerging"
        number="7"
        title="Fremadrettet: nye stoffer"
      />

      <p>
        Flere metabolitter af aktuelt godkendte pesticider n&aelig;rmer sig
        detektionsgr&aelig;nsen med stigende v&aelig;kstrater. Disse stoffer
        kvalificerer endnu ikke til korrelationsanalyse (kr&aelig;ver &ge;30
        detektioner pr. opland), men deres tendenser b&oslash;r f&oslash;lges:
      </p>

      <div className="border-border bg-card my-6 overflow-x-auto rounded border">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-border border-b">
              <th className="text-muted-foreground px-4 py-2.5 text-left font-medium">
                Metabolit
              </th>
              <th className="text-muted-foreground px-4 py-2.5 text-left font-medium">
                Moderstof
              </th>
              <th className="text-muted-foreground px-4 py-2.5 text-right font-medium">
                &Aring;rlig v&aelig;kst
              </th>
              <th className="text-muted-foreground px-4 py-2.5 text-right font-medium">
                Forventet kvalificering
              </th>
            </tr>
          </thead>
          <tbody className="text-muted-foreground">
            <tr className="border-border border-b">
              <td className="text-foreground px-4 py-2.5 font-medium">
                Metazachlor OA
              </td>
              <td className="px-4 py-2.5">Metazachlor</td>
              <td className="px-4 py-2.5 text-right">+28&nbsp;%/&aring;r</td>
              <td className="px-4 py-2.5 text-right">2027&ndash;2028</td>
            </tr>
            <tr className="border-border border-b">
              <td className="text-foreground px-4 py-2.5 font-medium">
                Dimethachlor ESA
              </td>
              <td className="px-4 py-2.5">Dimethachlor</td>
              <td className="px-4 py-2.5 text-right">+20&nbsp;%/&aring;r</td>
              <td className="px-4 py-2.5 text-right">2029&ndash;2030</td>
            </tr>
            <tr className="border-border border-b">
              <td className="text-foreground px-4 py-2.5 font-medium">
                Azoxystrobinsyre
              </td>
              <td className="px-4 py-2.5">Azoxystrobin</td>
              <td className="px-4 py-2.5 text-right">+15&nbsp;%/&aring;r</td>
              <td className="px-4 py-2.5 text-right">2028&ndash;2029</td>
            </tr>
            <tr>
              <td className="text-foreground px-4 py-2.5 font-medium">
                Propyzamid
              </td>
              <td className="px-4 py-2.5">Propyzamid</td>
              <td className="px-4 py-2.5 text-right">+5&nbsp;%/&aring;r</td>
              <td className="px-4 py-2.5 text-right">2032+</td>
            </tr>
          </tbody>
        </table>
      </div>

      <p>
        Metazachlor OA har allerede overskredet EU&apos;s
        drikkevandsgr&aelig;nsev&aelig;rdi p&aring; 0,1&nbsp;&mu;g/L i mindst
        &eacute;n overv&aring;gningsboring. Vi vil inkludere disse stoffer i
        korrelationsanalysen, n&aring;r de n&aring;r detektionsgr&aelig;nsen.
      </p>
    </section>
  );
}
