import { SubsectionHeader } from '@/components/methodology/article-layout';

export function FindingsCorrelations() {
  return (
    <>
      <SubsectionHeader
        id="findings-correlations"
        number="5.1"
        title="Signifikante fund og dosis-respons"
      />
      <p>
        Ud af de 11 stoffer viste{' '}
        <mark>
          <strong className="text-foreground">7 stoffer (64&nbsp;%)</strong> en
          statistisk signifikant sammenh&aelig;ng
        </mark>
        : Jo mere der spr&oslash;jtes p&aring; marken, des oftere findes stoffet
        i grundvandet. Tre af stofferne klarede sig hele vejen gennem vores
        strenge firetrins-validering:
      </p>

      <div className="border-border bg-card my-6 overflow-x-auto rounded border">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-border border-b">
              <th className="text-muted-foreground px-4 py-2.5 text-left font-medium">
                Stof (Type)
              </th>
              <th className="text-muted-foreground px-4 py-2.5 text-right font-medium">
                Korrelation (r)
              </th>
              <th className="text-muted-foreground px-4 py-2.5 text-right font-medium">
                Fund (H&oslash;jeste vs. laveste forbrug)
              </th>
            </tr>
          </thead>
          <tbody className="text-muted-foreground">
            <tr className="border-border border-b">
              <td className="text-foreground px-4 py-2.5 font-medium">
                Bentazon (Moderstof)
              </td>
              <td className="px-4 py-2.5 text-right">0,213</td>
              <td className="px-4 py-2.5 text-right font-semibold">
                4,4&times; h&oslash;jere
              </td>
            </tr>
            <tr className="border-border border-b">
              <td className="text-foreground px-4 py-2.5 font-medium">
                1,2,4-Triazol (Metabolit)
              </td>
              <td className="px-4 py-2.5 text-right">0,232</td>
              <td className="px-4 py-2.5 text-right font-semibold">
                3,8&times; h&oslash;jere
              </td>
            </tr>
            <tr>
              <td className="text-foreground px-4 py-2.5 font-medium">
                4-Chlor-2-methylphenol (Metabolit)
              </td>
              <td className="px-4 py-2.5 text-right">0,222</td>
              <td className="px-4 py-2.5 text-right font-semibold">
                3,7&times; h&oslash;jere
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <p className="text-muted-foreground text-sm">
        <em>
          L&aelig;ses&aring;dan: I de oplande, hvor der blev spr&oslash;jtet
          mest med bentazon, fandt man stoffet i grundvandet 4,4 gange oftere
          end i de oplande, hvor der blev spr&oslash;jtet mindst.
        </em>
      </p>
    </>
  );
}
