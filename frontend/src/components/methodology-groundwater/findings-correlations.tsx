import { SubsectionHeader } from '@/components/methodology/article-layout';

export function FindingsCorrelations() {
  return (
    <>
      <SubsectionHeader
        id="findings-correlations"
        number="5.1"
        title="Signifikante korrelationer"
      />
      <p>
        Af 11 kvalificerende stoffer (med &ge;30 detektioner pr. opland i
        2018+-vinduet) viser{' '}
        <strong className="text-foreground">7 (64&nbsp;%)</strong> statistisk
        signifikante positive korrelationer efter
        Benjamini&ndash;Hochberg-korrektion (q&lt;0,05) mellem
        anvendelsesintensitet og detektionssandsynlighed i grundvandet. Tre af
        disse overlever desuden alle fire valideringslag (se afsnit 4).
      </p>
      <p>
        De st&aelig;rkeste korrelationer ses for 1,2,4-triazol (r=0,232),
        4-chlor-2-methylphenol (r=0,222) og bentazon (r=0,213). For de tre
        robuste stoffer er detektionsrater{' '}
        <strong className="text-foreground">3,7&ndash;4,4&times;</strong>{' '}
        h&oslash;jere i det &oslash;verste anvendelseskvartil sammenlignet med
        det laveste.
      </p>
      <div className="border-border bg-card my-6 rounded border p-5">
        <p className="text-foreground text-sm font-semibold">
          Alle 7 FDR-signifikante stoffer (efter korrelationsstyrke)
        </p>
        <ol className="text-muted-foreground mt-3 list-decimal space-y-1 pl-5 text-sm">
          <li>
            1,2,4-Triazol (metabolit) &mdash; r=0,232, Q4/Q1&nbsp;3,8&times;
            &nbsp;&#x2713;
          </li>
          <li>
            4-Chlor-2-methylphenol (metabolit) &mdash; r=0,222,
            Q4/Q1&nbsp;3,7&times; &nbsp;&#x2713;
          </li>
          <li>
            Bentazon (moderstof) &mdash; r=0,213, Q4/Q1&nbsp;4,4&times;
            &nbsp;&#x2713;
          </li>
          <li>AMPA (metabolit) &mdash; r=0,181, Q4/Q1&nbsp;4,7&times;</li>
          <li>
            Glyphosat (moderstof) &mdash; r=0,123, Q4/Q1&nbsp;4,0&times;
            &nbsp;(p=0,055 multivariat)
          </li>
          <li>
            2,4-Dichlorphenol (metabolit) &mdash; r=0,071,
            Q4/Q1&nbsp;10,2&times;
          </li>
          <li>MCPA (moderstof) &mdash; r=0,051, Q4/Q1&nbsp;5,1&times;</li>
        </ol>
        <p className="text-muted-foreground mt-3 text-xs">
          &#x2713; = overlever multivariat justering og rumlig modellering.
          Glyphosat viser signifikant bivariat korrelation, men overlever ikke
          multivariat justering (p=0,055), hvilket indikerer delvis
          konfoundering af hydrogeologiske variable.
        </p>
      </div>
    </>
  );
}
