import { SubsectionHeader } from '@/components/methodology/article-layout';

export function FindingsLegacy() {
  return (
    <>
      <SubsectionHeader
        id="findings-legacy"
        number="5.6"
        title="Historisk forurening"
      />
      <p>
        Flere stoffer med h&oslash;je detektionsrater viser{' '}
        <em>ingen korrelation</em> med nuv&aelig;rende
        anvendelsesm&oslash;nstre:
      </p>
      <div className="border-border bg-card my-6 rounded border p-5">
        <ul className="text-muted-foreground space-y-2 text-sm">
          <li>
            <strong className="text-foreground">Desphenyl chloridazon</strong>{' '}
            &ndash; p&aring;vist i 47,7&nbsp;% af boringer. Chloridazon blev
            forbudt i 1996.
          </li>
          <li>
            <strong className="text-foreground">BAM</strong>{' '}
            (2,6-dichlorbenzamid) &ndash; p&aring;vist i 31,4&nbsp;% af
            boringer. Dichlobenil blev forbudt i 1997.
          </li>
        </ul>
      </div>
      <p>
        Disse stoffer er nu allestedsn&aelig;rv&aelig;rende i dansk grundvand
        uanset aktuel landbrugsaktivitet &ndash; og vil forblive det i
        &aring;rtier p&aring; grund af grundvandets opholdstid. Det understreger
        de langsigtede konsekvenser af pesticidgodkendelser og vigtigheden af
        proaktiv grundvandsbeskyttelse.
      </p>
    </>
  );
}
