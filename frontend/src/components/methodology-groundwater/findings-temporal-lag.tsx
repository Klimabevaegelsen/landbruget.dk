import { SubsectionHeader } from '@/components/methodology/article-layout';

export function FindingsTemporalLag() {
  return (
    <>
      <SubsectionHeader
        id="findings-lag"
        number="5.3"
        title="Forsinkelser p&aring; op til 9 &aring;r"
      />
      <p>
        Vores analyse af tidsforsinkelser viser, hvorn&aring;r en
        spr&oslash;jtning p&aring; overfladen kan m&aring;les i boringen:
      </p>
      <ul className="my-4 list-disc space-y-2 pl-6">
        <li>
          <strong className="text-foreground">
            Hurtige (1,5&ndash;3,5&nbsp;&aring;r):
          </strong>{' '}
          Bentazon og glyphosat siver relativt hurtigt ned.
        </li>
        <li>
          <strong className="text-foreground">
            Mellemlange (ca. 5&nbsp;&aring;r):
          </strong>{' '}
          AMPA og MCPA.
        </li>
        <li>
          <strong className="text-foreground">
            Langsomme (5,5&ndash;9&nbsp;&aring;r):
          </strong>{' '}
          Triazol og MCPA-metabolitter. Det kan tage n&aelig;sten et &aring;rti,
          f&oslash;r konsekvenserne af disse spr&oslash;jtninger sl&aring;r
          fuldt igennem i drikkevandet.
        </li>
      </ul>
    </>
  );
}
