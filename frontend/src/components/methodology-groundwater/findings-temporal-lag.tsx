import { SubsectionHeader } from '@/components/methodology/article-layout';

export function FindingsTemporalLag() {
  return (
    <>
      <SubsectionHeader
        id="findings-lag"
        number="5.3"
        title="Forsinkelser på op til 9 år"
      />
      <p>
        Vores analyse af tidsforsinkelser viser, hvornår en sprøjtning på
        overfladen kan måles i boringen:
      </p>
      <ul className="my-4 list-disc space-y-2 pl-6">
        <li>
          <strong className="text-foreground">Hurtige (1,5–3,5 år):</strong>{' '}
          Bentazon og glyphosat siver relativt hurtigt ned.
        </li>
        <li>
          <strong className="text-foreground">Mellemlange (ca. 5 år):</strong>{' '}
          AMPA og MCPA.
        </li>
        <li>
          <strong className="text-foreground">Langsomme (5,5–9 år):</strong>{' '}
          Triazol og MCPA-metabolitter. Det kan tage næsten et årti, før
          konsekvenserne af disse sprøjtninger slår fuldt igennem i
          drikkevandet.
        </li>
      </ul>
    </>
  );
}
