import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionGrundvandLimitations() {
  return (
    <section data-testid="section-grundvand-limitations">
      <SectionHeader id="limitations" number="6" title="Begrænsninger" />

      <ol className="my-4 list-decimal space-y-4 pl-6">
        <li>
          <strong className="text-foreground">Kræver mange boringer:</strong> Vi
          kan kun påvise disse sammenhænge i oplande, der overvåges tæt (mere
          end 5 boringer). I områder med færre boringer forsvinder det
          statistiske signal.
        </li>
        <li>
          <strong className="text-foreground">Tidsforsinkelse:</strong> Vandet
          transporteres langsomt. Dagens fund kan stamme fra fortiden, og dagens
          sprøjtning rammer os måske først i fremtiden.
        </li>
        <li>
          <strong className="text-foreground">Ikke-lineære sammenhænge:</strong>{' '}
          Risikoen stiger ikke altid i en lige linje. Estimaterne er derfor
          tilnærmelser og bør ikke overføres direkte til ekstreme scenarier.
        </li>
        <li>
          <strong className="text-foreground">
            Kausalitet vs. korrelation:
          </strong>{' '}
          Vi påviser et stærkt statistisk mønster, men ikke et direkte juridisk
          bevis for, at mark A forurenede boring B.
        </li>
        <li>
          <strong className="text-foreground">Zonegrænsernes præcision:</strong>{' '}
          Modeller for, hvordan vand flyder under jorden, er altid forbundet med
          usikkerhed.
        </li>
      </ol>
    </section>
  );
}
