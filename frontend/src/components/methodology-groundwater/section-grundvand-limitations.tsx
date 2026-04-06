import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionGrundvandLimitations() {
  return (
    <section data-testid="section-grundvand-limitations">
      <SectionHeader id="limitations" number="6" title="Begr&aelig;nsninger" />

      <ol className="my-4 list-decimal space-y-4 pl-6">
        <li>
          <strong className="text-foreground">
            Kr&aelig;ver mange boringer:
          </strong>{' '}
          Vi kan kun p&aring;vise disse sammenh&aelig;nge i oplande, der
          overv&aring;ges t&aelig;t (mere end 5 boringer). I omr&aring;der med
          f&aelig;rre boringer forsvinder det statistiske signal.
        </li>
        <li>
          <strong className="text-foreground">Tidsforsinkelse:</strong> Vandet
          transporteres langsomt. Dagens fund kan stamme fra fortiden, og dagens
          spr&oslash;jtning rammer os m&aring;ske f&oslash;rst i fremtiden.
        </li>
        <li>
          <strong className="text-foreground">
            Ikke-line&aelig;re sammenh&aelig;nge:
          </strong>{' '}
          Risikoen stiger ikke altid i en lige linje. Estimaterne er derfor
          tiln&aelig;rmelser og b&oslash;r ikke overf&oslash;res direkte til
          ekstreme scenarier.
        </li>
        <li>
          <strong className="text-foreground">
            Kausalitet vs. korrelation:
          </strong>{' '}
          Vi p&aring;viser et st&aelig;rkt statistisk m&oslash;nster, men ikke
          et direkte juridisk bevis for, at mark A forurenede boring B.
        </li>
        <li>
          <strong className="text-foreground">
            Zonegr&aelig;nsernes pr&aelig;cision:
          </strong>{' '}
          Modeller for, hvordan vand flyder under jorden, er altid forbundet med
          usikkerhed.
        </li>
      </ol>
    </section>
  );
}
