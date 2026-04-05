import {
  SectionHeader,
  SubsectionHeader,
} from '@/components/methodology/article-layout';

export function SectionGrundvandLimitations() {
  return (
    <section data-testid="section-grundvand-limitations">
      <SectionHeader id="limitations" number="6" title="Begr&aelig;nsninger" />

      <p>
        Koblingen mellem pesticidanvendelse og grundvandskvalitet er forbundet
        med en r&aelig;kke usikkerheder, som b&oslash;r indg&aring; i enhver
        fortolkning af resultaterne.
      </p>

      <SubsectionHeader
        id="lim-density"
        number="6.1"
        title="Overv&aring;gningst&aelig;thed"
      />
      <p>
        Korrelationerne er kun p&aring;viselige i oplande med h&oslash;j
        boringst&aelig;thed (&gt;5 boringer). I tyndt overv&aring;gede oplande
        er signalerne t&aelig;t p&aring; nul (se afsnit 5.3). Resultaterne kan
        ikke generaliseres til flertallet af danske grundvandsoplande.
      </p>

      <SubsectionHeader
        id="lim-temporal"
        number="6.2"
        title="Tidsforsinkelse"
      />
      <p>
        Grundvandets transporttid fra markoverfladen til en boring kan
        v&aelig;re fra f&aring; &aring;r til flere &aring;rtier. De pesticider,
        der p&aring;vises i dag, kan stamme fra anvendelse for
        10&ndash;30&nbsp;&aring;r siden &ndash; og aktuel anvendelse vil
        f&oslash;rst vise sig &aring;r frem i tiden.
      </p>

      <SubsectionHeader
        id="lim-nonlinearity"
        number="6.3"
        title="Ikke-linearitet"
      />
      <p>
        To af tre multivariatrobuste stoffer (1,2,4-triazol og bentazon) viser
        ikke-line&aelig;re dosis-respons-forhold. De rapporterede odds ratios er
        derfor line&aelig;re tilnærmelser og b&oslash;r ikke ekstrapoleres.
      </p>

      <SubsectionHeader
        id="lim-zones"
        number="6.4"
        title="Zonegr&aelig;nsernes pr&aelig;cision"
      />
      <p>
        Grundvandsoplandenes afgr&aelig;nsning er baseret p&aring;
        hydrogeologisk modellering med iboende usikkerheder. Marker lige uden
        for et indsatsomr&aring;de kan i praksis bidrage til
        grundvandsbelastningen inden for omr&aring;det.
      </p>

      <SubsectionHeader
        id="lim-communication"
        number="6.5"
        title="Fortolkning"
      />
      <p>
        De rapporterede sammenh&aelig;nge er{' '}
        <strong className="text-foreground">
          korrelationer, ikke &aring;rsagssammenh&aelig;nge
        </strong>
        . Resultaterne b&oslash;r fortolkes som hypotesegenererende evidens, der
        underst&oslash;tter m&aring;lrettet mekanistisk forskning &ndash; ikke
        som grundlag for stofspecifikke regulatoriske beslutninger.
      </p>
    </section>
  );
}
