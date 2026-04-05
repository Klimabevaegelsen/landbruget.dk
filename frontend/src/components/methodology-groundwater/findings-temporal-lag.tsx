import { SubsectionHeader } from '@/components/methodology/article-layout';

export function FindingsTemporalLag() {
  return (
    <>
      <SubsectionHeader
        id="findings-lag"
        number="5.5"
        title="Stofspecifikke transporttider"
      />
      <p>
        Korrelationer beregnet over flere detektionsvinduer afsl&oslash;rer
        optimale tidsforsinkelser p&aring; 1,5&ndash;9&nbsp;&aring;r mellem
        anvendelse og detektion. Tre grupper tegner sig:
      </p>
      <ul className="my-4 list-disc space-y-2 pl-6">
        <li>
          <strong className="text-foreground">
            Hurtige (1,5&ndash;3,5&nbsp;&aring;r):
          </strong>{' '}
          Bentazon 1,5&nbsp;&aring;r [95&nbsp;% CI: 1,0&ndash;3,0],
          2,4-dichlorphenol 3&nbsp;&aring;r [1,5&ndash;4,0], glyphosat
          3,5&nbsp;&aring;r [2,0&ndash;5,0] &ndash; mobile stoffer, der hurtigt
          n&aring;r grundvandet.
        </li>
        <li>
          <strong className="text-foreground">
            Mellemlange (5&nbsp;&aring;r):
          </strong>{' '}
          AMPA 5&nbsp;&aring;r [3,5&ndash;5,5] og MCPA 5&nbsp;&aring;r
          [3,5&ndash;5,5] &ndash; l&aelig;ngere vadosetransport.
        </li>
        <li>
          <strong className="text-foreground">
            Langsomme (5,5&ndash;9&nbsp;&aring;r):
          </strong>{' '}
          1,2,4-triazol 5,5&nbsp;&aring;r [3,5&ndash;7,0] og
          4-chlor-2-methylphenol 9&nbsp;&aring;r [7,5&ndash;9,0] &ndash;
          h&oslash;j jordbinding og langsom nedbrydning.
        </li>
      </ul>
      <p>
        Fundet indeb&aelig;rer, at korrelationsanalyser baseret p&aring; et
        enkelt tidspunkt systematisk kan undervurdere den reelle
        sammenh&aelig;ng for langsomt transporterede stoffer.
        4-chlor-2-methylphenols optimale forsinkelse p&aring; 9&nbsp;&aring;r
        antyder, at MCPA-metabolittens p&aring;virkning af grundvandet
        kr&aelig;ver n&aelig;sten et &aring;rti for at manifestere sig fuldt ud.
      </p>
    </>
  );
}
