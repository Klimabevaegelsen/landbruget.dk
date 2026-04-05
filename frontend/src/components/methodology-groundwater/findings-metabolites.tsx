import { SubsectionHeader } from '@/components/methodology/article-layout';

export function FindingsMetabolites() {
  return (
    <>
      <SubsectionHeader
        id="findings-metabolites"
        number="5.4"
        title="Metabolitter vs. moderstoffer"
      />
      <p>
        Metabolitter &ndash; nedbrydningsprodukter af pesticider &ndash; kan
        v&aelig;re bedre rumlige mark&oslash;rer for
        moderstof-anvendelsesintensitet end moderstofferne selv. Det klareste
        eksempel er MCPA-metabolitten 4-chlor-2-methylphenol (r=0,222,
        multivariat p=0,006), der overlever alle valideringslag, mens MCPA selv
        (r=0,051) ikke g&oslash;r. Tilsvarende har de to st&aelig;rkeste
        korrelationer i analysen &ndash; 1,2,4-triazol og 4-chlor-2-methylphenol
        &ndash; begge metabolitter.
      </p>
      <p>
        Dette afspejler, at metabolitter ofte har l&aelig;ngere
        milj&oslash;m&aelig;ssig halveringstid, er mere mobile i jord- og
        grundvandssystemer, og akkumuleres fra flere moderstoffer (f.eks. dannes
        1,2,4-triazol fra 12 forskellige triazol-fungicider). Uden
        stofkortl&aelig;gningen, der forbinder overv&aring;gningsnavne med
        anvendelsesregistrenes navne, ville disse signaler v&aelig;re usynlige.
      </p>
      <p>
        <strong className="text-foreground">Politisk implikation:</strong>{' '}
        Grundvandsoverv&aring;gningsprogrammer b&oslash;r prioritere detektion
        af metabolitter frem for moderstoffer, n&aring;r landbrugets
        pesticidpres vurderes.
      </p>
    </>
  );
}
