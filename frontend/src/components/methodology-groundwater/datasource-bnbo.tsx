import { SubsectionHeader } from '@/components/methodology/article-layout';

export function DatasourceBnbo() {
  return (
    <>
      <SubsectionHeader
        id="data-bnbo"
        number="2.2"
        title="BNBO &ndash; Boringsnære beskyttelsesomr&aring;der"
      />
      <p>
        BNBO er de arealer, der ligger t&aelig;ttest p&aring;
        drikkevandsboringer, og som er s&aelig;rligt s&aring;rbare over for
        forurening. Cirka 20.000&nbsp;hektar er udpeget som BNBO i Danmark,
        heraf ca. 9.500&nbsp;ha landbrugsjord. Et generelt spr&oslash;jteforbud
        i BNBO tr&aring;dte i kraft 1.&nbsp;juli 2024 [3].
      </p>
      <p>
        Data hentes fra Milj&oslash;portalens WFS-tjeneste og indeholder
        kommunernes statusindberetninger for hvert BNBO-omr&aring;de. De seks
        detaljerede statuskategorier forenkles til tre:
      </p>
      <ul className="my-4 list-disc space-y-2 pl-6">
        <li>
          <strong className="text-foreground">Indsats n&oslash;dvendig</strong>{' '}
          &ndash; omr&aring;det kr&aelig;ver yderligere handling (frivillig
          aftale udl&oslash;bet, ikke gennemg&aring;et, eller gennemg&aring;et
          med behov for indsats).
        </li>
        <li>
          <strong className="text-foreground">Gennemf&oslash;rt</strong> &ndash;
          indsatsen er afsluttet, eller der er ingen erhvervsm&aelig;ssig
          pesticidanvendelse.
        </li>
        <li>
          <strong className="text-foreground">Ukendt</strong> &ndash;
          statusoplysninger mangler.
        </li>
      </ul>
      <p>
        Denne statusoversigt g&oslash;r det muligt at identificere, hvor
        spr&oslash;jteforbuddet endnu ikke er implementeret &ndash; og om der
        finder dokumenteret pesticidanvendelse sted p&aring; disse arealer.
      </p>
    </>
  );
}
