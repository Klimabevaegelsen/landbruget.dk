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
        Dette er de arealer, der ligger t&aelig;ttest p&aring;
        drikkevandsboringerne. Der er udpeget cirka 20.000&nbsp;hektar BNBO i
        Danmark, hvoraf cirka 9.500&nbsp;hektar er landbrugsjord. Den
        1.&nbsp;juli 2024 tr&aring;dte et generelt spr&oslash;jteforbud i kraft
        i disse zoner. Vi kategoriserer kommunernes BNBO-status i tre
        letl&aelig;selige grupper:
      </p>
      <ul className="my-4 list-disc space-y-2 pl-6">
        <li>
          <strong className="text-foreground">Indsats n&oslash;dvendig</strong>{' '}
          &ndash; omr&aring;det kr&aelig;ver yderligere handling.
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
    </>
  );
}
