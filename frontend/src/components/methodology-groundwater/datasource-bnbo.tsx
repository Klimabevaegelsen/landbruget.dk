import { SubsectionHeader } from '@/components/methodology/article-layout';

export function DatasourceBnbo() {
  return (
    <>
      <SubsectionHeader
        id="data-bnbo"
        number="2.2"
        title="BNBO – Boringsnære beskyttelsesområder"
      />
      <p>
        Dette er de arealer, der ligger tættest på drikkevandsboringerne. Der er
        udpeget cirka 20.000 hektar BNBO i Danmark, hvoraf cirka 9.500 hektar er
        landbrugsjord. Den 1. juli 2024 trådte et generelt sprøjteforbud i kraft
        i disse zoner. Vi kategoriserer kommunernes BNBO-status i tre
        letlæselige grupper:
      </p>
      <ul className="my-4 list-disc space-y-2 pl-6">
        <li>
          <strong className="text-foreground">Indsats nødvendig</strong> –
          området kræver yderligere handling.
        </li>
        <li>
          <strong className="text-foreground">Gennemført</strong> – indsatsen er
          afsluttet, eller der er ingen erhvervsmæssig pesticidanvendelse.
        </li>
        <li>
          <strong className="text-foreground">Ukendt</strong> –
          statusoplysninger mangler.
        </li>
      </ul>
    </>
  );
}
