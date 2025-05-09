import { PageBuilderItem } from "@/services/supabase/types";
import { NavigationItem, Sidenav } from "../layout/sidenav";
import { PlaceholderBlock } from "./pageBlocks/placeholder-block";
import { InfoCardBlock } from "./pageBlocks/info-card-block";
import { BlockContainer } from "./pageBlocks/block-container";

function PageBlock({ block }: { block: PageBuilderItem }) {
  switch (block._type) {
    case "infoCard":
      return <InfoCardBlock infoCard={block} />;
    case "dataGrid":
    case "kpiGroup":
    case "barChart":
    case "stackedBarChart":
    case "horizontalStackedBarChart":
    case "comboChart":
    case "filterableDataGrid":
    case "collapsibleDataGrid":
    case "iteratedSection":
    default:
      return <PlaceholderBlock block={block} />;
  }
}

export function PageBuilder({ pageBlocks }: { pageBlocks: PageBuilderItem[] }) {
  const navigationItems: NavigationItem[] = pageBlocks.map((item, index) => ({
    name: item.title,
    href: `#${item._key}`,
    current: index === 0,
  }));

  return (
    <div className="flex w-full gap-30 relative">
      <div className="w-4/12 sticky top-4 max-h-screen overflow-y-auto">
        <Sidenav navigation={navigationItems} title="Indholdsfortegnelse" />
      </div>
      <div className="w-8/12  flex flex-col gap-11">
        {pageBlocks.map((item) => (
          <div key={item._key} id={item._key}>
            <BlockContainer title={item.title} href={`#${item._key}`}>
              <PageBlock block={item} />
            </BlockContainer>
          </div>
        ))}
      </div>
    </div>
  );
}
