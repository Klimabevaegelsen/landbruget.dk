import { PageBuilderItem } from "@/services/supabase/types";
import { NavigationItem, Sidenav } from "../layout/sidenav";
import { PlaceholderBlock } from "./pageBlocks/placeholder-block";

function PageBlock({ block }: { block: PageBuilderItem }) {
  switch (block._type) {
    case "infoCard":
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
    <div className="flex w-full gap-30">
      <div className="w-4/12">
        <Sidenav navigation={navigationItems} title="Indholdsfortegnelse" />
      </div>
      <div className="w-8/12  flex flex-col gap-11">
        {pageBlocks.map((item) => (
          <div key={item._key} id={item._key}>
            <PageBlock block={item} />
          </div>
        ))}
      </div>
    </div>
  );
}
