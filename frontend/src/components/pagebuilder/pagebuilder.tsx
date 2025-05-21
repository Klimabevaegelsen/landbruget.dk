import { PageBuilderItem } from "@/services/supabase/types";
import { NavigationItem, Sidenav } from "../layout/sidenav";
import { BlockPlaceholder } from "./pageBlocks/block-placeholder";
import { BlockInfoCard } from "./pageBlocks/block-info-card";
import { BlockContainer } from "./pageBlocks/block-container";
import { BlockTable } from "./pageBlocks/block-table";
import { BlockBarChart } from "./pageBlocks/block-bar-chart";
import { BlockComboChart } from "./pageBlocks/block-combo-chart";
import { BlockTimeline } from "./pageBlocks/block-timeline";
import { BlockKpiGroup } from "./pageBlocks/block-kpi-group";

export function PageBlock({ block }: { block: PageBuilderItem }) {
  switch (block._type) {
    case "kpiGroup":
      return <BlockKpiGroup kpiGroup={block} />;
    case "infoCard":
      return <BlockInfoCard infoCard={block} />;
    case "dataGrid":
      return <BlockTable grid={block} />;
    case "stackedBarChart":
    case "horizontalStackedBarChart":
    case "barChart":
      return <BlockBarChart chart={block} />;
    case "comboChart":
      return <BlockComboChart chart={block} />;
    case "timeline":
      return <BlockTimeline timeline={block} />;
    case "mapChart":
    case "iteratedSection":
    default:
      return <BlockPlaceholder block={block} />;
  }
}

export function PageBuilder({ pageBlocks }: { pageBlocks: PageBuilderItem[] }) {
  const navigationItems: NavigationItem[] = pageBlocks.map((item, index) => ({
    name: item.title,
    href: `#${item._key}`,
    current: index === 0,
  }));

  return (
    <div className="flex flex-col md:flex-row w-full gap-10 md:gap-30 relative">
      <div className="w-full md:w-4/12 md:sticky md:top-4 md:max-h-screen md:overflow-y-auto border-b md:border-b-0 md:border-none">
        <Sidenav navigation={navigationItems} title="Indholdsfortegnelse" />
      </div>
      <div className="w-full md:w-8/12 flex flex-col gap-11">
        {pageBlocks.map((item) => (
          <div key={item._key} id={item._key}>
            <BlockContainer
              title={item.title}
              href={`#${item._key}`}
              secondaryTitle={item._type}
            >
              <PageBlock block={item} />
            </BlockContainer>
          </div>
        ))}
      </div>
    </div>
  );
}
