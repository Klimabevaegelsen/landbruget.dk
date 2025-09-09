import { PageBuilderItem } from '@/services/supabase/types';
import { NavigationItem, Sidenav } from '../layout/sidenav';
import { BlockPlaceholder } from './pageBlocks/block-placeholder';
import { BlockInfoCard } from './pageBlocks/block-info-card';
import { BlockContainer } from './pageBlocks/block-container';
import { BlockTable } from './pageBlocks/block-table';
import { BlockBarChart } from './pageBlocks/block-bar-chart';
import { BlockComboChart } from './pageBlocks/block-combo-chart';
import { BlockTimeline } from './pageBlocks/block-timeline';
import { BlockKpiGroup } from './pageBlocks/block-kpi-group';
import { BlockMapChart } from './pageBlocks/block-map-chart';
import { BlockIteratedSection } from './pageBlocks/block-iterated-section';
import {
  SkeletonKpiGroup,
  SkeletonInfoCard,
  SkeletonTable,
  SkeletonChart,
  SkeletonMap,
  SkeletonTimeline,
  SkeletonIteratedSection,
} from './pageBlocks/skeleton-blocks';

export function PageBlockProgressive({
  block,
  level = 0,
  isLoading = false,
}: {
  block: PageBuilderItem | null;
  level?: number;
  isLoading?: boolean;
}) {
  if (isLoading || !block) {
    // Show skeleton based on expected block type
    const blockType = block?._type || 'unknown';
    switch (blockType) {
      case 'kpiGroup':
        return <SkeletonKpiGroup />;
      case 'infoCard':
        return <SkeletonInfoCard />;
      case 'dataGrid':
        return <SkeletonTable />;
      case 'stackedBarChart':
      case 'horizontalStackedBarChart':
      case 'barChart':
      case 'comboChart':
        return <SkeletonChart />;
      case 'timeline':
        return <SkeletonTimeline />;
      case 'mapChart':
        return <SkeletonMap />;
      case 'iteratedSection':
        return <SkeletonIteratedSection />;
      default:
        return <SkeletonChart />; // Default skeleton
    }
  }

  // Render actual component
  switch (block._type) {
    case 'kpiGroup':
      return <BlockKpiGroup kpiGroup={block} />;
    case 'infoCard':
      return <BlockInfoCard infoCard={block} />;
    case 'dataGrid':
      return <BlockTable grid={block} />;
    case 'stackedBarChart':
    case 'horizontalStackedBarChart':
    case 'barChart':
      return <BlockBarChart chart={block} />;
    case 'comboChart':
      return <BlockComboChart chart={block} />;
    case 'timeline':
      return <BlockTimeline timeline={block} />;
    case 'mapChart':
      return <BlockMapChart chart={block} />;
    case 'iteratedSection':
      return <BlockIteratedSection iteratedSection={block} level={level} />;
    default:
      return <BlockPlaceholder block={block} />;
  }
}

export function PageBuilderProgressive({
  pageBlocks,
  isLoading = false,
  skeletonCount = 5,
}: {
  pageBlocks: PageBuilderItem[];
  isLoading?: boolean;
  skeletonCount?: number;
}) {
  // Create skeleton navigation items
  const skeletonNavigationItems: NavigationItem[] = Array.from(
    { length: skeletonCount },
    (_, index) => ({
      name: `Indlæser...`,
      href: `#skeleton-${index}`,
      current: index === 0,
      id: `skeleton-${index}`,
    })
  );

  const navigationItems: NavigationItem[] = isLoading
    ? skeletonNavigationItems
    : pageBlocks.map((item, index) => ({
        name: item.title,
        href: `#${item._key}`,
        current: index === 0,
        id: item._key,
        subItems:
          item._type === 'iteratedSection' && item.sections
            ? item.sections.map((section, index) => {
                return {
                  name: section.title,
                  href: `#${section._key}`,
                  current: index === 0,
                  id: section._key + index,
                };
              })
            : undefined,
      }));

  const blocksToRender = isLoading
    ? Array.from(
        { length: skeletonCount },
        (_, index) =>
          ({
            _key: `skeleton-${index}`,
            _type: index % 2 === 0 ? 'kpiGroup' : 'barChart',
            title: 'Indlæser...',
          }) as PageBuilderItem
      )
    : pageBlocks;

  return (
    <div className="relative flex w-full flex-col gap-10 md:flex-row md:gap-30">
      <div className="w-full border-b md:sticky md:top-4 md:max-h-screen md:w-4/12 md:overflow-y-auto md:border-b-0 md:border-none">
        <Sidenav navigation={navigationItems} title="Indholdsfortegnelse" />
      </div>
      <div className="flex w-full flex-col gap-11 md:w-8/12">
        {blocksToRender.map((item) => (
          <div key={item._key} id={item._key}>
            <BlockContainer
              title={item.title}
              href={`#${item._key}`}
              stickyTitle={item._type === 'iteratedSection'}
            >
              <PageBlockProgressive
                block={isLoading ? null : item}
                isLoading={isLoading}
              />
            </BlockContainer>
          </div>
        ))}
      </div>
    </div>
  );
}
