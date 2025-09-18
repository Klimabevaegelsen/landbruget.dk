import { PageBuilderItem } from '@/services/supabase/types';
import { NavigationItem, NavigationGroup, Sidenav } from '../layout/sidenav';
import {
  Building2,
  DollarSign,
  MapPin,
  Leaf,
  Users,
  Heart,
  TreePine,
} from 'lucide-react';
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

function groupNavigationItems(items: NavigationItem[]): NavigationGroup[] {
  const groups: NavigationGroup[] = [
    { name: 'Grundlæggende oplysninger', items: [], icon: Building2 },
    { name: 'Økonomi', items: [], icon: DollarSign },
    { name: 'Arealanvendelse', items: [], icon: MapPin },
    { name: 'Miljø', items: [], icon: Leaf },
    { name: 'Medarbejderforhold', items: [], icon: Users },
    { name: 'Dyrevelærd', items: [], icon: Heart },
    { name: 'Klimaregnskab', items: [], icon: TreePine },
  ];

  // Group mapping based on item keys/titles
  const groupMapping: Record<string, number> = {
    // Grundlæggende oplysninger (0)
    'company-identity': 0,
    'company-ownership': 0,
    'company-leadership': 0,
    'company-map-overview': 0,

    // Økonomi (1)
    'financials-latest-kpis': 1,
    'financials-history': 1,
    'financials-detailed-kpis': 1,
    'subsidies-history-stacked': 1,

    // Arealanvendelse (2)
    'land-use-kpis': 2,
    'land-use-crop-distribution': 2,
    'land-use-field-map': 2,

    // Miljø (3)
    'environmental-compliance-overview': 3,
    'bnbo-environmental-status-chart': 3,
    'wetlands-environmental-status-chart': 3,
    'environmental-action-status-chart': 3,
    'water-coverage-effectiveness-chart': 3,
    'environmental-compliance-kpis': 3,
    'environment-kpis': 3,
    'environment-slurry-incidents': 3,
    'environment-nitrogen-leaching': 3,
    'environment-nitrogen-per-field': 3,
    'environment-pesticide-load': 3,
    'environment-pesticide-risks': 3,

    // Medarbejderforhold (4)
    'worker-welfare-kpis': 4,
    'worker-welfare-employees-monthly': 4,
    'worker-welfare-visas': 4,
    'worker-welfare-injuries': 4,

    // Dyrevelærd (5)
    'animal-welfare-kpis-overall': 5,
    'animal-welfare-production-species-chart': 5,
    'animal-welfare-antibiotics-usage-chart': 5,
    'animal-welfare-site-map': 5,
    'animal-welfare-sites-iteration': 5,
    'animal-welfare-transport-chart': 5,

    // Klimaregnskab (6)
    'carbon-accounting-kpis': 6,
    'carbon-accounting-history': 6,
    'carbon-accounting-details': 6,
  };

  // Group items
  items.forEach((item) => {
    const groupIndex = groupMapping[item.id || ''];
    if (groupIndex !== undefined) {
      groups[groupIndex].items.push(item);
    } else {
      // If no group found, add to first group as fallback
      groups[0].items.push(item);
    }
  });

  // Filter out empty groups
  return groups.filter((group) => group.items.length > 0);
}

export function PageBlock({
  block,
  level = 0,
}: {
  block: PageBuilderItem;
  level?: number;
}) {
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

export function PageBuilder({ pageBlocks }: { pageBlocks: PageBuilderItem[] }) {
  const navigationItems: NavigationItem[] = pageBlocks.map((item, index) => ({
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

  const groupedNavigation = groupNavigationItems(navigationItems);

  return (
    <div className="relative flex w-full flex-col gap-10 md:flex-row md:gap-30">
      <div className="w-full border-b md:sticky md:top-4 md:max-h-screen md:w-4/12 md:overflow-y-auto md:border-b-0 md:border-none">
        <Sidenav
          groupedNavigation={groupedNavigation}
          title="Indholdsfortegnelse"
        />
      </div>
      <div className="flex w-full flex-col gap-11 md:w-8/12">
        {pageBlocks.map((item) => (
          <div key={item._key} id={item._key}>
            <BlockContainer
              title={item.title}
              href={`#${item._key}`}
              stickyTitle={item._type === 'iteratedSection'}
            >
              <PageBlock block={item} />
            </BlockContainer>
          </div>
        ))}
      </div>
    </div>
  );
}
