import { KPIGroup } from '@/services/supabase/types';
import { shouldShowPlaceholder } from './chart-utils';
import { PlaceholderChart } from './placeholder-chart';
import { useCategoryDataContext } from './CategoryDataContext';
import { NoDataPlaceholder } from './no-data-placeholder';
import { DocumentationAccordion } from '@/components/chart';

export function BlockKpiGroup({ kpiGroup }: { kpiGroup: KPIGroup }) {
  const { isInCategoryWithData } = useCategoryDataContext();

  // Check if this KPI group should show a placeholder
  const placeholderDataType = shouldShowPlaceholder(kpiGroup._key);
  if (placeholderDataType) {
    return <PlaceholderChart dataType={placeholderDataType} />;
  }

  // Check if KPI group has no data
  if (!kpiGroup.kpis || kpiGroup.kpis.length === 0) {
    if (!isInCategoryWithData) {
      return <NoDataPlaceholder />;
    } else {
      // If we're in a category with data, render nothing instead of individual message
      return null;
    }
  }

  return (
    <div>
      <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
        {kpiGroup.kpis.map((kpi, index) => (
          <div
            key={`${kpiGroup._key}-${index}`}
            className="bg-primary-foreground flex flex-col gap-2 rounded p-4"
          >
            <label className="text-sm font-medium">{kpi.label}</label>
            <p className="text-xl font-bold text-green-900 md:text-2xl">
              {kpi.value}
            </p>
          </div>
        ))}
      </div>

      {/* Documentation accordion */}
      {kpiGroup.documentation && (
        <DocumentationAccordion documentation={kpiGroup.documentation} />
      )}
    </div>
  );
}
