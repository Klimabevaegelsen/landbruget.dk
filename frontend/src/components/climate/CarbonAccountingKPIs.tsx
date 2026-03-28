import { KPIGroup } from '@/services/supabase/types';
import type { ClimateEmission } from '@/services/supabase/climate';
import { formatNumberFixed } from '@/lib/formatting';

interface CarbonAccountingKPIsProps {
  emission: ClimateEmission;
}

export function CarbonAccountingKPIs({ emission }: CarbonAccountingKPIsProps) {
  const formatNumber = formatNumberFixed;

  // Create KPI data structure matching the existing pattern
  const kpiGroup: KPIGroup = {
    _key: `climate-kpis-${emission.year}`,
    _type: 'kpiGroup',
    title: `Klimaregnskab ${emission.year}`,
    kpis: [
      {
        key: 'total_co2e',
        label: 'Total CO₂e (kg)',
        value: formatNumber(emission.total_co2e_kg, 0),
      },
      {
        key: 'co2e_per_ha',
        label: 'CO₂e pr. hektar (kg/ha)',
        value: formatNumber(emission.co2e_per_ha, 2),
      },
      {
        key: 'co2e_per_animal_unit',
        label: 'CO₂e pr. dyreenhed (kg/DE)',
        value: formatNumber(emission.co2e_per_animal_unit, 2),
      },
    ],
  };

  return (
    <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
      {kpiGroup.kpis.map((kpi, index) => (
        <div
          key={`${kpiGroup._key}-${index}`}
          className="bg-primary-foreground flex flex-col gap-2 rounded p-4"
        >
          <label className="text-sm font-medium">{kpi.label}</label>
          <p className="text-primary text-xl font-bold md:text-2xl">
            {kpi.value}
          </p>
        </div>
      ))}
    </div>
  );
}
