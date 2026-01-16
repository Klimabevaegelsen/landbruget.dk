import { KPIGroup } from "@/services/supabase/types";
import { ClimateEmission } from "@/services/supabase/climate";

interface CarbonAccountingKPIsProps {
  emission: ClimateEmission;
}

export function CarbonAccountingKPIs({ emission }: CarbonAccountingKPIsProps) {
  // Format numbers with Danish locale
  const formatNumber = (value: number, decimals: number = 0) => {
    return value.toLocaleString("da-DK", {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
  };

  // Create KPI data structure matching the existing pattern
  const kpiGroup: KPIGroup = {
    _key: `climate-kpis-${emission.year}`,
    _type: "kpiGroup",
    title: `Klimaregnskab ${emission.year}`,
    kpis: [
      {
        key: "total_co2e",
        label: "Total CO₂e (kg)",
        value: formatNumber(emission.total_co2e_kg, 0),
      },
      {
        key: "co2e_per_ha",
        label: "CO₂e pr. hektar (kg/ha)",
        value: formatNumber(emission.co2e_per_ha, 2),
      },
      {
        key: "co2e_per_animal_unit",
        label: "CO₂e pr. dyreenhed (kg/DE)",
        value: formatNumber(emission.co2e_per_animal_unit, 2),
      },
    ],
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
      {kpiGroup.kpis.map((kpi, index) => (
        <div
          key={`${kpiGroup._key}-${index}`}
          className="rounded bg-primary-foreground p-4 flex flex-col gap-2"
        >
          <label className="text-sm font-medium">{kpi.label}</label>
          <p className="text-xl md:text-2xl font-bold text-green-900">
            {kpi.value}
          </p>
        </div>
      ))}
    </div>
  );
}
