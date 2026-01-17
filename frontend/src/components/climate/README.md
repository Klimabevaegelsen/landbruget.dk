# Climate Components

Frontend components for displaying farm carbon emissions data.

## Components

### CarbonAccountingKPIs

Displays key performance indicators for farm emissions including:

- Total CO₂e (kg)
- CO₂e per hectare (kg/ha)
- CO₂e per animal unit (kg/DE)

**Usage:**

```typescript
import { CarbonAccountingKPIs } from '@/components/climate';

<CarbonAccountingKPIs emission={emission} />
```

### CarbonAccountingBreakdown

Displays a horizontal stacked bar chart showing emissions by category over multiple years.

Categories include:

- Enteric fermentation (Fordøjelse)
- Manure management (Gødningshåndtering)
- Fertilizer (Gødning)
- Crop residues (Afgrøderester)
- Fuel & machinery (Brændstof & maskiner)
- Electricity (Elektricitet)
- Transport
- Purchased feed (Indkøbt foder)
- Land use change (Arealanvendelsesændringer)
- Other (Andet)

**Usage:**

```typescript
import { CarbonAccountingBreakdown } from '@/components/climate';

<CarbonAccountingBreakdown emissions={emissions} />
```

## PageBuilder Integration

These components are integrated into the PageBuilder system via block types:

### climateKPIs Block

```typescript
{
  _key: "climate-kpis-2023",
  _type: "climateKPIs",
  title: "Klimaregnskab 2023",
  cvr: "12345678",
  year: 2023  // Optional - if omitted, shows most recent
}
```

### climateBreakdown Block

```typescript
{
  _key: "climate-breakdown",
  _type: "climateBreakdown",
  title: "CO₂-udledning over tid",
  cvr: "12345678",
  yearRange: {  // Optional - if omitted, shows all years
    start: 2020,
    end: 2023
  }
}
```

## Service Layer

The climate service (`frontend/src/services/supabase/climate.ts`) provides:

### getClimateEmissions(cvr, year?)

Fetches carbon emissions data for a given CVR number, optionally filtered by year.

**Example:**

```typescript
import { getClimateEmissions } from '@/services/supabase/climate';

// Get all emissions for a farm
const emissions = await getClimateEmissions('12345678');

// Get emissions for a specific year
const emission2023 = await getClimateEmissions('12345678', 2023);
```

## Data Structure

Climate emission data follows this interface:

```typescript
interface ClimateEmission {
  id: string;
  company_id: string;
  cvr_number: string;
  year: number;
  total_co2e_kg: number;
  emissions_by_category: {
    [category: string]: number;
  };
  co2e_per_ha: number;
  co2e_per_animal_unit: number;
  co2e_per_production_unit: number;
  data_completeness: number;
  calculation_timestamp: string;
}
```

## Database

Data is stored in the `farm_carbon_emissions` table with the following key fields:

- `cvr_number`: Company CVR (8 digits)
- `year`: Calendar year
- `total_co2e_kg`: Total emissions in kg CO₂ equivalents
- `emissions_by_category`: JSONB field with category breakdowns
- `co2e_per_ha`: Emissions per hectare
- `co2e_per_animal_unit`: Emissions per animal unit
- `data_completeness`: Quality metric (0.00-1.00)

See migrations:

- `supabase/migrations/20260110225256_create_farm_climate_emissions.sql` (original)
- `supabase/migrations/20260116000000_rename_climate_to_carbon_emissions.sql` (rename)
