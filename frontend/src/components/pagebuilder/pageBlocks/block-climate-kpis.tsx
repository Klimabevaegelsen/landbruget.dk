import { ClimateKPIs } from '@/services/supabase/types';
import { getClimateEmissions } from '@/lib/climate-data';
import { CarbonAccountingKPIs } from '@/components/climate';

export async function BlockClimateKPIs({ block }: { block: ClimateKPIs }) {
  try {
    const emissions = await getClimateEmissions(block.cvr, block.year);

    if (!emissions || emissions.length === 0) {
      return (
        <div className="bg-muted text-muted-foreground rounded p-4">
          Ingen klimadata tilgængelig for dette år
        </div>
      );
    }

    // Use the most recent emission if no year specified, or the first result
    const emission = emissions[0];

    return <CarbonAccountingKPIs emission={emission} />;
  } catch (error) {
    console.error('Error loading climate KPIs:', error);
    return (
      <div className="bg-destructive/5 text-destructive rounded p-4">
        Fejl ved indlæsning af klimadata
      </div>
    );
  }
}
