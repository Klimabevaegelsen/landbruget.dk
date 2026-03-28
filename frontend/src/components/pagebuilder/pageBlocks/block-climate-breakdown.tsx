import { ClimateBreakdown } from '@/services/supabase/types';
import { getClimateEmissions } from '@/lib/climate-data';
import { CarbonAccountingBreakdown } from '@/components/climate';

export async function BlockClimateBreakdown({
  block,
}: {
  block: ClimateBreakdown;
}) {
  try {
    const emissions = await getClimateEmissions(block.cvr);

    if (!emissions || emissions.length === 0) {
      return (
        <div className="bg-muted text-muted-foreground rounded p-4">
          Ingen klimadata tilgængelig
        </div>
      );
    }

    // Filter by year range if specified
    let filteredEmissions = emissions;
    if (block.yearRange) {
      filteredEmissions = emissions.filter(
        (e) =>
          e.year >= block.yearRange!.start && e.year <= block.yearRange!.end
      );
    }

    // Sort by year ascending for proper chart display
    filteredEmissions.sort((a, b) => a.year - b.year);

    if (filteredEmissions.length === 0) {
      return (
        <div className="bg-muted text-muted-foreground rounded p-4">
          Ingen klimadata tilgængelig for det valgte årsinterval
        </div>
      );
    }

    return <CarbonAccountingBreakdown emissions={filteredEmissions} />;
  } catch (error) {
    console.error('Error loading climate breakdown:', error);
    return (
      <div className="bg-destructive/5 text-destructive rounded p-4">
        Fejl ved indlæsning af klimadata
      </div>
    );
  }
}
