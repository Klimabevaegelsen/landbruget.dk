'use client';

import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useLoadingToast } from '@/hooks/useLoadingToast';

/**
 * Demo component to test loading toast functionality
 * This component can be temporarily added to pages to test the loading modal behavior
 */
export function LoadingToastDemo() {
  const { showLoadingToast, hideLoadingToast, isLoading } = useLoadingToast();
  const [simulateDelay, setSimulateDelay] = useState(2000);

  const handleSimulateDataLoad = async () => {
    showLoadingToast('Indlæser data', 'Simulerer data indlæsning...');

    // Simulate data loading
    await new Promise((resolve) => setTimeout(resolve, simulateDelay));

    hideLoadingToast();
  };

  const handleSimulateSearch = async () => {
    showLoadingToast('Søger', 'Simulerer søgning...');

    // Simulate search
    await new Promise((resolve) => setTimeout(resolve, simulateDelay));

    hideLoadingToast();
  };

  const handleNewSearchOverride = () => {
    // This should override any existing loading toast
    showLoadingToast('Ny søgning', 'Ny søgning erstatter den forrige...');

    setTimeout(() => {
      hideLoadingToast();
    }, simulateDelay);
  };

  return (
    <Card className="w-full max-w-md">
      <CardHeader>
        <CardTitle>Loading Toast Test</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div>
          <label className="mb-2 block text-sm font-medium">
            Delay (ms): {simulateDelay}
          </label>
          <input
            type="range"
            min="500"
            max="5000"
            step="500"
            value={simulateDelay}
            onChange={(e) => setSimulateDelay(Number(e.target.value))}
            data-testid="delay-range-input"
            className="w-full"
          />
        </div>

        <div className="space-y-2">
          <Button
            onClick={handleSimulateDataLoad}
            disabled={isLoading}
            className="w-full"
          >
            Test Data Loading
          </Button>

          <Button
            onClick={handleSimulateSearch}
            disabled={isLoading}
            className="w-full"
            variant="outline"
          >
            Test Search
          </Button>

          <Button
            onClick={handleNewSearchOverride}
            className="w-full"
            variant="secondary"
          >
            Test Search Override
          </Button>

          <Button
            onClick={hideLoadingToast}
            disabled={!isLoading}
            className="w-full"
            variant="destructive"
          >
            Force Hide Toast
          </Button>
        </div>

        <div className="text-muted-foreground text-sm">
          Status: {isLoading ? 'Loading...' : 'Ready'}
        </div>
      </CardContent>
    </Card>
  );
}
