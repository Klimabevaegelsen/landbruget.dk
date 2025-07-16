"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

interface PipelineConfig {
  years: string;
  modes: string;
  h3_resolutions: string;
  memory_limit: string;
  thread_count: string;
  chunk_size: string;
}

export function H3PfasTrigger() {
  const [isLoading, setIsLoading] = useState(false);
  const [config, setConfig] = useState<PipelineConfig>({
    years: "2023",
    modes: "h3,kommune",
    h3_resolutions: "7,8,9,10",
    memory_limit: "14GB",
    thread_count: "4",
    chunk_size: "10000"
  });

  const triggerPipeline = async () => {
    setIsLoading(true);
    
    try {
      // GitHub Actions workflow dispatch API call
      const response = await fetch('/api/trigger-h3-pfas', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(config),
      });

      if (response.ok) {
        alert('H3 PFAS pipeline triggered successfully!');
      } else {
        throw new Error('Failed to trigger pipeline');
      }
    } catch (error) {
      console.error('Error triggering pipeline:', error);
      alert('Failed to trigger pipeline. Please try again.');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="p-6 bg-white rounded-lg shadow-lg max-w-2xl mx-auto">
      <h2 className="text-2xl font-bold mb-6">🧪 H3 PFAS Pipeline Trigger</h2>
      
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        {/* Years Selection */}
        <div>
          <label className="block text-sm font-medium mb-2">Years</label>
          <Select value={config.years} onValueChange={(value) => setConfig({...config, years: value})}>
            <SelectTrigger>
              <SelectValue placeholder="Select years" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="2023">2023 (Latest)</SelectItem>
              <SelectItem value="2022,2023">2022-2023</SelectItem>
              <SelectItem value="2021,2022,2023">2021-2023</SelectItem>
              <SelectItem value="2015,2016,2017,2018,2019,2020,2021,2022,2023">All Years (2015-2023)</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* H3 Resolutions */}
        <div>
          <label className="block text-sm font-medium mb-2">H3 Resolutions</label>
          <Select value={config.h3_resolutions} onValueChange={(value) => setConfig({...config, h3_resolutions: value})}>
            <SelectTrigger>
              <SelectValue placeholder="Select resolutions" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="10">Resolution 10 (~1.5ha)</SelectItem>
              <SelectItem value="9,10">Resolutions 9-10</SelectItem>
              <SelectItem value="8,9,10">Resolutions 8-10</SelectItem>
              <SelectItem value="7,8,9,10">All Resolutions (7-10)</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Memory Limit */}
        <div>
          <label className="block text-sm font-medium mb-2">Memory Limit</label>
          <Select value={config.memory_limit} onValueChange={(value) => setConfig({...config, memory_limit: value})}>
            <SelectTrigger>
              <SelectValue placeholder="Select memory" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="12GB">12GB (Conservative)</SelectItem>
              <SelectItem value="14GB">14GB (Recommended)</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Thread Count */}
        <div>
          <label className="block text-sm font-medium mb-2">Thread Count</label>
          <Select value={config.thread_count} onValueChange={(value) => setConfig({...config, thread_count: value})}>
            <SelectTrigger>
              <SelectValue placeholder="Select threads" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="2">2 Threads</SelectItem>
              <SelectItem value="4">4 Threads (Max)</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      {/* Current Configuration Display */}
      <div className="bg-gray-50 p-4 rounded-lg mb-6">
        <h3 className="font-medium mb-2">Current Configuration:</h3>
        <div className="text-sm space-y-1">
          <div><strong>Years:</strong> {config.years}</div>
          <div><strong>Modes:</strong> H3 + Kommune Analysis</div>
          <div><strong>H3 Resolutions:</strong> {config.h3_resolutions}</div>
          <div><strong>Resources:</strong> {config.memory_limit} memory, {config.thread_count} threads</div>
          <div><strong>Optimization:</strong> Single process, shared data loading</div>
        </div>
      </div>

      {/* Trigger Button */}
      <Button 
        onClick={triggerPipeline} 
        disabled={isLoading}
        className="w-full"
        size="lg"
      >
        {isLoading ? (
          <>
            <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
            Triggering Pipeline...
          </>
        ) : (
          <>
            🚀 Trigger H3 PFAS Analysis
          </>
        )}
      </Button>

      <div className="mt-4 text-sm text-gray-600">
        <p><strong>Note:</strong> This will trigger the optimized pipeline that processes all resolutions in a single job to avoid redundant data downloads.</p>
        <p><strong>Processing time:</strong> ~10-20 minutes depending on years selected.</p>
      </div>
    </div>
  );
} 