'use client';

import React from 'react';
import { Monitor, Moon, Sun, Download, Eye } from 'lucide-react';
import { useTheme } from '@/components/theme/theme-provider';

interface SettingsPanelProps {
  className?: string;
}

export function SettingsPanel({ className = '' }: SettingsPanelProps) {
  const { theme, setTheme } = useTheme();

  const handleExportData = () => {
    // Placeholder for data export functionality
    alert('Dataeksport funktionalitet kommer snart!');
  };

  const handleToggleFullscreen = () => {
    if (!document.fullscreenElement) {
      document.documentElement.requestFullscreen();
    } else {
      document.exitFullscreen();
    }
  };

  return (
    <div className={`space-y-6 p-4 ${className}`}>
      <div>
        <h3 className="text-foreground mb-4 text-lg font-semibold">
          Indstillinger
        </h3>
        <p className="text-muted-foreground text-sm">
          Tilpas kortvisningen og dataindstillinger
        </p>
      </div>

      {/* Theme Settings */}
      <div className="space-y-3">
        <h4 className="text-foreground text-sm font-medium">Tema</h4>
        <div className="border-border bg-card flex items-center rounded-lg border p-1">
          <button
            onClick={() => setTheme('light')}
            data-testid="settings-theme-light-button"
            className={`flex items-center justify-center rounded-md px-3 py-2 text-sm font-medium transition-colors ${
              theme === 'light'
                ? 'bg-background text-foreground shadow-sm'
                : 'text-muted-foreground hover:text-foreground'
            }`}
            aria-label="Lyst tema"
          >
            <Sun className="h-4 w-4" />
          </button>
          <button
            onClick={() => setTheme('dark')}
            data-testid="settings-theme-dark-button"
            className={`flex items-center justify-center rounded-md px-3 py-2 text-sm font-medium transition-colors ${
              theme === 'dark'
                ? 'bg-background text-foreground shadow-sm'
                : 'text-muted-foreground hover:text-foreground'
            }`}
            aria-label="Mørkt tema"
          >
            <Moon className="h-4 w-4" />
          </button>
          <button
            onClick={() => setTheme('system')}
            data-testid="settings-theme-system-button"
            className={`flex items-center justify-center rounded-md px-3 py-2 text-sm font-medium transition-colors ${
              theme === 'system'
                ? 'bg-background text-foreground shadow-sm'
                : 'text-muted-foreground hover:text-foreground'
            }`}
            aria-label="System tema"
          >
            <Monitor className="h-4 w-4" />
          </button>
        </div>
      </div>

      {/* Map Settings */}
      <div className="space-y-3">
        <h4 className="text-foreground text-sm font-medium">Kort</h4>
        <button
          onClick={handleToggleFullscreen}
          data-testid="toggle-fullscreen-button"
          className="bg-background hover:bg-muted/50 border-border flex w-full items-center gap-3 rounded-lg border px-3 py-2 text-left transition-colors"
        >
          <Eye className="h-4 w-4" />
          <div className="flex-1">
            <span className="text-foreground text-sm font-medium">
              Fuldskærm
            </span>
            <p className="text-muted-foreground text-xs">
              Vis kortet i fuldskærm
            </p>
          </div>
        </button>
      </div>

      {/* Data Export */}
      <div className="space-y-3">
        <h4 className="text-foreground text-sm font-medium">Data</h4>
        <button
          onClick={handleExportData}
          data-testid="export-data-button"
          className="bg-background hover:bg-muted/50 border-border flex w-full items-center gap-3 rounded-lg border px-3 py-2 text-left transition-colors"
        >
          <Download className="h-4 w-4" />
          <div className="flex-1">
            <span className="text-foreground text-sm font-medium">
              Eksporter data
            </span>
            <p className="text-muted-foreground text-xs">
              Download synlige data som CSV
            </p>
          </div>
        </button>
      </div>

      {/* About */}
      <div className="space-y-3">
        <h4 className="text-foreground text-sm font-medium">Om</h4>
        <div className="bg-muted/50 rounded-lg p-3">
          <h5 className="text-foreground mb-1 text-sm font-medium">
            Landbruget.dk Markanalyse
          </h5>
          <p className="text-muted-foreground text-xs leading-relaxed">
            Visualisering af danske landbrugsmarker med pesticidforbrug,
            miljødata og geografiske oplysninger. Baseret på åbne data fra
            offentlige myndigheder.
          </p>
          <div className="mt-2 flex items-center gap-4 text-xs">
            <span className="text-muted-foreground">Version 2.0</span>
            <span className="text-muted-foreground">•</span>
            <span className="text-muted-foreground">2025 data</span>
          </div>
        </div>
      </div>
    </div>
  );
}
