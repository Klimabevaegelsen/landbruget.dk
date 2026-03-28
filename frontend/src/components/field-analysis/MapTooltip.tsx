import type { CSSProperties } from 'react';
import { TooltipInfo } from './map-constants';
import { buildTooltipData } from './map-tooltip-data';
import { formatTooltipValue } from './map-tooltip-format';

export function MapTooltip({
  x,
  y,
  properties,
  layerName,
  visualizationMode,
  colorUnit,
}: TooltipInfo) {
  const relevantData = buildTooltipData(
    properties,
    layerName,
    visualizationMode,
    colorUnit
  );
  const tooltipStyle: CSSProperties = {
    left: x,
    top: y,
    transform: 'translate(-50%, -100%)',
    marginTop: -12,
  };

  return (
    <div
      className="border-border bg-background absolute z-[60] max-w-sm rounded-xl border shadow-xl backdrop-blur-sm"
      style={tooltipStyle}
    >
      <div className="border-border border-b px-4 py-3">
        <h3 className="text-foreground text-base leading-tight font-semibold">
          {layerName}
        </h3>
        {properties.site_name ? (
          <p className="text-muted-foreground mt-1 text-sm font-medium">
            {String(properties.site_name)}
          </p>
        ) : null}
        {(layerName === 'BNBO Område' ||
          layerName === 'Lavbundsområde' ||
          layerName === 'Vandprojekt') &&
          Boolean(properties.crop_name) && (
            <p className="text-muted-foreground mt-1 text-xs italic">
              Inkluderer markdata
            </p>
          )}
      </div>

      <div className="px-4 py-3">
        <div className="space-y-2.5">
          {relevantData.map(({ label, value, unit }, index) => (
            <div
              key={index}
              className="flex items-baseline justify-between gap-3"
            >
              <span className="text-muted-foreground text-sm leading-tight font-medium">
                {label}:
              </span>
              <span className="text-foreground text-right text-sm leading-tight font-semibold">
                {formatTooltipValue(value, unit)}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
