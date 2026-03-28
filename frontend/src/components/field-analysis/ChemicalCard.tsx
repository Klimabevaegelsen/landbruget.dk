'use client';

import React from 'react';

interface Metric {
  label: string;
  value: string;
}

export function ChemicalCard({
  icon,
  label,
  count,
  border,
  bg,
  textColor,
  metricColor,
  metrics,
}: {
  icon?: React.ReactNode;
  label: string;
  count: number;
  border?: string;
  bg: string;
  textColor: string;
  metricColor: string;
  metrics: (Metric | null)[];
}) {
  const filtered = metrics.filter(Boolean) as Metric[];
  return (
    <div className={`${bg} ${border ? `${border} border` : ''} rounded-lg p-2`}>
      <div className="mb-1 flex items-center justify-between">
        <span className={`${textColor} flex items-center text-sm font-medium`}>
          {icon}
          {label}
        </span>
        <span className={`${textColor} text-sm font-bold`}>{count} apps</span>
      </div>
      {filtered.length > 0 && (
        <div className={`${metricColor} space-y-1 text-xs`}>
          {filtered.map((m) => (
            <div key={m.label} className="flex justify-between">
              <span>{m.label}</span>
              <span className="font-medium">{m.value}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
