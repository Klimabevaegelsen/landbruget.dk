'use client';

import { useState, useMemo } from 'react';
import { AllocationBar } from '@/components/methodology/allocation-bar';

export function AllocationExplorer() {
  const [totalDosage, setTotalDosage] = useState(50);
  const [fieldAreas, setFieldAreas] = useState([10, 8, 7]);

  const totalArea = fieldAreas.reduce((sum, a) => sum + a, 0);

  const allocations = useMemo(
    () =>
      fieldAreas.map((area, i) => ({
        label: `Mark ${String.fromCharCode(65 + i)} (${area} ha)`,
        dosage: Math.round(totalDosage * (area / totalArea) * 100) / 100,
        pct: Math.round((area / totalArea) * 1000) / 10,
      })),
    [fieldAreas, totalArea, totalDosage]
  );

  const updateField = (index: number, value: number) => {
    const next = [...fieldAreas];
    next[index] = value;
    setFieldAreas(next);
  };

  return (
    <div data-testid="allocation-explorer">
      <div className="text-foreground/70 mb-5 flex items-baseline gap-6 text-sm">
        <label htmlFor="dosage-input" className="flex items-baseline gap-2">
          Samlet dosis:
          <input
            id="dosage-input"
            type="number"
            min={1}
            max={500}
            value={totalDosage}
            onChange={(e) => setTotalDosage(Number(e.target.value) || 1)}
            className="border-border text-foreground focus:border-foreground/50 w-16 border-b bg-transparent text-center focus:outline-none"
            data-testid="dosage-input"
          />
          <span>L</span>
        </label>
        <span className="text-muted-foreground">|</span>
        <span>
          Samlet markareal:{' '}
          <strong className="text-foreground">{totalArea} ha</strong>
        </span>
        <span className="text-muted-foreground">|</span>
        <span>
          {fieldAreas.length} marker
          <button
            type="button"
            onClick={() =>
              fieldAreas.length < 5 && setFieldAreas([...fieldAreas, 5])
            }
            disabled={fieldAreas.length >= 5}
            className="text-muted-foreground hover:text-foreground ml-1 disabled:invisible"
            data-testid="add-field-btn"
          >
            (+)
          </button>
          <button
            type="button"
            onClick={() =>
              fieldAreas.length > 2 && setFieldAreas(fieldAreas.slice(0, -1))
            }
            disabled={fieldAreas.length <= 2}
            className="text-muted-foreground hover:text-foreground ml-1 disabled:invisible"
            data-testid="remove-field-btn"
          >
            (-)
          </button>
        </span>
      </div>

      <div className="mb-5 space-y-2">
        {fieldAreas.map((area, i) => (
          <div key={i} className="flex items-center gap-3 text-sm">
            <span className="text-muted-foreground w-14 font-mono">
              Mark {String.fromCharCode(65 + i)}
            </span>
            <input
              type="range"
              min={1}
              max={50}
              value={area}
              onChange={(e) => updateField(i, Number(e.target.value))}
              className="bg-muted accent-foreground h-1 flex-1 appearance-none rounded"
              data-testid={`field-${i}-slider`}
            />
            <span className="text-foreground/70 w-10 text-right font-mono">
              {area} ha
            </span>
          </div>
        ))}
      </div>

      <table className="w-full text-sm" data-testid="allocation-table">
        <thead>
          <tr className="border-border text-muted-foreground border-b text-left text-xs tracking-wider uppercase">
            <th className="pb-2">Mark</th>
            <th className="pb-2 text-right">Areal</th>
            <th className="pb-2 text-right">Andel</th>
            <th className="pb-2 text-right">Tildelt dosis</th>
            <th className="w-1/3 pb-2" />
          </tr>
        </thead>
        <tbody>
          {allocations.map((a) => (
            <AllocationBar
              key={a.label}
              name={a.label.split(' (')[0]}
              areaLabel={a.label.match(/\((.+)\)/)?.[1] ?? ''}
              dosage={a.dosage}
              percentage={a.pct}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}
