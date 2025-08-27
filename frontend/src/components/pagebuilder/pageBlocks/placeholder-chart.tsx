'use client';

import { ResponsiveContainer } from 'recharts';

interface PlaceholderChartProps {
  title: string;
  dataType: 'nitrate' | 'carbon' | 'subsidies';
}

const DATA_TYPE_CONFIG = {
  nitrate: {
    title: 'Kvælstof data',
    description: 'Data om kvælstofudledning og -håndtering',
    githubUrl: 'https://github.com/Klimabevaegelsen/landbruget.dk/issues/351',
  },
  carbon: {
    title: 'Carbon accounting data',
    description: 'Data om CO2-regnskab og klimaaftryk',
    githubUrl: 'https://github.com/Klimabevaegelsen/landbruget.dk/issues/259',
  },
  subsidies: {
    title: 'Tilskudsdata',
    description: 'Data om landbrugstilskud og støtteordninger',
    githubUrl: 'https://github.com/Klimabevaegelsen/landbruget.dk/issues/284',
  },
};

export function PlaceholderChart({ title, dataType }: PlaceholderChartProps) {
  const config = DATA_TYPE_CONFIG[dataType];

  const handleClick = () => {
    window.open(config.githubUrl, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="relative">
      {/* Chart title */}
      <h3 className="mb-4 text-lg font-semibold">{title}</h3>

      {/* Placeholder chart area */}
      <div
        style={{ width: '100%', height: 400, minHeight: 400, minWidth: 100 }}
        className="mt-4 flex items-center justify-center rounded-lg border-2 border-dashed border-gray-200 bg-gray-50"
      >
        <ResponsiveContainer>
          <div className="flex h-full flex-col items-center justify-center p-8 text-center">
            <div className="mx-auto mb-6 flex h-20 w-20 items-center justify-center rounded-lg bg-gray-200">
              <svg
                className="h-10 w-10 text-gray-400"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                />
              </svg>
            </div>
            <p className="mb-2 text-lg font-medium text-gray-600">
              Data kommer snart
            </p>
            <p className="text-sm text-gray-500">Klik for at hjælpe til</p>
          </div>
        </ResponsiveContainer>
      </div>

      {/* Overlay with call-to-action */}
      <div className="absolute inset-0 flex items-center justify-center rounded-lg bg-white/80 opacity-0 backdrop-blur-sm transition-opacity hover:opacity-100">
        <div className="max-w-sm p-6 text-center">
          <p className="mb-3 text-xl font-semibold text-gray-800">
            Data er der snart
          </p>
          <p className="mb-4 text-sm text-gray-600">
            Hjælp os med at få det endnu hurtigere udgivet
          </p>
          <button
            onClick={handleClick}
            className="cursor-pointer rounded-md bg-green-600 px-6 py-3 font-medium text-white transition-colors hover:bg-green-700"
          >
            Bidrag til open-source
          </button>
        </div>
      </div>
    </div>
  );
}
