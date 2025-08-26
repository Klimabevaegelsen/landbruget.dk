"use client";

import { ResponsiveContainer } from "recharts";

interface PlaceholderChartProps {
  title: string;
  dataType: "nitrate" | "carbon" | "subsidies";
}

const DATA_TYPE_CONFIG = {
  nitrate: {
    title: "Kvælstof data",
    description: "Data om kvælstofudledning og -håndtering",
    githubUrl: "https://github.com/Klimabevaegelsen/landbruget.dk/issues/351"
  },
  carbon: {
    title: "Carbon accounting data",
    description: "Data om CO2-regnskab og klimaaftryk",
    githubUrl: "https://github.com/Klimabevaegelsen/landbruget.dk/issues/259"
  },
  subsidies: {
    title: "Tilskudsdata",
    description: "Data om landbrugstilskud og støtteordninger",
    githubUrl: "https://github.com/Klimabevaegelsen/landbruget.dk/issues/284"
  }
};

export function PlaceholderChart({ title, dataType }: PlaceholderChartProps) {
  const config = DATA_TYPE_CONFIG[dataType];

  const handleClick = () => {
    window.open(config.githubUrl, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="relative">
      {/* Chart title */}
      <h3 className="text-lg font-semibold mb-4">{title}</h3>

      {/* Placeholder chart area */}
      <div
        style={{ width: "100%", height: 400, minHeight: 400, minWidth: 100 }}
        className="mt-4 bg-gray-50 rounded-lg border-2 border-dashed border-gray-200 flex items-center justify-center"
      >
        <ResponsiveContainer>
          <div className="text-center p-8 flex flex-col items-center justify-center h-full">
            <div className="w-20 h-20 mx-auto mb-6 bg-gray-200 rounded-lg flex items-center justify-center">
              <svg
                className="w-10 h-10 text-gray-400"
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
            <p className="text-gray-600 font-medium text-lg mb-2">Data kommer snart</p>
            <p className="text-sm text-gray-500">Klik for at hjælpe til</p>
          </div>
        </ResponsiveContainer>
      </div>

      {/* Overlay with call-to-action */}
      <div className="absolute inset-0 bg-white/80 backdrop-blur-sm rounded-lg flex items-center justify-center opacity-0 hover:opacity-100 transition-opacity">
        <div className="text-center p-6 max-w-sm">
          <p className="text-xl font-semibold text-gray-800 mb-3">Data er der snart</p>
          <p className="text-sm text-gray-600 mb-4">Hjælp os med at få det endnu hurtigere udgivet</p>
          <button
            onClick={handleClick}
            className="bg-green-600 text-white px-6 py-3 rounded-md hover:bg-green-700 transition-colors font-medium cursor-pointer"
          >
            Bidrag til open-source
          </button>
        </div>
      </div>
    </div>
  );
}
