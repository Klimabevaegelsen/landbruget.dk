"use client";

import { useState } from "react";
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
  const [showModal, setShowModal] = useState(false);
  const config = DATA_TYPE_CONFIG[dataType];

  return (
    <>
      <div className="relative">
        {/* Chart title */}
        <h3 className="text-lg font-semibold mb-4">{title}</h3>

        {/* Placeholder chart area */}
        <div
          style={{ width: "100%", height: 400, minHeight: 400, minWidth: 100 }}
          className="mt-4 bg-gray-50 rounded-lg border-2 border-dashed border-gray-200 flex items-center justify-center cursor-pointer hover:bg-gray-100 transition-colors"
          onClick={() => setShowModal(true)}
        >
          <ResponsiveContainer>
            <div className="text-center p-8">
              <div className="w-16 h-16 mx-auto mb-4 bg-gray-200 rounded-lg flex items-center justify-center">
                <svg
                  className="w-8 h-8 text-gray-400"
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
              <p className="text-gray-600 font-medium">Data kommer snart</p>
              <p className="text-sm text-gray-500 mt-1">Klik for at hjælpe til</p>
            </div>
          </ResponsiveContainer>
        </div>

        {/* Overlay with call-to-action */}
        <div className="absolute inset-0 bg-white/80 backdrop-blur-sm rounded-lg flex items-center justify-center opacity-0 hover:opacity-100 transition-opacity cursor-pointer"
             onClick={() => setShowModal(true)}>
          <div className="text-center p-4">
            <p className="text-lg font-semibold text-gray-800 mb-2">Data er der snart</p>
            <p className="text-sm text-gray-600 mb-3">Hjælp os med at få det endnu hurtigere udgivet</p>
            <button className="bg-green-600 text-white px-4 py-2 rounded-md hover:bg-green-700 transition-colors">
              Bidrag til open-source
            </button>
          </div>
        </div>
      </div>

      {/* Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg max-w-md w-full p-6">
            <div className="flex justify-between items-start mb-4">
              <h2 className="text-xl font-bold text-gray-900">{config.title}</h2>
              <button
                onClick={() => setShowModal(false)}
                className="text-gray-400 hover:text-gray-600"
              >
                <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>

            <p className="text-gray-600 mb-6">{config.description}</p>

            <div className="space-y-4">
              <p className="text-sm text-gray-700">
                Vi arbejder på at tilgængeliggøre disse data. Du kan hjælpe til ved at bidrage til vores open-source projekt på GitHub.
              </p>

              <div className="flex space-x-3">
                <a
                  href={config.githubUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex-1 bg-green-600 text-white text-center py-2 px-4 rounded-md hover:bg-green-700 transition-colors"
                >
                  Se GitHub issue
                </a>
                <button
                  onClick={() => setShowModal(false)}
                  className="flex-1 bg-gray-200 text-gray-800 py-2 px-4 rounded-md hover:bg-gray-300 transition-colors"
                >
                  Luk
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
