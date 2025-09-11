'use client';

import { ResponsiveContainer } from 'recharts';

interface PlaceholderChartProps {
  dataType: 'nitrate' | 'carbon' | 'subsidies' | 'worker_welfare';
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
  worker_welfare: {
    title: 'Arbejdssikkerhed data',
    description: 'Data om arbejdsskader og sikkerhed på arbejdspladsen',
    githubUrl:
      'https://github.com/Klimabevaegelsen/landbruget.dk/issues/new?title=Worker%20Welfare%20Data%20Implementation&body=Please%20implement%20worker%20safety%20and%20injury%20data%20collection',
  },
};

export function PlaceholderChart({ dataType }: PlaceholderChartProps) {
  const config = DATA_TYPE_CONFIG[dataType];

  const handleClick = () => {
    window.open(config.githubUrl, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="relative">
      {/* Placeholder chart area */}
      <div
        style={{ width: '100%', height: 250, minHeight: 250, minWidth: 100 }}
        className="border-muted-foreground/25 bg-muted/50 mt-4 flex items-center justify-center rounded-lg border-2 border-dashed"
      >
        <ResponsiveContainer>
          <div className="flex h-full flex-col items-center justify-center p-6 text-center">
            <div className="bg-muted mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-lg">
              <svg
                className="text-muted-foreground h-10 w-10"
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
            <p className="text-muted-foreground mb-2 text-lg font-medium">
              Data kommer snart
            </p>
            <p className="text-muted-foreground/80 text-sm">
              Klik for at hjælpe til
            </p>
          </div>
        </ResponsiveContainer>
      </div>

      {/* Overlay with call-to-action */}
      <div className="bg-background/80 absolute inset-0 flex items-center justify-center rounded-lg opacity-0 backdrop-blur-sm transition-opacity hover:opacity-100">
        <div className="max-w-sm p-6 text-center">
          <p className="text-foreground mb-3 text-xl font-semibold">
            Data er der snart
          </p>
          <p className="text-muted-foreground mb-4 text-sm">
            Hjælp os med at få det endnu hurtigere udgivet
          </p>
          <button
            onClick={handleClick}
            className="cursor-pointer rounded-md bg-green-600 px-6 py-3 font-medium text-white transition-colors hover:bg-green-700"
          >
            Bidrag til{' '}
            <a
              href="https://github.com/klimabevaegelsen/landbruget.dk/"
              target="_blank"
              rel="noopener noreferrer"
              className="underline"
            >
              open-source
            </a>
          </button>
        </div>
      </div>
    </div>
  );
}
