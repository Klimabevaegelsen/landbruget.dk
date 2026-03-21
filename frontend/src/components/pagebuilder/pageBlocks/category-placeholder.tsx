'use client';

import { ResponsiveContainer } from 'recharts';

export function CategoryPlaceholder() {
  const handleClick = () => {
    window.open(
      'https://github.com/klimabevaegelsen/landbruget.dk/issues',
      '_blank',
      'noopener,noreferrer'
    );
  };

  return (
    <div className="relative">
      {/* Placeholder area for entire category */}
      <div className="border-muted-foreground/25 bg-muted/50 mt-4 flex h-[300px] min-h-[300px] w-full min-w-[100px] items-center justify-center rounded-lg border-2 border-dashed">
        <ResponsiveContainer>
          <div className="flex h-full flex-col items-center justify-center p-8 text-center">
            <div className="bg-muted mx-auto mb-6 flex h-20 w-20 items-center justify-center rounded-lg">
              <svg
                className="text-muted-foreground h-12 w-12"
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
            <p className="text-muted-foreground mb-3 text-xl font-medium">
              Der er ingen data tilgængelig i denne kategori for virksomheden
            </p>
            <p className="text-muted-foreground/80 mb-4 max-w-md text-sm">
              Denne sektion indeholder flere diagrammer og tabeller, men der er
              desværre ingen data at vise for denne virksomhed.
            </p>
            <p className="text-muted-foreground/80 text-sm">
              Bidrag med nye datakilder her
            </p>
          </div>
        </ResponsiveContainer>
      </div>

      {/* Overlay with call-to-action */}
      <div className="bg-background/80 absolute inset-0 flex items-center justify-center rounded-lg opacity-0 backdrop-blur-sm transition-opacity hover:opacity-100">
        <button
          onClick={handleClick}
          data-testid="contribute-category-data-button"
          className="bg-primary hover:bg-primary/90 text-primary-foreground flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-medium shadow-lg transition-colors focus:ring-2 focus:ring-offset-2 focus:outline-none"
        >
          <svg
            className="h-4 w-4"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M12 6v6m0 0v6m0-6h6m-6 0H6"
            />
          </svg>
          Hjælp til med data
        </button>
      </div>
    </div>
  );
}
