import { Container } from '../container';

export function NavBanner() {
  return (
    <div className="w-full border-b border-yellow-200 bg-yellow-100 py-3">
      <Container>
        <div className="flex justify-center text-center">
          <p className="text-sm font-medium text-yellow-800">
            <span className="mr-2">🚜</span>
            <strong>Undskyld vi roder</strong>
            <span className="mx-2">•</span>
            Hjemmesiden er ved at blive gjort klar til lanceringen. Det betyder,
            at du vil opleve at noget data mangler eller er forkert, og at
            tingene ikke helt spiller. Vi tager imod feedback på{' '}
            <a
              href="https://github.com/Klimabevaegelsen/landbruget.dk/issues"
              target="_blank"
              rel="noopener noreferrer"
              className="font-semibold underline hover:no-underline"
            >
              Github
            </a>{' '}
            eller{' '}
            <a
              href="https://join.slack.com/t/landbrugetdk/shared_invite/zt-3bcf1whh0-mY6GqDGRhC0BuG3ADNLB2Q"
              target="_blank"
              rel="noopener noreferrer"
              className="font-semibold underline hover:no-underline"
            >
              Slack
            </a>{' '}
            med kyshånd <span className="ml-1">🫶</span>
          </p>
        </div>
      </Container>
    </div>
  );
}
