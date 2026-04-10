import type { NextRequest } from 'next/server';

type PesticideGrade = 'A' | 'B' | 'C' | 'D' | 'E';

const GRADE_COLORS: Record<PesticideGrade, string> = {
  A: '#2d9a3e',
  B: '#5a9e2f',
  C: '#c5a832',
  D: '#d4762c',
  E: '#c43030',
};

const GRADE_LABELS: Record<PesticideGrade, string> = {
  A: 'Laveste femtedel',
  B: 'Under gennemsnit',
  C: 'Omkring gennemsnit',
  D: 'Over gennemsnit',
  E: 'Højeste femtedel',
};

function isValidGrade(value: string): value is PesticideGrade {
  return ['A', 'B', 'C', 'D', 'E'].includes(value);
}

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

export function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;

  const addr = escapeHtml(searchParams.get('addr') ?? 'Ukendt adresse');
  const year = escapeHtml(searchParams.get('y') ?? '2024');
  const rawGrade = searchParams.get('grade') ?? 'C';
  const grade: PesticideGrade = isValidGrade(rawGrade) ? rawGrade : 'C';
  const score = escapeHtml(searchParams.get('score') ?? '0');
  const fields = escapeHtml(searchParams.get('fields') ?? '0');
  const pfas = escapeHtml(searchParams.get('pfas') ?? '0');
  const dist = escapeHtml(searchParams.get('dist') ?? '0');

  const color = GRADE_COLORS[grade];
  const label = GRADE_LABELS[grade];
  const now = new Date().toLocaleDateString('da-DK', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });

  const html = `<!DOCTYPE html>
<html lang="da">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pesticidrapport \u2014 ${addr}</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, -apple-system, sans-serif; max-width: 680px;
    margin: 3rem auto; padding: 0 1.5rem; color: #1a1a1a; line-height: 1.5; }
  header { font-size: 0.85rem; letter-spacing: 0.05em; text-transform: uppercase;
    color: #666; border-bottom: 2px solid #e5e5e5; padding-bottom: 0.75rem;
    margin-bottom: 2rem; }
  h1 { font-size: 1.6rem; font-weight: 700; margin-bottom: 0.25rem; }
  .subtitle { color: #555; margin-bottom: 2rem; }
  .grade-row { display: flex; align-items: center; gap: 1.25rem;
    margin-bottom: 2rem; }
  .grade-badge { width: 72px; height: 72px; border-radius: 12px;
    display: flex; align-items: center; justify-content: center;
    font-size: 2rem; font-weight: 800; color: #fff;
    background: ${color}; }
  .grade-detail h2 { font-size: 1.15rem; font-weight: 600; }
  .grade-detail p { color: #555; font-size: 0.95rem; }
  .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem;
    margin-bottom: 2.5rem; }
  .stat { background: #f7f7f7; border-radius: 8px; padding: 1rem;
    text-align: center; }
  .stat-value { font-size: 1.5rem; font-weight: 700; }
  .stat-label { font-size: 0.8rem; color: #666; margin-top: 0.25rem; }
  footer { border-top: 1px solid #e5e5e5; padding-top: 1rem;
    font-size: 0.75rem; color: #888; }
  footer p { margin-bottom: 0.25rem; }
  @media print {
    body { margin: 1cm auto; }
    .grade-badge { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
    .stat { background: none; border: 1px solid #ccc; }
  }
</style>
</head>
<body>
  <header>Pesticidkortet &mdash; Landbruget.dk</header>
  <h1>${addr}</h1>
  <p class="subtitle">Pesticidbelastning for ${year}</p>
  <div class="grade-row">
    <div class="grade-badge">${grade}</div>
    <div class="grade-detail">
      <h2>${label}</h2>
      <p>Samlet score: ${score} B/ha</p>
    </div>
  </div>
  <div class="stats">
    <div class="stat">
      <div class="stat-value">${fields}</div>
      <div class="stat-label">Marker i n\u00e6rheden</div>
    </div>
    <div class="stat">
      <div class="stat-value">${pfas}</div>
      <div class="stat-label">Med PFAS-pesticider</div>
    </div>
    <div class="stat">
      <div class="stat-value">${dist} m</div>
      <div class="stat-label">N\u00e6rmeste mark</div>
    </div>
  </div>
  <footer>
    <p>Data: Milj\u00f8styrelsen (BPS), Landbrugsstyrelsen (FVM). Genereret ${now}.</p>
    <p>Rapporten er vejledende. Brug Cmd+P / Ctrl+P for at gemme som PDF.</p>
  </footer>
</body>
</html>`;

  return new Response(html, {
    headers: {
      'Content-Type': 'text/html; charset=utf-8',
      'Content-Disposition': 'inline',
    },
  });
}
