import type { NextRequest } from 'next/server';
import {
  GRADE_DEFINITIONS,
  getGradeHexColor,
  isPesticideGrade,
} from '@/lib/pesticide-score';
import type { PesticideGrade } from '@/components/pesticidkort/types';

const DEFAULT_GRADE: PesticideGrade = 'TOP_50';

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
  const rawGrade = searchParams.get('grade') ?? DEFAULT_GRADE;
  const grade: PesticideGrade = isPesticideGrade(rawGrade)
    ? rawGrade
    : DEFAULT_GRADE;
  const score = escapeHtml(searchParams.get('score') ?? '0');
  const fields = escapeHtml(searchParams.get('fields') ?? '0');
  const pfas = escapeHtml(searchParams.get('pfas') ?? '0');
  const dist = escapeHtml(searchParams.get('dist') ?? '0');

  const color = getGradeHexColor(grade);
  const { label, description } = GRADE_DEFINITIONS[grade];
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
  .grade-block { border-left: 6px solid ${color}; padding: 0.75rem 1rem;
    background: #f7f7f7; border-radius: 4px; margin-bottom: 2rem; }
  .grade-block h2 { font-size: 1.4rem; font-weight: 700; color: ${color};
    margin-bottom: 0.25rem; }
  .grade-block p.desc { color: #444; font-size: 0.95rem; margin-bottom: 0.25rem; }
  .grade-block p.score { color: #666; font-size: 0.85rem; }
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
    .grade-block { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
    .stat { background: none; border: 1px solid #ccc; }
  }
</style>
</head>
<body>
  <header>Pesticidkortet &mdash; Landbruget.dk</header>
  <h1>${addr}</h1>
  <p class="subtitle">Pesticideksponering for ${year}</p>
  <div class="grade-block">
    <h2>${escapeHtml(label)}</h2>
    <p class="desc">${escapeHtml(description)}</p>
    <p class="score">Lokal belastning: ${score} B/ha</p>
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
