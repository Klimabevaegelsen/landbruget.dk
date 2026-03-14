'use client';

import { useState, useRef } from 'react';
import { useSearchParams } from 'next/navigation';
import { cn } from '@/lib/utils';

const DISPLAY_FONT = 'var(--font-fraunces), Georgia, "Times New Roman", serif';
const EASE = 'var(--ease-out-quint)';

// Deterministic — no Math.random() to avoid hydration mismatch
function makeGridCells(count: number) {
  return Array.from({ length: count }, (_, i) => {
    const v = (i * 7919 + 3571) % 9999;
    const opacity = 0.02 + ((i * 17 + 3) % 60) / 3500;
    return { value: String(v).padStart(4, '0'), opacity };
  });
}

const GRID_CELLS = makeGridCells(560); // 14 cols × 40 rows

export default function LoginForm() {
  const searchParams = useSearchParams();
  const from = searchParams.get('from') || '/';

  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      const res = await fetch('/api/auth', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ password }),
      });

      if (res.ok) {
        const safeFrom = from.startsWith('/') ? from : '/';
        window.location.assign(safeFrom);
      } else {
        const data = await res.json().catch(() => null);
        setError(
          data?.error === 'Server misconfigured'
            ? 'Serveren er ikke konfigureret korrekt.'
            : 'Forkert adgangskode'
        );
        setPassword('');
        inputRef.current?.focus();
      }
    } catch {
      setError('Noget gik galt. Prøv igen.');
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="bg-background flex min-h-screen flex-col">
      {/* ── Nav ────────────────────────────────────────────────────── */}
      <nav
        className="motion-animate flex items-center gap-2.5 px-6 pt-8 sm:px-10 lg:px-16"
        style={{ animation: `fadeUp 0.5s ${EASE} 0s both` }}
      >
        <span className="text-foreground text-sm font-semibold tracking-tight">
          Landbruget.dk
        </span>
        <span className="text-border">·</span>
        <span className="text-muted-foreground text-sm">Data Explorer</span>
      </nav>

      {/* ── Main — split layout ─────────────────────────────────────── */}
      <div className="flex flex-1 lg:grid lg:grid-cols-5">
        {/* Left: form column — 2/5 width on desktop */}
        <div className="flex flex-1 items-center px-6 py-16 sm:px-10 lg:col-span-2 lg:px-16">
          <div className="w-full max-w-xs">
            <p
              className="motion-animate text-muted-foreground mb-6 text-xs font-semibold tracking-[0.2em] uppercase"
              style={{ animation: `fadeUp 0.6s ${EASE} 0.15s both` }}
            >
              Begrænset adgang
            </p>

            <h1
              className="motion-animate text-foreground mb-10 text-[clamp(2.25rem,5vw,3.5rem)] leading-[0.9] font-bold"
              style={{
                fontFamily: DISPLAY_FONT,
                animation: `fadeUp 0.8s ${EASE} 0.28s both`,
              }}
            >
              Indtast
              <br />
              adgangskode.
            </h1>

            <form
              onSubmit={handleSubmit}
              className="motion-animate space-y-4"
              style={{ animation: `fadeUp 0.6s ${EASE} 0.48s both` }}
            >
              <div className="space-y-2">
                <label
                  htmlFor="password"
                  className="text-foreground block text-xs font-semibold tracking-[0.15em] uppercase"
                >
                  Adgangskode
                </label>
                <input
                  ref={inputRef}
                  id="password"
                  type="password"
                  autoComplete="current-password"
                  autoFocus
                  required
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className={cn(
                    'w-full rounded-sm border bg-background px-4 py-3 text-sm',
                    'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1',
                    'transition-colors duration-150',
                    error
                      ? 'border-destructive focus-visible:ring-destructive'
                      : 'border-border hover:border-foreground/30'
                  )}
                />
                {error && (
                  <p
                    className="text-destructive text-xs font-medium"
                    role="alert"
                  >
                    {error}
                  </p>
                )}
              </div>

              <button
                type="submit"
                disabled={loading || !password}
                className={cn(
                  'w-full rounded-sm px-4 py-3 text-sm font-semibold',
                  'bg-primary text-primary-foreground',
                  'hover:bg-primary/90 transition-colors duration-150',
                  'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1',
                  'disabled:opacity-40 disabled:cursor-not-allowed',
                  'active:scale-[0.98]'
                )}
              >
                {loading ? (
                  <span className="inline-flex items-center justify-center gap-2">
                    <span
                      className="h-3.5 w-3.5 animate-spin rounded-full border-2 border-current border-t-transparent"
                      aria-hidden="true"
                    />
                    Logger ind…
                  </span>
                ) : (
                  'Log ind'
                )}
              </button>
            </form>

            <p
              className="motion-animate text-muted-foreground mt-10 text-xs"
              style={{ animation: `fadeUp 0.5s ${EASE} 0.65s both` }}
            >
              Del af{' '}
              <a
                href="https://landbruget.dk"
                target="_blank"
                rel="noopener noreferrer"
                className="text-foreground hover:text-primary font-semibold transition-colors"
              >
                Landbruget.dk
              </a>{' '}
              — åben dansk landbrugsdata
            </p>
          </div>
        </div>

        {/* Right: data-field texture — the data, visible but locked */}
        {/* Shows what's behind the access gate — creates meaning from the visual */}
        <div
          className="border-border relative hidden overflow-hidden border-l lg:col-span-3 lg:block"
          aria-hidden="true"
        >
          <div
            className="grid gap-x-3 gap-y-1.5 p-12 select-none"
            style={{ gridTemplateColumns: 'repeat(14, 1fr)' }}
          >
            {GRID_CELLS.map((cell, i) => (
              <span
                key={i}
                className="text-primary font-mono text-[9px]"
                style={{ opacity: cell.opacity }}
              >
                {cell.value}
              </span>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
