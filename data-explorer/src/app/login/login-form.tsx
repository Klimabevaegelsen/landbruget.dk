'use client';

import { useState, useRef } from 'react';
import { useSearchParams } from 'next/navigation';
import { Database, Lock } from 'lucide-react';
import { cn } from '@/lib/utils';

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
        setError(data?.error === 'Server misconfigured' ? 'Serveren er ikke konfigureret korrekt.' : 'Forkert adgangskode');
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
    <div className="min-h-screen bg-background flex items-center justify-center px-4">
      <div className="w-full max-w-sm space-y-8">
        {/* Logo / branding */}
        <div className="text-center space-y-3">
          <div className="inline-flex items-center justify-center w-14 h-14 bg-primary/10 rounded-2xl">
            <Database className="h-7 w-7 text-primary" />
          </div>
          <div>
            <h1 className="text-2xl font-bold tracking-tight">Landbruget.dk</h1>
            <p className="text-sm text-muted-foreground mt-1">Data Explorer</p>
          </div>
        </div>

        {/* Card */}
        <div className="rounded-xl border bg-card shadow-sm p-8 space-y-6">
          <div className="space-y-1">
            <h2 className="text-lg font-semibold flex items-center gap-2">
              <Lock className="h-4 w-4" />
              Adgangskode påkrævet
            </h2>
            <p className="text-sm text-muted-foreground">
              Indtast adgangskoden for at fortsætte.
            </p>
          </div>

          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-1.5">
              <label htmlFor="password" className="text-sm font-medium">
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
                placeholder="••••••••"
                className={cn(
                  'w-full rounded-lg border bg-background px-3 py-2 text-sm',
                  'placeholder:text-muted-foreground',
                  'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1',
                  'transition-shadow',
                  error && 'border-destructive focus-visible:ring-destructive'
                )}
              />
              {error && (
                <p className="text-xs text-destructive font-medium">{error}</p>
              )}
            </div>

            <button
              type="submit"
              disabled={loading || !password}
              className={cn(
                'w-full rounded-lg px-4 py-2.5 text-sm font-semibold',
                'bg-primary text-primary-foreground',
                'hover:bg-primary/90 transition-colors',
                'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1',
                'disabled:opacity-50 disabled:cursor-not-allowed'
              )}
            >
              {loading ? 'Logger ind…' : 'Log ind'}
            </button>
          </form>
        </div>

        <p className="text-center text-xs text-muted-foreground">
          Del af{' '}
          <a
            href="https://landbruget.dk"
            target="_blank"
            rel="noopener noreferrer"
            className="font-medium text-foreground hover:text-primary transition-colors"
          >
            Landbruget.dk
          </a>
        </p>
      </div>
    </div>
  );
}
