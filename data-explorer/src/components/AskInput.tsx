'use client';

import { useState } from 'react';
import { Loader2, AlertCircle } from 'lucide-react';
import { cn } from '@/lib/utils';

interface AskInputProps {
  onSqlGenerated: (
    sql: string,
    tableUrls?: Record<string, string>,
    meta?: { question: string; explanation: string }
  ) => void;
  disabled?: boolean;
  className?: string;
}

interface ApiResponse {
  sql: string;
  explanation: string;
  tables?: string[];
  tableUrls?: Record<string, string>;
  error?: string;
  details?: string;
}

const EXAMPLES = [
  'Top 10 farms by land area',
  'Most common crop types',
  'Farms in Region Midtjylland',
  'How many organic farms?',
];

export function AskInput({
  onSqlGenerated,
  disabled = false,
  className,
}: AskInputProps) {
  const [question, setQuestion] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastExplanation, setLastExplanation] = useState<string | null>(null);

  async function handleAsk() {
    if (!question.trim() || isLoading || disabled) return;

    setIsLoading(true);
    setError(null);
    setLastExplanation(null);

    try {
      const response = await fetch('/api/ask', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: question.trim() }),
      });

      const data: ApiResponse = await response.json();

      if (!response.ok) {
        const errorMessage = data.error || 'Failed to generate SQL query';
        const details = data.details ? ` — ${data.details}` : '';
        setError(`${errorMessage}${details}`);
        return;
      }

      setLastExplanation(data.explanation);
      onSqlGenerated(data.sql, data.tableUrls, {
        question: question.trim(),
        explanation: data.explanation,
      });
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : 'Network error. Please check your connection.'
      );
    } finally {
      setIsLoading(false);
    }
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleAsk();
    }
  }

  return (
    <div className={cn('', className)}>
      {/* Section label */}
      <div className="mb-3 flex items-center justify-between">
        <span
          className="text-[10px] font-semibold tracking-[0.2em] uppercase"
          style={{ color: 'var(--muted-foreground)' }}
        >
          Ask
        </span>
        {lastExplanation && (
          <button
            type="button"
            onClick={() => setLastExplanation(null)}
            className="text-[10px] tracking-wider uppercase transition-colors"
            style={{ color: 'var(--muted-foreground)' }}
          >
            Clear
          </button>
        )}
      </div>

      {/* Main input container — left border accent */}
      <div
        className={cn(
          'border-l-2 pl-4 transition-all',
          disabled && 'pointer-events-none opacity-40'
        )}
        style={{
          borderColor: question.trim() ? 'var(--primary)' : 'var(--border)',
        }}
      >
        <textarea
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          onKeyDown={handleKeyDown}
          disabled={disabled || isLoading}
          rows={3}
          placeholder="Ask a question about the data…"
          className="w-full resize-none bg-transparent text-base leading-relaxed outline-none placeholder:italic disabled:cursor-not-allowed"
          style={{
            fontFamily: 'var(--font-fraunces), Georgia, serif',
            color: 'var(--foreground)',
          }}
        />

        {/* Bottom bar */}
        <div className="flex items-center justify-between pt-2 pb-1">
          <div
            className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px]"
            style={{ color: 'var(--muted-foreground)' }}
          >
            <span className="text-[10px] tracking-wider uppercase opacity-60">
              Try:
            </span>
            {EXAMPLES.map((example) => (
              <button
                key={example}
                type="button"
                onClick={() => setQuestion(example)}
                disabled={isLoading || disabled}
                className="transition-colors hover:underline disabled:opacity-40"
                style={{ color: 'var(--muted-foreground)' }}
              >
                {example}
              </button>
            ))}
          </div>

          <button
            type="button"
            onClick={handleAsk}
            disabled={!question.trim() || isLoading || disabled}
            className={cn(
              'flex-shrink-0 ml-4 flex items-center gap-1.5 px-4 py-1.5',
              'text-[11px] font-semibold uppercase tracking-wider',
              'transition-all disabled:opacity-40 disabled:cursor-not-allowed'
            )}
            style={{
              background: 'var(--primary)',
              color: 'var(--primary-foreground)',
            }}
          >
            {isLoading ? (
              <>
                <Loader2 className="h-3 w-3 animate-spin" />
                <span>Thinking</span>
              </>
            ) : (
              <>
                <span>Ask</span>
                <span className="text-[13px] leading-none">→</span>
              </>
            )}
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div
          className="mt-3 flex items-start gap-2.5 border-l-2 pl-4"
          style={{ borderColor: 'var(--destructive)' }}
        >
          <AlertCircle
            className="mt-0.5 h-3.5 w-3.5 flex-shrink-0"
            style={{ color: 'var(--destructive)' }}
          />
          <div className="flex-1">
            <p
              className="text-xs leading-relaxed"
              style={{ color: 'var(--destructive)' }}
            >
              {error}
            </p>
          </div>
          <button
            type="button"
            onClick={() => setError(null)}
            className="text-xs leading-none transition-colors"
            style={{ color: 'var(--muted-foreground)' }}
          >
            ×
          </button>
        </div>
      )}

      {/* Explanation */}
      {lastExplanation && !error && (
        <div
          className="mt-3 border-l-2 pl-4"
          style={{ borderColor: 'var(--accent)' }}
        >
          <p
            className="text-xs leading-relaxed"
            style={{ color: 'var(--muted-foreground)' }}
          >
            {lastExplanation}
          </p>
        </div>
      )}
    </div>
  );
}
