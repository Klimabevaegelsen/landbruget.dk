'use client';

import { useState } from 'react';
import { ArrowRight, Loader2, AlertCircle, CheckCircle } from 'lucide-react';
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

export function AskInput({
  onSqlGenerated,
  disabled = false,
  className,
}: AskInputProps) {
  const [question, setQuestion] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastResult, setLastResult] = useState<{
    sql: string;
    explanation: string;
  } | null>(null);

  async function handleAsk() {
    if (!question.trim() || isLoading || disabled) {
      return;
    }

    setIsLoading(true);
    setError(null);
    setLastResult(null);

    try {
      const response = await fetch('/api/ask', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ question: question.trim() }),
      });

      const data: ApiResponse = await response.json();

      if (!response.ok) {
        // Handle error responses
        const errorMessage = data.error || 'Failed to generate SQL query';
        const details = data.details ? ` Details: ${data.details}` : '';
        setError(`${errorMessage}${details}`);
        return;
      }

      // Success - store result and pass SQL + table URLs to parent
      setLastResult({
        sql: data.sql,
        explanation: data.explanation,
      });

      onSqlGenerated(data.sql, data.tableUrls, {
        question: question.trim(),
        explanation: data.explanation,
      });
    } catch (err) {
      console.error('Error calling /api/ask:', err);
      setError(
        err instanceof Error
          ? err.message
          : 'Network error. Please check your connection and try again.'
      );
    } finally {
      setIsLoading(false);
    }
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    // Enter to submit (Shift+Enter for new line)
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleAsk();
    }
  }

  function handleClearResults() {
    setLastResult(null);
    setError(null);
  }

  return (
    <div className={cn('flex flex-col gap-3', className)}>
      {/* Input area */}
      <div
        className={cn(
          'rounded-lg border bg-card transition-all',
          disabled && 'opacity-50 pointer-events-none'
        )}
      >
        <textarea
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask a question about the data... (e.g., 'Show me the top 10 farms by area')"
          disabled={disabled || isLoading}
          rows={3}
          className={cn(
            'w-full px-4 py-3 bg-transparent resize-none',
            'placeholder:text-muted-foreground',
            'focus:outline-none focus-visible:ring-0',
            'disabled:cursor-not-allowed'
          )}
        />

        {/* Action buttons */}
        <div className="bg-muted/30 flex items-center justify-between border-t px-4 py-3">
          <div className="text-muted-foreground text-xs">
            Press{' '}
            <kbd className="bg-background rounded border px-1.5 py-0.5 text-xs">
              Enter
            </kbd>{' '}
            to ask,{' '}
            <kbd className="bg-background rounded border px-1.5 py-0.5 text-xs">
              Shift+Enter
            </kbd>{' '}
            for new line
          </div>
          <button
            type="button"
            onClick={handleAsk}
            disabled={!question.trim() || isLoading || disabled}
            className={cn(
              'inline-flex items-center justify-center gap-2',
              'px-4 py-2 rounded-lg font-semibold text-sm',
              'transition-all',
              'disabled:opacity-50 disabled:cursor-not-allowed',
              'bg-primary text-primary-foreground hover:bg-primary/90',
              'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2'
            )}
          >
            {isLoading ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                <span>Thinking...</span>
              </>
            ) : (
              <>
                <span>Ask</span>
                <ArrowRight className="h-4 w-4" />
              </>
            )}
          </button>
        </div>
      </div>

      {/* Error display */}
      {error && (
        <div className="border-destructive/50 bg-destructive/10 rounded-lg border px-4 py-3">
          <div className="flex gap-3">
            <AlertCircle className="text-destructive mt-0.5 h-5 w-5 flex-shrink-0" />
            <div className="flex-1 space-y-1">
              <p className="text-destructive text-sm font-semibold">Error</p>
              <p className="text-destructive/90 text-sm">{error}</p>
            </div>
            <button
              type="button"
              onClick={() => setError(null)}
              className="text-destructive/70 hover:text-destructive transition-colors"
            >
              <span className="sr-only">Dismiss</span>×
            </button>
          </div>
        </div>
      )}

      {/* Success result display */}
      {lastResult && (
        <div className="border-primary/20 bg-primary/5 rounded-md border px-4 py-3">
          <div className="flex gap-3">
            <CheckCircle className="text-primary mt-0.5 h-4 w-4 flex-shrink-0" />
            <p className="text-foreground flex-1 text-sm">
              {lastResult.explanation}
            </p>
            <button
              type="button"
              onClick={handleClearResults}
              className="text-muted-foreground hover:text-foreground leading-none transition-colors"
            >
              <span className="sr-only">Dismiss</span>×
            </button>
          </div>
        </div>
      )}

      {/* Example questions as clickable chips */}
      <div className="flex flex-wrap gap-2">
        {[
          'Top 10 farms by land area',
          'Most common crop types',
          'Farms in Region Midtjylland',
          'How many organic farms?',
        ].map((example) => (
          <button
            key={example}
            type="button"
            onClick={() => setQuestion(example)}
            disabled={isLoading || disabled}
            className="border-border bg-card text-muted-foreground hover:border-primary/40 hover:bg-primary/5 hover:text-foreground rounded-full border px-3 py-1 text-xs transition-colors disabled:opacity-40"
          >
            {example}
          </button>
        ))}
      </div>
    </div>
  );
}
