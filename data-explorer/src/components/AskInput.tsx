'use client';

import { useState } from 'react';
import { MessageSquare, Loader2, Sparkles, AlertCircle, CheckCircle } from 'lucide-react';
import { cn } from '@/lib/utils';

interface AskInputProps {
  onSqlGenerated: (sql: string, tableUrls?: Record<string, string>) => void;
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

export function AskInput({ onSqlGenerated, disabled = false, className }: AskInputProps) {
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

      onSqlGenerated(data.sql, data.tableUrls);
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
    <div className={cn('flex flex-col gap-4', className)}>
      {/* Header */}
      <div className="flex items-center gap-2">
        <Sparkles className="h-5 w-5 text-primary" />
        <h3 className="text-sm font-semibold">Ask in Natural Language</h3>
      </div>

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
        <div className="flex items-center justify-between px-4 py-3 border-t bg-muted/30">
          <div className="text-xs text-muted-foreground">
            Press <kbd className="px-1.5 py-0.5 bg-background rounded border text-xs">Enter</kbd> to
            ask,{' '}
            <kbd className="px-1.5 py-0.5 bg-background rounded border text-xs">Shift+Enter</kbd>{' '}
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
                <MessageSquare className="h-4 w-4" />
                <span>Ask</span>
              </>
            )}
          </button>
        </div>
      </div>

      {/* Error display */}
      {error && (
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 px-4 py-3">
          <div className="flex gap-3">
            <AlertCircle className="h-5 w-5 text-destructive flex-shrink-0 mt-0.5" />
            <div className="flex-1 space-y-1">
              <p className="text-sm font-semibold text-destructive">Error</p>
              <p className="text-sm text-destructive/90">{error}</p>
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
        <div className="rounded-lg border border-green-500/50 bg-green-500/10 px-4 py-3">
          <div className="flex gap-3">
            <CheckCircle className="h-5 w-5 text-green-600 flex-shrink-0 mt-0.5" />
            <div className="flex-1 space-y-2">
              <p className="text-sm font-semibold text-green-900 dark:text-green-100">
                SQL Generated
              </p>
              <p className="text-sm text-green-800 dark:text-green-200">{lastResult.explanation}</p>
              <div className="bg-green-950/20 rounded px-3 py-2 mt-2">
                <code className="text-xs text-green-900 dark:text-green-100 font-mono break-all">
                  {lastResult.sql}
                </code>
              </div>
              <p className="text-xs text-green-700 dark:text-green-300 mt-2">
                The SQL query has been loaded into the editor below. You can review and edit it
                before running.
              </p>
            </div>
            <button
              type="button"
              onClick={handleClearResults}
              className="text-green-600/70 hover:text-green-600 transition-colors"
            >
              <span className="sr-only">Dismiss</span>×
            </button>
          </div>
        </div>
      )}

      {/* Help text */}
      <div className="text-xs text-muted-foreground space-y-1">
        <p className="font-semibold">Example questions:</p>
        <ul className="list-disc list-inside space-y-0.5 ml-2">
          <li>Show me the top 10 farms by land area</li>
          <li>What are the most common crop types?</li>
          <li>List all farms in Region Midtjylland</li>
          <li>How many organic farms are there?</li>
        </ul>
      </div>
    </div>
  );
}
