'use client';

import { useState, useEffect, useRef } from 'react';
import { EditorView, basicSetup } from 'codemirror';
import { EditorState } from '@codemirror/state';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { Play, Loader2, FileCode } from 'lucide-react';
import { cn } from '@/lib/utils';

interface SQLEditorProps {
  initialQuery?: string;
  onExecute: (query: string) => void;
  isExecuting?: boolean;
  disabled?: boolean;
  className?: string;
}

export function SQLEditor({
  initialQuery = '',
  onExecute,
  isExecuting = false,
  disabled = false,
  className,
}: SQLEditorProps) {
  const editorRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const [query, setQuery] = useState(initialQuery);

  // Initialize CodeMirror
  useEffect(() => {
    if (!editorRef.current) return;

    // Create editor state
    const state = EditorState.create({
      doc: initialQuery,
      extensions: [
        basicSetup,
        sql(),
        oneDark,
        EditorView.updateListener.of(update => {
          if (update.docChanged) {
            setQuery(update.state.doc.toString());
          }
        }),
        EditorView.theme({
          '&': {
            height: '100%',
            fontSize: '14px',
          },
          '.cm-scroller': {
            fontFamily: '"JetBrains Mono", "Fira Code", Consolas, monospace',
          },
          '.cm-content': {
            minHeight: '200px',
          },
        }),
        EditorView.lineWrapping,
      ],
    });

    // Create editor view
    const view = new EditorView({
      state,
      parent: editorRef.current,
    });

    viewRef.current = view;

    // Cleanup
    return () => {
      view.destroy();
      viewRef.current = null;
    };
  }, []); // Only run once on mount

  // Update editor content when initialQuery changes
  useEffect(() => {
    if (viewRef.current && initialQuery !== query) {
      const transaction = viewRef.current.state.update({
        changes: {
          from: 0,
          to: viewRef.current.state.doc.length,
          insert: initialQuery,
        },
      });
      viewRef.current.dispatch(transaction);
      // Note: setQuery will be triggered by the editor's updateListener
    }
  }, [initialQuery, query]);

  function handleExecute() {
    if (query.trim() && !isExecuting && !disabled) {
      onExecute(query.trim());
    }
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    // Cmd/Ctrl + Enter to execute
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault();
      handleExecute();
    }
  }

  return (
    <div className={cn('flex flex-col gap-3', className)}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <FileCode className="h-4 w-4 text-primary" />
          <h3 className="text-sm font-semibold">SQL Query</h3>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-muted-foreground">
            Press <kbd className="px-1.5 py-0.5 bg-muted rounded border text-xs">Cmd</kbd> +{' '}
            <kbd className="px-1.5 py-0.5 bg-muted rounded border text-xs">Enter</kbd> to run
          </span>
        </div>
      </div>

      <div
        className={cn(
          'rounded-lg border overflow-hidden transition-all',
          disabled && 'opacity-50 pointer-events-none'
        )}
        onKeyDown={handleKeyDown}
      >
        <div ref={editorRef} className="min-h-[200px]" />
      </div>

      <div className="flex items-center gap-3">
        <button
          type="button"
          onClick={handleExecute}
          disabled={!query.trim() || isExecuting || disabled}
          className={cn(
            'inline-flex items-center justify-center gap-2',
            'px-4 py-2 rounded-lg font-semibold text-sm',
            'transition-all',
            'disabled:opacity-50 disabled:cursor-not-allowed',
            'bg-primary text-primary-foreground hover:bg-primary/90',
            'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2'
          )}
        >
          {isExecuting ? (
            <>
              <Loader2 className="h-4 w-4 animate-spin" />
              <span>Executing...</span>
            </>
          ) : (
            <>
              <Play className="h-4 w-4" />
              <span>Run Query</span>
            </>
          )}
        </button>

        {query.trim() && (
          <span className="text-sm text-muted-foreground">
            {query.split('\n').length} lines, {query.length} characters
          </span>
        )}
      </div>

      <div className="text-xs text-muted-foreground space-y-1">
        <p className="font-semibold">DuckDB SQL Dialect</p>
        <p>
          Example: <code className="bg-muted px-1.5 py-0.5 rounded">SELECT * FROM dataset_name LIMIT 100</code>
        </p>
      </div>
    </div>
  );
}
