"use client";

import { useState, useEffect, useRef } from "react";
import { EditorView, basicSetup } from "codemirror";
import { EditorState } from "@codemirror/state";
import { sql } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import { Loader2 } from "lucide-react";
import { cn } from "@/lib/utils";

interface SQLEditorProps {
  initialQuery?: string;
  onExecute: (query: string) => void;
  isExecuting?: boolean;
  disabled?: boolean;
  className?: string;
}

export function SQLEditor({
  initialQuery = "",
  onExecute,
  isExecuting = false,
  disabled = false,
  className,
}: SQLEditorProps) {
  const editorRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const [query, setQuery] = useState(initialQuery);

  useEffect(() => {
    if (!editorRef.current) return;

    const state = EditorState.create({
      doc: initialQuery,
      extensions: [
        basicSetup,
        sql(),
        oneDark,
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            setQuery(update.state.doc.toString());
          }
        }),
        EditorView.theme({
          "&": {
            height: "100%",
            fontSize: "13px",
          },
          ".cm-scroller": {
            fontFamily: '"JetBrains Mono", "Fira Code", Consolas, monospace',
          },
          ".cm-content": {
            minHeight: "160px",
          },
          ".cm-gutters": {
            borderRight: "1px solid rgba(255,255,255,0.06)",
          },
        }),
        EditorView.lineWrapping,
      ],
    });

    const view = new EditorView({
      state,
      parent: editorRef.current,
    });

    viewRef.current = view;
    return () => {
      view.destroy();
      viewRef.current = null;
    };
  }, []);

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
    }
  }, [initialQuery, query]);

  function handleExecute() {
    if (query.trim() && !isExecuting && !disabled) {
      onExecute(query.trim());
    }
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault();
      handleExecute();
    }
  }

  const lineCount = query ? query.split("\n").length : 0;

  return (
    <div className={cn("", className)}>
      {/* Section label row */}
      <div className="mb-3 flex items-center justify-between">
        <span
          className="text-[10px] font-semibold tracking-[0.2em] uppercase"
          style={{ color: "var(--muted-foreground)" }}
        >
          SQL
        </span>
        <span
          className="text-[10px]"
          style={{
            fontFamily: "var(--font-geist-mono)",
            color: "var(--muted-foreground)",
          }}
        >
          ⌘↵ to run
        </span>
      </div>

      {/* Editor wrapper */}
      <div
        className={cn(
          "overflow-hidden border transition-all",
          disabled && "pointer-events-none opacity-40",
        )}
        style={{ borderColor: "var(--border)" }}
        onKeyDown={handleKeyDown}
      >
        <div ref={editorRef} />
      </div>

      {/* Run bar */}
      <div className="mt-3 flex items-center gap-4">
        <button
          type="button"
          onClick={handleExecute}
          disabled={!query.trim() || isExecuting || disabled}
          className={cn(
            "flex items-center gap-2 px-5 py-2",
            "text-[11px] font-semibold uppercase tracking-wider",
            "transition-all disabled:opacity-40 disabled:cursor-not-allowed",
          )}
          style={{
            background: "var(--primary)",
            color: "var(--primary-foreground)",
          }}
        >
          {isExecuting ? (
            <>
              <Loader2 className="h-3 w-3 animate-spin" />
              <span>Running</span>
            </>
          ) : (
            <>
              <span className="text-[13px] leading-none">▷</span>
              <span>Run</span>
            </>
          )}
        </button>

        {query.trim() && (
          <span
            className="text-[10px] tabular-nums"
            style={{
              fontFamily: "var(--font-geist-mono)",
              color: "var(--muted-foreground)",
            }}
          >
            {lineCount} {lineCount === 1 ? "line" : "lines"} · {query.length} chars
          </span>
        )}
      </div>
    </div>
  );
}
