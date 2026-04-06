'use client';

import { cn } from '@/lib/utils';

interface CodeBlockProps {
  code: string;
  language?: string;
  title?: string;
  className?: string;
}

export function CodeBlock({
  code,
  language = 'python',
  title,
  className,
}: CodeBlockProps) {
  return (
    <div
      className={cn(
        'border-primary/20 my-4 overflow-hidden rounded border',
        className
      )}
      data-testid="code-block"
    >
      {title && (
        <div className="border-primary/20 bg-primary/[0.04] border-b px-4 py-1.5">
          <span className="text-primary/60 font-mono text-xs">{title}</span>
        </div>
      )}
      <pre className="bg-card overflow-x-auto p-4">
        <code
          className={cn(
            'font-mono text-[14px] leading-relaxed text-foreground/80',
            `language-${language}`
          )}
        >
          {code}
        </code>
      </pre>
    </div>
  );
}
