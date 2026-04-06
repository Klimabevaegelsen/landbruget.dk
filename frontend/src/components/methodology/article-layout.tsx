import { type ReactNode } from 'react';
import { cn } from '@/lib/utils';

interface ArticleLayoutProps {
  children: ReactNode;
  className?: string;
  'data-testid'?: string;
}

export function ArticleLayout({
  children,
  className,
  'data-testid': testId = 'article-layout',
}: ArticleLayoutProps) {
  return (
    <article
      className={cn(
        'mx-auto max-w-[65ch] px-5 pb-32 pt-12 text-[16.5px] leading-[1.75] text-foreground selection:bg-primary/20 selection:text-foreground [&_p+p]:mt-6 [&_ol]:my-5 [&_ul]:my-5 [&_strong]:mr-[0.1em] [&_mark]:bg-primary/[0.12] [&_mark]:px-1 [&_mark]:py-0.5 [&_mark]:rounded-sm [&_mark]:text-foreground [&_code]:rounded [&_code]:bg-muted [&_code]:px-1.5 [&_code]:py-0.5 [&_code]:text-[0.9em] sm:px-6',
        className
      )}
      data-testid={testId}
    >
      {children}
    </article>
  );
}

interface FigureProps {
  id: string;
  number: number;
  caption: string;
  children: ReactNode;
  className?: string;
}

export function Figure({
  id,
  number,
  caption,
  children,
  className,
}: FigureProps) {
  return (
    <figure
      id={id}
      className={cn(
        'border-border bg-card my-12 rounded-lg border p-5',
        className
      )}
      data-testid={`figure-${number}`}
    >
      {children}
      <figcaption className="text-muted-foreground mt-5 border-t border-dashed pt-3 text-[13px] leading-normal">
        <span className="text-primary/60 font-semibold">
          Figur&nbsp;{number}.
        </span>{' '}
        {caption}
      </figcaption>
    </figure>
  );
}

interface SidenoteProps {
  number: number;
  children: ReactNode;
}

export function Sidenote({ number, children }: SidenoteProps) {
  return (
    <span className="group relative">
      <a
        href={`#ref-${number}`}
        className="text-primary/70 hover:text-primary cursor-pointer text-[11px] font-semibold no-underline transition-colors"
      >
        <sup>[{number}]</sup>
      </a>
      <span className="border-border bg-popover text-muted-foreground invisible absolute top-0 left-full z-10 ml-4 w-56 rounded border p-3 text-[13px] leading-snug shadow-sm group-hover:visible">
        {children}
      </span>
    </span>
  );
}

interface SectionHeaderProps {
  id: string;
  number: string;
  title: string;
}

export function SectionHeader({ id, number, title }: SectionHeaderProps) {
  return (
    <h2
      id={id}
      className="font-display text-foreground mt-24 mb-7 text-[28px] leading-tight font-semibold tracking-tight"
    >
      <span className="text-primary/50 mr-3 font-light tabular-nums">
        {number}
      </span>
      {title}
      <div className="border-border mt-4 border-b" />
    </h2>
  );
}

interface SubsectionHeaderProps {
  id: string;
  number: string;
  title: string;
}

export function SubsectionHeader({ id, number, title }: SubsectionHeaderProps) {
  return (
    <h3
      id={id}
      className="font-display text-foreground mt-14 mb-4 text-[21px] leading-snug font-semibold tracking-tight"
    >
      <span className="text-primary/40 mr-2 text-[15px] font-normal tabular-nums">
        {number}
      </span>
      {title}
    </h3>
  );
}
