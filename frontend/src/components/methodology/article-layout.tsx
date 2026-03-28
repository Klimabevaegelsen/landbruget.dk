import { type ReactNode } from 'react';
import { cn } from '@/lib/utils';

interface ArticleLayoutProps {
  children: ReactNode;
  className?: string;
}

export function ArticleLayout({ children, className }: ArticleLayoutProps) {
  return (
    <article
      className={cn(
        'mx-auto max-w-[720px] px-6 pb-32 pt-12 text-[17px] leading-[1.7] text-foreground',
        className
      )}
      data-testid="article-layout"
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
        'my-10 rounded border border-border bg-card p-5',
        className
      )}
      data-testid={`figure-${number}`}
    >
      {children}
      <figcaption className="text-muted-foreground mt-3 text-[13px] leading-snug">
        <strong className="text-foreground/70 font-semibold">
          Figur {number}.
        </strong>{' '}
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
      <sup className="text-muted-foreground cursor-help text-[11px] font-semibold">
        [{number}]
      </sup>
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
      className="border-border text-foreground mt-16 mb-4 border-b pb-3 text-2xl font-semibold tracking-tight"
    >
      <span className="text-muted-foreground mr-2">{number}</span>
      {title}
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
    <h3 id={id} className="text-foreground mt-10 mb-2 text-lg font-semibold">
      <span className="text-muted-foreground mr-2">{number}</span>
      {title}
    </h3>
  );
}
