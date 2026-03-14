'use client';

import { useState, useEffect, useRef } from 'react';
import Link from 'next/link';
import { ArrowUpRight } from 'lucide-react';
import type { ManifestData } from '@/types';

const DISPLAY_FONT = 'var(--font-fraunces), Georgia, "Times New Roman", serif';
const EASE = 'var(--ease-out-quint)';

const DATASET_NAMES = [
  'Markblok',
  'CHR Register',
  'Husdyrhold',
  'Afgrødekort',
  'Pesticidkontrol',
  'Jordstykker',
  'CVR Landbrug',
  'Gødningsregnskab',
  'Vandmiljøplan',
  'Naturarealer',
  'Skovregistret',
  'Ejendomsvurdering',
  'Svineflytninger',
  'Kvægtælling',
  'Sprøjtejournaler',
  'Markplan',
  'Nitratklasser',
  'Dyrevelfærd',
];

// Deterministic grid — no Math.random() to avoid hydration mismatch
function makeGridCells(count: number) {
  return Array.from({ length: count }, (_, i) => {
    const v = (i * 7919 + 3571) % 9999;
    const opacity = 0.035 + ((i * 17 + 3) % 60) / 2000;
    return { value: String(v).padStart(4, '0'), opacity };
  });
}

const GRID_CELLS = makeGridCells(120);

/** One-shot intersection observer — fires `true` once element enters viewport */
function useInView(threshold = 0.15) {
  const ref = useRef<HTMLElement>(null);
  const [visible, setVisible] = useState(false);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const obs = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) setVisible(true);
      },
      { threshold }
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, [threshold]);
  return [ref, visible] as const;
}

export default function HomePage() {
  const [stats, setStats] = useState<{
    datasets: number;
    totalRows: number;
    totalSize: number;
  } | null>(null);

  const [statsRef, statsVisible] = useInView(0.2);
  const [featuresRef, featuresVisible] = useInView(0.1);
  const [ctaRef, ctaVisible] = useInView(0.2);

  const R2_BASE_URL =
    process.env.NEXT_PUBLIC_R2_BASE_URL || 'https://your-r2-bucket.r2.dev';

  useEffect(() => {
    async function loadStats() {
      try {
        const response = await fetch(`${R2_BASE_URL}/manifest.json`);
        if (!response.ok) return;
        const manifest: ManifestData = await response.json();
        setStats({
          datasets: manifest.datasets.length,
          totalRows: manifest.datasets.reduce((s, d) => s + d.rowCount, 0),
          totalSize: manifest.datasets.reduce((s, d) => s + d.sizeBytes, 0),
        });
      } catch {
        // Stats are decorative — fail silently
      }
    }
    loadStats();
  }, [R2_BASE_URL]);

  function fmtRows(n: number) {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K`;
    return String(n);
  }

  function fmtSize(bytes: number) {
    if (bytes >= 1_073_741_824)
      return `${(bytes / 1_073_741_824).toFixed(1)} GB`;
    if (bytes >= 1_048_576) return `${(bytes / 1_048_576).toFixed(0)} MB`;
    return `${bytes} B`;
  }

  return (
    <div className="bg-background min-h-screen">
      {/* ── Navigation ─────────────────────────────────────────────── */}
      <nav
        className="motion-animate flex items-center justify-between px-6 pt-8 sm:px-10 lg:px-16"
        style={{ animation: `fadeUp 0.5s ${EASE} 0s both` }}
      >
        <div className="flex items-center gap-2.5">
          <span className="text-foreground text-sm font-semibold tracking-tight">
            Landbruget.dk
          </span>
          <span className="text-border">·</span>
          <span className="text-muted-foreground text-sm">Data Explorer</span>
        </div>
        <Link
          href="/explore"
          className="border-primary/30 text-primary hover:bg-primary hover:text-primary-foreground rounded-full border px-4 py-1.5 text-xs font-semibold transition-all duration-200 active:scale-95"
        >
          Open Explorer
        </Link>
      </nav>

      {/* ── Hero ───────────────────────────────────────────────────── */}
      <section className="px-6 pt-16 sm:px-10 lg:px-16">
        <div className="grid grid-cols-1 items-start gap-12 lg:grid-cols-5 lg:gap-6">
          {/* Left: Editorial headline — staggered entrance */}
          <div className="lg:col-span-3">
            <p
              className="motion-animate text-muted-foreground mb-8 text-xs font-semibold tracking-[0.2em] uppercase"
              style={{ animation: `fadeUp 0.6s ${EASE} 0.15s both` }}
            >
              Danish Agricultural Transparency
            </p>

            <h1
              className="motion-animate text-foreground mb-8 text-[clamp(3.25rem,8vw,6.5rem)] leading-[0.9] font-bold"
              style={{
                fontFamily: DISPLAY_FONT,
                animation: `fadeUp 0.9s ${EASE} 0.3s both`,
              }}
            >
              Agricultural
              <br />
              data,
              <br />
              <em className="text-primary not-italic">open.</em>
            </h1>

            <p
              className="motion-animate text-muted-foreground mb-10 max-w-[420px] text-base leading-relaxed"
              style={{ animation: `fadeUp 0.6s ${EASE} 0.55s both` }}
            >
              Query, analyze, and download data from 18+ Danish agricultural
              registries — directly in your browser. No account required.
            </p>

            <div
              className="motion-animate flex items-center gap-6"
              style={{ animation: `fadeUp 0.6s ${EASE} 0.7s both` }}
            >
              <Link
                href="/explore"
                className="group bg-primary text-primary-foreground hover:bg-primary/90 inline-flex items-center gap-2 rounded-sm px-6 py-3 text-sm font-semibold transition-colors active:scale-[0.97]"
              >
                Start Exploring
                <ArrowUpRight className="h-4 w-4 transition-transform duration-200 group-hover:translate-x-0.5 group-hover:-translate-y-0.5" />
              </Link>
              <a
                href="https://landbruget.dk"
                target="_blank"
                rel="noopener noreferrer"
                className="text-muted-foreground decoration-border hover:text-foreground text-sm font-semibold underline underline-offset-4 transition-colors"
              >
                About the project
              </a>
            </div>
          </div>

          {/* Right: Data-field texture — fades in alongside copy */}
          <div
            className="motion-animate hidden lg:col-span-2 lg:flex lg:justify-end"
            style={{ animation: `fadeUp 1s ${EASE} 0.45s both` }}
          >
            <DataFieldVisual />
          </div>
        </div>
      </section>

      {/* ── Dataset ticker ─────────────────────────────────────────── */}
      <div
        className="motion-animate border-border mt-16 overflow-hidden border-t border-b"
        style={{ animation: `fadeUp 0.5s ${EASE} 0.85s both` }}
      >
        <div
          className="text-muted-foreground flex py-3 text-[10px] font-semibold tracking-[0.15em] whitespace-nowrap uppercase"
          style={{
            animation: 'marquee 45s linear infinite',
            willChange: 'transform',
          }}
          aria-hidden="true"
        >
          {[...DATASET_NAMES, ...DATASET_NAMES].map((name, i) => (
            <span key={i} className="inline-flex items-center gap-5 px-5">
              <span>{name}</span>
              <span className="opacity-30">—</span>
            </span>
          ))}
        </div>
      </div>

      {/* ── Stats strip ────────────────────────────────────────────── */}
      {/* ref on section for IntersectionObserver; each StatItem animates independently */}
      <section ref={statsRef} className="px-6 py-16 sm:px-10 lg:px-16">
        <div className="flex max-w-2xl flex-col gap-10 sm:flex-row sm:items-start sm:gap-0">
          <StatItem
            value={stats ? `${stats.datasets}` : '—'}
            suffix={stats ? '+' : ''}
            label="Datasets available"
            visible={statsVisible}
            delay={0}
          />
          <div className="border-border hidden self-stretch border-r sm:mx-10 sm:block lg:mx-14" />
          <div className="bg-border h-px sm:hidden" />
          <StatItem
            value={stats ? fmtRows(stats.totalRows) : '—'}
            label="Total rows of data"
            visible={statsVisible}
            delay={150}
          />
          <div className="border-border hidden self-stretch border-r sm:mx-10 sm:block lg:mx-14" />
          <div className="bg-border h-px sm:hidden" />
          <StatItem
            value={stats ? fmtSize(stats.totalSize) : '—'}
            label="Compressed Parquet"
            visible={statsVisible}
            delay={300}
          />
        </div>
      </section>

      {/* ── Thin rule ──────────────────────────────────────────────── */}
      <div className="px-6 sm:px-10 lg:px-16">
        <div className="bg-border h-px" />
      </div>

      {/* ── Features ───────────────────────────────────────────────── */}
      <section ref={featuresRef} className="px-6 py-16 sm:px-10 lg:px-16">
        <div
          className="motion-animate mb-12"
          style={
            featuresVisible
              ? { animation: `fadeUp 0.6s ${EASE} 0ms both` }
              : { opacity: 0 }
          }
        >
          <h2
            className="text-foreground text-[clamp(1.5rem,3vw,2.25rem)] leading-tight font-bold"
            style={{ fontFamily: DISPLAY_FONT }}
          >
            What you can do
          </h2>
        </div>

        <div className="grid max-w-4xl grid-cols-1 gap-x-20 gap-y-10 md:grid-cols-2">
          <FeatureItem
            number="01"
            title="Ask in plain language"
            description="Type a question, get a SQL query. AI translates your intent across all available datasets — no SQL knowledge required."
            visible={featuresVisible}
            delay={80}
          />
          <FeatureItem
            number="02"
            title="Runs entirely in your browser"
            description="DuckDB-WASM processes millions of rows client-side. No data leaves your machine, no server roundtrips."
            visible={featuresVisible}
            delay={180}
          />
          <FeatureItem
            number="03"
            title="Join across registries"
            description="All datasets connect on CVR, CHR, and BFE identifiers — company, herd, and cadastral IDs link the full agricultural picture."
            visible={featuresVisible}
            delay={280}
          />
          <FeatureItem
            number="04"
            title="Export and take it further"
            description="Download query results as CSV. Bring them into Python, R, Excel, or any analysis tool of your choosing."
            visible={featuresVisible}
            delay={380}
          />
        </div>
      </section>

      {/* ── CTA ────────────────────────────────────────────────────── */}
      <section
        ref={ctaRef}
        className="border-border border-t px-6 py-16 sm:px-10 lg:px-16"
      >
        <div
          className="motion-animate flex flex-col gap-8 sm:flex-row sm:items-end sm:justify-between"
          style={
            ctaVisible
              ? { animation: `fadeUp 0.7s ${EASE} 0ms both` }
              : { opacity: 0 }
          }
        >
          <div>
            <h2
              className="text-foreground mb-3 text-[clamp(2rem,5vw,3.75rem)] leading-[0.92] font-bold"
              style={{ fontFamily: DISPLAY_FONT }}
            >
              Ready to explore
              <br />
              the data?
            </h2>
            <p className="text-muted-foreground text-sm">
              Updates weekly. All datasets under open data licenses.
            </p>
          </div>

          <Link
            href="/explore"
            className="group bg-primary text-primary-foreground hover:bg-primary/90 inline-flex shrink-0 items-center gap-3 self-start rounded-sm px-7 py-4 text-sm font-semibold transition-colors active:scale-[0.97] sm:self-auto"
          >
            Launch Explorer
            <ArrowUpRight className="h-4 w-4 transition-transform duration-200 group-hover:translate-x-0.5 group-hover:-translate-y-0.5" />
          </Link>
        </div>
      </section>

      {/* ── Footer ─────────────────────────────────────────────────── */}
      <footer className="border-border border-t px-6 py-8 sm:px-10 lg:px-16">
        <div className="text-muted-foreground flex flex-col gap-2 text-xs sm:flex-row sm:items-center sm:justify-between">
          <span>
            Part of{' '}
            <a
              href="https://landbruget.dk"
              target="_blank"
              rel="noopener noreferrer"
              className="text-foreground hover:text-primary font-semibold transition-colors"
            >
              Landbruget.dk
            </a>{' '}
            — Organizing Danish agricultural information
          </span>
          <span>DuckDB-WASM · Gemini AI · Open Data</span>
        </div>
      </footer>
    </div>
  );
}

function DataFieldVisual() {
  return (
    <div className="relative w-full max-w-[320px]" aria-hidden="true">
      {/* Faint grid of data numbers — the "field" texture */}
      <div
        className="grid gap-x-2 gap-y-1 select-none"
        style={{ gridTemplateColumns: 'repeat(10, 1fr)' }}
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

      {/* Ghost number: very slow breathe — like a pulse of data */}
      <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
        <span
          className="text-primary text-[clamp(5rem,12vw,8rem)] leading-none font-bold select-none"
          style={{
            fontFamily: DISPLAY_FONT,
            opacity: 0.1,
            animation: 'breathe 7s ease-in-out infinite',
          }}
        >
          18+
        </span>
      </div>
    </div>
  );
}

function StatItem({
  value,
  label,
  suffix = '',
  visible = true,
  delay = 0,
}: {
  value: string;
  label: string;
  suffix?: string;
  visible?: boolean;
  delay?: number;
}) {
  return (
    <div className="flex flex-col gap-2">
      {/* Number blurs into focus — "data resolving" */}
      <div
        className="motion-animate text-foreground text-[clamp(2.75rem,6vw,4rem)] leading-none font-bold"
        style={{
          fontFamily: DISPLAY_FONT,
          ...(visible
            ? {
                animation: `blurReveal 0.9s var(--ease-out-quint) ${delay}ms both`,
              }
            : { opacity: 0 }),
        }}
      >
        {value}
        {suffix && <span className="text-primary">{suffix}</span>}
      </div>
      <p className="text-muted-foreground text-[10px] font-semibold tracking-[0.15em] uppercase">
        {label}
      </p>
    </div>
  );
}

function FeatureItem({
  number,
  title,
  description,
  visible = true,
  delay = 0,
}: {
  number: string;
  title: string;
  description: string;
  visible?: boolean;
  delay?: number;
}) {
  return (
    <div
      className="motion-animate flex gap-5"
      style={
        visible
          ? {
              animation: `fadeUp 0.6s var(--ease-out-quint) ${delay}ms both`,
            }
          : { opacity: 0 }
      }
    >
      <span className="text-muted-foreground/40 mt-0.5 shrink-0 pt-px font-mono text-xs font-bold">
        {number}
      </span>
      <div>
        <h3 className="text-foreground mb-2 text-sm font-semibold">{title}</h3>
        <p className="text-muted-foreground text-sm leading-relaxed">
          {description}
        </p>
      </div>
    </div>
  );
}
