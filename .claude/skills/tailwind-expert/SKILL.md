---
name: tailwind-expert
description: Tailwind CSS v4 patterns for Next.js — configuration, utilities, theming, and dark mode
---

# Tailwind CSS v4 Expert

> Version: Tailwind CSS 4.x with `@tailwindcss/postcss`. This project uses `tailwindcss@^4`.

## Key Changes from v3

| v3 | v4 |
|----|----|
| `tailwindcss` in postcss | `@tailwindcss/postcss` plugin |
| `@tailwind base/components/utilities` | `@import "tailwindcss"` |
| `tailwind.config.ts` | CSS-first config via `@theme` |
| `theme.extend` in JS | `@theme { --color-brand: ... }` in CSS |
| `darkMode: 'class'` config | `@variant dark (...)` in CSS |

## PostCSS Config

```javascript
// postcss.config.mjs
const config = {
  plugins: {
    "@tailwindcss/postcss": {},
  },
}
export default config
```

## CSS Entry Point

```css
/* app/globals.css */
@import "tailwindcss";

/* Custom theme tokens */
@theme {
  --color-brand: oklch(0.55 0.2 145);
  --color-brand-foreground: oklch(0.98 0 0);
  --font-sans: 'Inter', sans-serif;
  --radius: 0.5rem;
}
```

## Utility Patterns

```tsx
// Responsive
<div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4" />

// Dark mode (automatic via prefers-color-scheme, or class-based)
<div className="bg-white dark:bg-zinc-900 text-zinc-900 dark:text-zinc-50" />

// Group hover
<div className="group">
  <span className="opacity-0 group-hover:opacity-100 transition-opacity" />
</div>

// Arbitrary values
<div className="w-[372px] top-[117px]" />

// CSS variable utilities
<div className="text-[var(--color-brand)]" />
```

## Theming with shadcn/ui (CSS variables)

```css
/* globals.css */
@import "tailwindcss";

@theme {
  --background: oklch(1 0 0);
  --foreground: oklch(0.145 0 0);
  --primary: oklch(0.205 0 0);
  --primary-foreground: oklch(0.985 0 0);
  --muted: oklch(0.97 0 0);
  --muted-foreground: oklch(0.556 0 0);
  --border: oklch(0.922 0 0);
  --radius: 0.625rem;
}

@media (prefers-color-scheme: dark) {
  @theme {
    --background: oklch(0.145 0 0);
    --foreground: oklch(0.985 0 0);
  }
}
```

## Common Utility Classes

```
Layout:     flex items-center justify-between gap-4
            grid grid-cols-12 col-span-4
            container mx-auto px-4

Spacing:    p-4 px-6 py-3 m-0 mt-8 space-y-4 gap-6

Typography: text-sm text-base text-lg text-xl text-2xl
            font-medium font-semibold font-bold
            text-muted-foreground leading-relaxed tracking-tight

Borders:    border border-border rounded-lg rounded-full
            ring-1 ring-border ring-offset-2

Shadows:    shadow-sm shadow shadow-lg shadow-xl

Sizing:     w-full h-full max-w-screen-lg min-h-screen
            w-10 h-10 size-4 (shorthand for w-4 h-4)

Interactivity: hover:bg-accent focus-visible:ring-2
               disabled:opacity-50 disabled:cursor-not-allowed
               transition-colors duration-200 ease-in-out
               cursor-pointer select-none

Overflow:   overflow-hidden truncate line-clamp-2 whitespace-nowrap
```

## Rules

- Use `@import "tailwindcss"` not the old three-line directive
- Configure via `@theme {}` in CSS, not `tailwind.config.ts`
- Use `postcss.config.mjs` with `"@tailwindcss/postcss"` (not `tailwindcss`)
- `size-{n}` is the v4 shorthand for `w-{n} h-{n}`
- `oklch()` color space is preferred for theme tokens (perceptually uniform)
- Arbitrary values with `[]` when you need exact pixel values
