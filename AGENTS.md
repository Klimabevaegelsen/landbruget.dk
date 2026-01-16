# Agent UI Development Guidelines

This file instructs AI agents on how to build high-quality, accessible UI for this project.

## Stack Requirements

- **Framework**: Next.js 15 with React 19
- **Styling**: Tailwind CSS (use defaults unless custom values exist)
- **Animation**: `motion/react` for JavaScript animations, `tw-animate-css` for entrance effects
- **Class utilities**: Use `cn` utility combining `clsx` and `tailwind-merge`
- **Components**: Leverage existing project components before building new ones

## Accessibility (WCAG 2.1) - Non-Negotiable

### Critical Requirements
- All images must have `alt` text
- All icon-only buttons must have `aria-label` attributes
- All form inputs must have associated `<label>` elements
- Use semantic HTML elements (`<button>`, `<a>`, `<nav>`, etc.)
- Links must have `href` attributes
- Maintain hierarchical headings (`<h1>` through `<h6>`)
- Include "Skip to content" link

### Focus & Keyboard
- All flows must be keyboard-operable (WAI-ARIA patterns)
- Every focusable element needs visible focus ring (use `:focus-visible`)
- Never remove focus outlines without providing replacements
- Implement focus traps per WAI-ARIA patterns
- Don't manually implement keyboard/focus behavior - use primitives

### Color & Contrast
- Minimum 4.5:1 contrast ratio for text
- Never convey information through color alone - include text labels
- Use color-blind-friendly palettes for charts
- Increase contrast for `:hover`, `:active`, `:focus` states

### Touch & Hit Targets
- Visual targets under 24px should expand to ≥24px (44px on mobile)
- Set `touch-action: manipulation` to prevent double-tap zoom
- Remove dead zones - interactive-looking elements must be interactive

## Component Architecture

### Primitive Selection
- Prioritize accessible primitives (`Radix`, `React Aria`, `Base UI`)
- Don't mix primitive systems within the same interaction surface
- Prefer native HTML elements before applying ARIA attributes

### State Variations
All interactive elements need complete states:
- Default
- Hover
- Focus
- Active
- Disabled
- Loading (where applicable)

## Interaction Patterns

### Destructive Actions
- Must use `AlertDialog` component
- Require confirmation or provide undo capability
- Include safe time window for recovery

### Loading States
- Display structural skeletons, not spinners
- Keep original button labels while showing loading indicator
- Add 150-300ms show-delay for loading states
- Minimum 300-500ms visible duration

### Forms
- Enter key submits single-control forms
- `⌘/⌃+Enter` submits in `<textarea>`
- Keep submit enabled until submission begins
- Show errors adjacent to fields
- Focus first error on submit
- Never prevent pasting in input/textarea fields
- Never disable browser zoom
- Input font size ≥16px on mobile (prevents iOS auto-zoom)
- Trim trailing whitespace from inputs
- Set `autocomplete` and meaningful `name` attributes

### State Persistence
- Persist UI state in URLs (filters, tabs, pagination)
- Deep-link all stateful UI
- Back/Forward navigation restores scroll position
- Update UI immediately (optimistic updates), reconcile with server

## Animation Rules

### Performance
- Only animate compositor properties (`transform`, `opacity`)
- Never animate layout properties (`width`, `height`, `margin`, `padding`)
- Limit interaction feedback to 200ms maximum
- Prefer CSS animations over JavaScript
- Avoid `will-change` outside active animations

### Accessibility
- Respect `prefers-reduced-motion` preference
- Pause looping animations when elements leave viewport
- Make animations cancelable by user input
- Don't animate large images or full-screen surfaces
- Avoid autoplay - trigger animations from user actions

### CSS Specifics
- Never use `transition: all` - explicitly list properties
- Set correct `transform-origin` for physical starting point

## Typography

- Use `text-balance` for headings
- Use `text-pretty` for body text
- Apply `tabular-nums` for numerical data
- Use `truncate` or `line-clamp` for constrained layouts
- Use curly quotes (" ") not straight quotes
- Use ellipsis character (…) not three periods
- Avoid widows and orphans

## Layout

### Spacing & Alignment
- Every element aligns intentionally (grid, baseline, edge, optical center)
- Make optical adjustments (±1px) when perception outweighs precision
- Test responsively: mobile, laptop, ultra-wide

### Z-Index
- Implement fixed z-index scale (no arbitrary values)
- Use `size-*` utilities for square dimensions

### Viewport
- Replace `h-screen` with `h-dvh` for proper viewport handling
- Fixed elements must respect `safe-area-inset` properties
- Account for notches using safe-area CSS variables
- Set `overscroll-behavior: contain` in modals/drawers

### Scrolling
- Set `scroll-margin-top` on headers for section linking
- Only render necessary scrollbars
- Let CSS handle layout (flex/grid) rather than JS measurement

## Visual Design

### Colors & Shadows
- Use Tailwind's default shadows/colors before custom tokens
- Limit accent colors to one per view
- Omit gradients unless explicitly requested
- Layer shadows to mimic ambient and direct light
- Tint borders/shadows toward consistent hue on non-neutral backgrounds

### Borders & Radius
- Child border-radius ≤ parent radius (concentric alignment)
- Combine borders and semi-transparent borders for edge clarity

### Dark Mode
- Set `color-scheme: dark` on `<html>` for proper scrollbar contrast
- Set `<meta name="theme-color">` to align browser UI

## Content Guidelines

### Empty States
- Require one clear call-to-action
- Design all states: empty, sparse, dense, error

### Skeletons
- Must mirror final content exactly (no layout shift)
- Preload only above-the-fold images; lazy-load rest
- Set explicit image dimensions to prevent CLS

### Error Messages
- Position adjacent to triggering action
- Guide resolution: include how to fix, not just what failed
- Frame constructively: "Something went wrong—try again"
- Announce with `aria-live="polite"` for toasts

### Page Titles
- `<title>` should reflect current context

## Performance

### Targets
- Complete POST/PATCH/DELETE requests in <500ms
- Virtualize large lists
- Move expensive operations to Web Workers

### Images
- Preload critical above-fold images
- Use `<link rel="preconnect">` for asset domains
- Set explicit dimensions to prevent layout shift

### React Specifics
- Prefer uncontrolled inputs
- Replace `useEffect` with render logic where possible
- Keep controlled loops efficient

## Code Review Checklist

Before submitting UI code, verify:

- [ ] All images have alt text
- [ ] All icon-only buttons have aria-label
- [ ] All form inputs have labels
- [ ] Semantic HTML used (button, a, nav, etc.)
- [ ] Focus states visible
- [ ] Keyboard navigation works
- [ ] Color contrast ≥4.5:1
- [ ] Touch targets ≥44px on mobile
- [ ] Loading states use skeletons
- [ ] Destructive actions have confirmation
- [ ] Animations respect prefers-reduced-motion
- [ ] No layout property animations
- [ ] State persisted in URL where appropriate
- [ ] Error messages guide resolution
- [ ] Empty states have clear CTA
