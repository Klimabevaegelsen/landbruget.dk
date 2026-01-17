# Web Interface Guidelines

Based on Vercel's comprehensive web interface guidelines.

## Interactions

### Accessibility Navigation
- All flows must be keyboard-operable (WAI-ARIA Authoring Patterns)
- Every focusable element needs visible focus ring
- Use `:focus-visible` over `:focus`
- Implement focus traps per WAI-ARIA patterns
- Focus management: move focus appropriately in dialogs/modals

### Hit Targets
- Visual targets under 24px expand to ≥24px minimum
- Mobile targets: 44px minimum
- Remove dead zones: if it looks interactive, it must be

### Input Handling
- Input font size ≥16px on mobile (prevents iOS Safari auto-zoom)
- Respect user zoom settings
- Allow any keystroke; show validation feedback rather than blocking
- Trim trailing whitespace from text inputs
- Never disable paste functionality
- Never disable browser zoom

### State Management
```tsx
// ✅ Optimistic updates
const handleSave = async () => {
  // Update UI immediately
  setData(optimisticValue);

  try {
    const result = await saveToServer();
    // Reconcile with server response
    setData(result);
  } catch {
    // Rollback on error
    setData(previousValue);
  }
};
```

### URL State Persistence
```tsx
// ✅ Deep-link all stateful UI
// - Filters
// - Tabs
// - Pagination
// - Expanded panels
// - Search queries

// Use nuqs or similar
const [filter, setFilter] = useQueryState('filter');
```

### Navigation
- Use semantic `<a>` or `<Link>` for navigation
- Enables standard browser behaviors (cmd-click, right-click)
- Back/Forward restores scroll position

### Tooltips
- Delay first tooltip in a group
- Subsequent tooltips appear immediately
- Prefer inline explanations; tooltips as last resort

### Async Announcements
```tsx
// ✅ Announce async updates
<div aria-live="polite" aria-atomic="true">
  {toastMessage}
</div>
```

## Animations

### Motion Principles
```css
/* ✅ Respect user preference */
@media (prefers-reduced-motion: reduce) {
  * {
    animation-duration: 0.01ms !important;
    transition-duration: 0.01ms !important;
  }
}
```

### Performance Priority
1. CSS animations (preferred)
2. Web Animations API
3. JavaScript libraries (last resort)

### GPU-Accelerated Properties Only
```css
/* ✅ Animate these */
transform: translateX(10px);
opacity: 0.5;

/* ❌ Never animate (trigger reflow) */
width, height, margin, padding, top, left, right, bottom
```

### Easing Guidelines
- Choose based on: size, distance, trigger type
- Make animations cancelable by user input
- Avoid autoplay; trigger from user actions
- Anchor transforms to physical starting point

### CSS Rules
```css
/* ❌ Never */
transition: all 0.3s;

/* ✅ Explicit properties */
transition: transform 0.3s ease-out, opacity 0.3s ease-out;
```

### SVG Transforms
```css
/* ✅ SVG transform setup */
svg g {
  transform-box: fill-box;
  transform-origin: center;
}
```

## Layout

### Optical Adjustments
- Make ±1px adjustments when perception outweighs geometric precision
- Every element aligns intentionally (grid, baseline, edge, optical center)
- Balance weight, size, spacing, color when text and icons sit adjacent

### Responsive Testing
- Mobile devices
- Laptop screens
- Ultra-wide monitors (test at 50% zoom)

### Safe Areas
```css
/* ✅ Account for notches and system UI */
.fixed-bottom {
  padding-bottom: env(safe-area-inset-bottom);
}
```

### Scrollbars
- Only render necessary scrollbars
- Fix overflow issues (test on macOS with "Show scroll bars: Always")
- Let CSS handle layout (flex/grid) rather than JS measurement

## Content

### Page Structure
- `<title>` reflects current context
- Every screen offers next action or recovery path
- Design all states: empty, sparse, dense, error

### Skeleton Screens
```tsx
// ✅ Mirror final content exactly (prevent CLS)
function FarmCardSkeleton() {
  return (
    <div className="p-4 rounded-lg border">
      <div className="h-6 w-32 bg-gray-200 rounded animate-pulse" />
      <div className="mt-2 h-4 w-full bg-gray-200 rounded animate-pulse" />
      <div className="mt-1 h-4 w-3/4 bg-gray-200 rounded animate-pulse" />
    </div>
  );
}
```

### Typography
```tsx
// ✅ Curly quotes
<p>"Hello, world"</p>  // Not: "Hello, world"

// ✅ Proper ellipsis
<span>Loading…</span>  // Not: Loading...

// ✅ Numbers in tables
<td className="font-variant-numeric: tabular-nums">1,234.56</td>
```

### Status Indicators
```tsx
// ❌ Color alone
<span className="text-green-500">●</span>

// ✅ Color + text
<span className="text-green-500">● Active</span>
```

### Section Linking
```css
/* ✅ Account for fixed headers */
[id] {
  scroll-margin-top: 80px;
}
```

### Localization
- Format dates/times/numbers per user locale
- Detect language via `Accept-Language` header and `navigator.languages`
- Never detect via IP/GPS

### Non-Breaking Spaces
```tsx
// ✅ Keep units together
<span>10&nbsp;MB</span>
<span>⌘&nbsp;+&nbsp;K</span>

// ✅ Zero-width spacing
<span>Vercel&#x2060;SDK</span>
```

## Forms

### Enter Key Behavior
- Single text input: Enter submits
- Multiple controls: Enter on last control submits
- `<textarea>`: ⌘/⌃+Enter submits, Enter creates newline

### Labels
```tsx
// ✅ Every control needs a label
<label htmlFor="email">Email address</label>
<input id="email" type="email" />

// ✅ Or wrap input in label
<label>
  Email address
  <input type="email" />
</label>
```

### Submit Button State
```tsx
// ✅ Keep enabled until submission starts
<button
  type="submit"
  disabled={isSubmitting}
>
  {isSubmitting ? (
    <>
      <Spinner className="mr-2" />
      Saving…
    </>
  ) : (
    'Save Changes'
  )}
</button>
```

### Validation
- Allow submitting incomplete forms to surface feedback
- Show errors adjacent to fields
- Focus first error on submit

### Checkboxes & Radios
```tsx
// ✅ Single hit target for label + control
<label className="flex items-center gap-2 cursor-pointer">
  <input type="checkbox" />
  <span>Accept terms</span>
</label>
```

### Input Attributes
```tsx
<input
  type="email"
  name="email"
  autoComplete="email"
  spellCheck={false}  // For emails, codes, usernames
  className="text-base"  // ≥16px
/>
```

### Placeholders
```tsx
// ✅ End with ellipsis, show example pattern
<input placeholder="Search farms…" />
<input placeholder="name@example.com" />
```

### Unsaved Changes
```tsx
// ✅ Warn before navigation
useEffect(() => {
  const handler = (e: BeforeUnloadEvent) => {
    if (hasUnsavedChanges) {
      e.preventDefault();
    }
  };
  window.addEventListener('beforeunload', handler);
  return () => window.removeEventListener('beforeunload', handler);
}, [hasUnsavedChanges]);
```

### Password Managers
- Ensure compatibility with password managers
- Allow pasting one-time codes
- Use `autocomplete="off"` only for non-auth fields

## Performance

### Testing Conditions
- iOS Low Power Mode
- macOS Safari
- CPU throttling (4x slowdown)
- Network throttling (Slow 3G)
- Disable browser extensions

### Request Timing
- POST/PATCH/DELETE: complete in <500ms

### Input Performance
```tsx
// ✅ Prefer uncontrolled inputs
<input defaultValue={value} ref={inputRef} />

// ✅ If controlled, keep loops efficient
const handleChange = useCallback((e) => {
  setValue(e.target.value);
}, []);
```

### Large Lists
```tsx
// ✅ Virtualize 100+ items
import { VList } from 'virtua';

// ✅ Or use CSS
<div style={{ contentVisibility: 'auto' }}>
```

### Images
```tsx
// ✅ Set explicit dimensions
<Image
  src="/farm.jpg"
  width={800}
  height={600}
  alt="Farm"
  loading="lazy"  // Below fold
  priority        // Above fold only
/>
```

### Preloading
```tsx
// ✅ Preconnect to asset domains
<link rel="preconnect" href="https://cdn.example.com" />

// ✅ Preload critical fonts
<link
  rel="preload"
  href="/fonts/inter.woff2"
  as="font"
  type="font/woff2"
  crossOrigin="anonymous"
/>
```

### Web Workers
```tsx
// ✅ Move expensive operations off main thread
const worker = new Worker(new URL('./expensive.worker.ts', import.meta.url));
```

## Design

### Shadows
```css
/* ✅ Layer shadows for depth */
box-shadow:
  0 1px 2px rgba(0, 0, 0, 0.05),   /* Ambient */
  0 4px 8px rgba(0, 0, 0, 0.1);    /* Direct */
```

### Border Radius
```css
/* ✅ Child radius ≤ parent radius */
.card { border-radius: 12px; }
.card-image { border-radius: 8px; }  /* Concentric */
```

### Color Tinting
- Tint borders, shadows, text toward consistent hue on non-neutral backgrounds
- Use color-blind-friendly palettes for charts

### Contrast
- Use APCA over WCAG 2 for more accurate perceptual contrast
- Increase contrast for :hover, :active, :focus states

### Theme Color
```tsx
// ✅ Align browser UI with page
<meta name="theme-color" content="#000000" />
```

### Dark Mode Scrollbars
```css
/* ✅ Proper scrollbar contrast */
html {
  color-scheme: dark;
}
```

## Copywriting (Vercel Style)

### Voice
- Active voice: "Install the CLI" not "The CLI will be installed"
- Second person: "you" not "we" or "I"
- Concise: minimal words
- Use `&` over "and"

### Capitalization
- Headings/buttons: Title Case (Chicago style)
- Marketing: sentence case

### Numbers
- Display as numerals: "8 deployments" not "eight deployments"
- Currency: 0 or 2 decimals, never mixed
- Separate numbers and units: "10 MB"

### Placeholders
- Strings: `YOUR_API_TOKEN_HERE`
- Numbers: `0123456789`

### Errors
- Constructive: "Something went wrong—try again"
- Guide resolution: include how to fix
- No blame language

### Button Labels
- Specific: "Save API Key" not "Continue"
- Use ellipsis for options requiring follow-up: "Rename…"
