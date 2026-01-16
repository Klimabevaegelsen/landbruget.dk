# UI Development Rules

## Stack & Tooling

### Required Stack
- **Tailwind CSS**: Use defaults unless custom values exist or are requested
- **Animation**: `motion/react` for JS animations, `tw-animate-css` for entrance effects
- **Class utilities**: `cn` utility (combines `clsx` + `tailwind-merge`)
- **Primitives**: Radix, React Aria, or Base UI for interactive elements

### Before Building New Components
1. Check existing components in `frontend/src/components/`
2. Use accessible primitives for interactive elements
3. Don't mix primitive systems within the same surface

## Accessibility Requirements (WCAG 2.1)

### Critical - Must Fix Immediately
```tsx
// ❌ BAD - Image without alt
<img src="farm.jpg" />

// ✅ GOOD
<img src="farm.jpg" alt="Aerial view of Danish farm fields" />
```

```tsx
// ❌ BAD - Icon button without label
<button onClick={close}><XIcon /></button>

// ✅ GOOD
<button onClick={close} aria-label="Close dialog"><XIcon /></button>
```

```tsx
// ❌ BAD - Non-semantic interactive element
<div onClick={handleClick}>Click me</div>

// ✅ GOOD
<button onClick={handleClick}>Click me</button>
```

```tsx
// ❌ BAD - Input without label
<input type="text" placeholder="Search..." />

// ✅ GOOD
<label>
  <span className="sr-only">Search</span>
  <input type="text" placeholder="Search farms..." />
</label>
```

### Focus States
```css
/* ❌ NEVER remove focus without replacement */
*:focus { outline: none; }

/* ✅ Use focus-visible for keyboard users */
button:focus-visible {
  outline: 2px solid currentColor;
  outline-offset: 2px;
}
```

### Touch Targets
```tsx
// ❌ BAD - Small touch target
<button className="p-1">
  <Icon className="w-4 h-4" />
</button>

// ✅ GOOD - 44px minimum on mobile
<button className="p-2 min-w-[44px] min-h-[44px]">
  <Icon className="w-4 h-4" />
</button>
```

## Animation Rules

### Allowed Properties
```css
/* ✅ GPU-accelerated (compositor properties) */
transform: translateX(10px);
opacity: 0.5;

/* ❌ NEVER animate (cause layout reflow) */
width: 100px;
height: 100px;
margin: 10px;
padding: 10px;
```

### Duration Limits
```css
/* ✅ Interaction feedback: max 200ms */
.button:hover {
  transition: transform 150ms ease-out;
}

/* ✅ Entrance animations: 200-400ms */
.modal-enter {
  animation: fadeIn 300ms ease-out;
}
```

### Reduced Motion
```tsx
// ✅ Always respect user preference
<motion.div
  animate={{ opacity: 1 }}
  transition={{ duration: prefersReducedMotion ? 0 : 0.3 }}
/>
```

```css
/* ✅ CSS approach */
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    transition-duration: 0.01ms !important;
  }
}
```

### Never Use
```css
/* ❌ NEVER use transition: all */
transition: all 0.3s;

/* ✅ Explicitly list properties */
transition: transform 0.3s, opacity 0.3s;
```

## Loading States

### Use Skeletons, Not Spinners
```tsx
// ❌ BAD - Spinner
<div className="flex justify-center">
  <Spinner />
</div>

// ✅ GOOD - Skeleton matching content
<div className="space-y-4">
  <div className="h-8 w-48 bg-gray-200 animate-pulse rounded" />
  <div className="h-4 w-full bg-gray-200 animate-pulse rounded" />
  <div className="h-4 w-3/4 bg-gray-200 animate-pulse rounded" />
</div>
```

### Button Loading
```tsx
// ✅ Keep label, show loading indicator
<button disabled={isLoading}>
  {isLoading && <Spinner className="mr-2 h-4 w-4" />}
  Save Changes
</button>
```

## Forms

### Required Patterns
```tsx
// ✅ Input with proper attributes
<input
  type="email"
  name="email"
  autoComplete="email"
  className="text-base" // ≥16px prevents iOS zoom
  // Never disable paste
/>
```

### Error Handling
```tsx
// ✅ Error adjacent to field
<div>
  <input aria-invalid={!!error} aria-describedby="email-error" />
  {error && (
    <p id="email-error" className="text-red-600 text-sm mt-1">
      {error}
    </p>
  )}
</div>
```

### Destructive Actions
```tsx
// ✅ Always use AlertDialog for destructive actions
<AlertDialog>
  <AlertDialogTrigger asChild>
    <button className="text-red-600">Delete Farm</button>
  </AlertDialogTrigger>
  <AlertDialogContent>
    <AlertDialogTitle>Delete this farm?</AlertDialogTitle>
    <AlertDialogDescription>
      This action cannot be undone.
    </AlertDialogDescription>
    <AlertDialogAction onClick={handleDelete}>Delete</AlertDialogAction>
    <AlertDialogCancel>Cancel</AlertDialogCancel>
  </AlertDialogContent>
</AlertDialog>
```

## Layout

### Viewport Height
```tsx
// ❌ BAD - Doesn't account for mobile browser chrome
<div className="h-screen" />

// ✅ GOOD - Dynamic viewport height
<div className="h-dvh" />
```

### Safe Areas (Mobile)
```tsx
// ✅ Fixed elements respect safe areas
<nav className="fixed bottom-0 pb-[env(safe-area-inset-bottom)]" />
```

### Z-Index Scale
```tsx
// ✅ Use consistent scale
const zIndex = {
  dropdown: 10,
  modal: 20,
  popover: 30,
  toast: 40,
  tooltip: 50,
};
```

## Typography

### Text Utilities
```tsx
// ✅ Headings
<h1 className="text-balance">Long heading that should wrap nicely</h1>

// ✅ Body text
<p className="text-pretty">Paragraph content...</p>

// ✅ Numbers in tables
<td className="tabular-nums">1,234.56</td>

// ✅ Constrained text
<span className="truncate">Long text that might overflow...</span>
```

## State Persistence

### URL State
```tsx
// ✅ Persist filter state in URL
const [filter, setFilter] = useQueryState('filter');

// ✅ Deep-link all stateful UI
// Filters, tabs, pagination, expanded panels should be URL-based
```

## Color & Contrast

### Requirements
- Text: minimum 4.5:1 contrast ratio
- Large text (18px+): minimum 3:1 contrast ratio
- Non-text elements: minimum 3:1 contrast ratio

### Never Color-Only
```tsx
// ❌ BAD - Status conveyed only by color
<span className="text-green-500">●</span>

// ✅ GOOD - Include text label
<span className="text-green-500">● Active</span>
```

## Dark Mode

### Setup
```tsx
// ✅ In root layout
<html className="dark" style={{ colorScheme: 'dark' }}>
  <head>
    <meta name="theme-color" content="#000000" />
  </head>
</html>
```

## Performance

### Images
```tsx
// ✅ Always set dimensions
<Image
  src="/farm.jpg"
  alt="Farm"
  width={800}
  height={600}
  loading="lazy" // Below fold
  priority // Above fold only
/>
```

### Lists
```tsx
// ✅ Virtualize large lists (100+ items)
import { VList } from 'virtua';

<VList count={items.length}>
  {(index) => <Item key={items[index].id} data={items[index]} />}
</VList>
```

## Quick Reference

### Checklist Before PR
- [ ] All images have alt text
- [ ] All icon buttons have aria-label
- [ ] All inputs have labels
- [ ] Semantic HTML used
- [ ] Focus states visible
- [ ] Touch targets ≥44px mobile
- [ ] No layout property animations
- [ ] Skeletons match content structure
- [ ] Destructive actions use AlertDialog
- [ ] prefers-reduced-motion respected
