# Frontend Development Guide - Landbruget.dk

## Technology Stack

- **Framework**: Next.js 15 (App Router)
- **Language**: TypeScript 5 (strict mode)
- **UI Library**: React 19
- **Styling**: Tailwind CSS 4
- **UI Components**: Radix UI primitives + custom components
- **State Management**: Zustand
- **Data Fetching**: Native fetch with Supabase client
- **Maps**: MapLibre GL + PMTiles
- **Charts**: Recharts
- **Testing**: Playwright (E2E)
- **Linting**: oxlint (50-100x faster than ESLint)
- **Formatting**: Prettier with Tailwind plugin

## Directory Structure

```
src/
├── app/              # Next.js App Router pages
│   ├── page.tsx      # Homepage
│   ├── layout.tsx    # Root layout
│   └── [routes]/     # Dynamic and static routes
│
├── components/       # Reusable React components
│   ├── ui/          # Base UI primitives (buttons, inputs, etc.)
│   ├── map/         # Map-related components
│   ├── charts/      # Chart components
│   └── [domain]/    # Domain-specific components
│
├── hooks/           # Custom React hooks
│   ├── useMap.ts
│   ├── useSupabase.ts
│   └── [others]
│
├── lib/             # Utilities and configuration
│   ├── supabase.ts  # Supabase client
│   ├── utils.ts     # General utilities
│   └── cn.ts        # Class name utilities
│
├── services/        # API and data services
│   └── supabase.ts  # Supabase queries
│
├── stores/          # Zustand stores
│   └── mapStore.ts  # Map state management
│
├── types/           # TypeScript type definitions
│   └── index.ts     # Shared types
│
└── utils/           # Helper functions
```

## Component Guidelines

### Component Structure

Every component should follow this pattern:

```typescript
// ComponentName.tsx
import { ReactNode } from 'react'
import { cn } from '@/lib/utils'

interface ComponentNameProps {
  children?: ReactNode
  className?: string
  // Other props with explicit types
}

export function ComponentName({
  children,
  className,
  ...props
}: ComponentNameProps) {
  return (
    <div className={cn('base-classes', className)} {...props}>
      {children}
    </div>
  )
}
```

### Component Rules

1. **Functional Components Only**: Use function declarations, not arrow functions for components
2. **Props Interface**: Always define explicit props interface (no inline types)
3. **No Business Logic**: Components should be presentational, logic goes in hooks/services
4. **Composability**: Prefer composition over complex components
5. **Accessibility**: Use semantic HTML and ARIA attributes
6. **Type Safety**: No `any` types - use proper TypeScript types

### Styling Guidelines

1. **Tailwind First**: Use Tailwind utility classes for styling
2. **Class Name Merging**: Use `cn()` helper for conditional classes
3. **Responsive Design**: Mobile-first approach (use `sm:`, `md:`, `lg:` breakpoints)
4. **Dark Mode**: Support dark mode with `dark:` prefix (via next-themes)
5. **No Inline Styles**: Avoid `style` prop unless absolutely necessary

Example:

```typescript
<div className={cn(
  'flex items-center gap-2',
  'rounded-lg border p-4',
  'hover:bg-gray-50 dark:hover:bg-gray-900',
  isActive && 'bg-blue-50 dark:bg-blue-900',
  className
)}>
```

## State Management

### When to Use Zustand

Use Zustand stores for:

- Global UI state (map viewport, filters, selections)
- Cross-component shared state
- State that persists across route changes

Example:

```typescript
// stores/mapStore.ts
import { create } from 'zustand';

interface MapState {
  viewport: { latitude: number; longitude: number; zoom: number };
  selectedFieldId: string | null;
  setViewport: (viewport: MapState['viewport']) => void;
  setSelectedFieldId: (id: string | null) => void;
}

export const useMapStore = create<MapState>((set) => ({
  viewport: { latitude: 56.26, longitude: 9.5, zoom: 7 },
  selectedFieldId: null,
  setViewport: (viewport) => set({ viewport }),
  setSelectedFieldId: (id) => set({ selectedFieldId: id }),
}));
```

### When to Use React State

Use `useState` for:

- Local component state
- Form inputs
- UI toggles (modals, dropdowns, etc.)

Use `useReducer` for:

- Complex state logic with multiple sub-values
- State transitions based on actions

## Data Fetching

### Supabase Client

```typescript
// lib/supabase.ts
import { createClient } from '@supabase/supabase-js';

export const supabase = createClient(
  process.env.NEXT_PUBLIC_API_URL!,
  process.env.NEXT_PUBLIC_API_KEY!
);
```

### Fetching Data

```typescript
// services/fields.ts
import { supabase } from '@/lib/supabase';

export async function getFields(filters?: FieldFilters) {
  const { data, error } = await supabase
    .from('fields')
    .select('*')
    .eq('crop_type', filters?.cropType)
    .limit(100);

  if (error) throw error;
  return data;
}
```

### Server Components vs Client Components

- **Server Components (default)**: Use for data fetching, SEO, initial render
- **Client Components (`'use client'`)**: Use for interactivity, hooks, browser APIs

```typescript
// Server Component (default)
export default async function FieldsPage() {
  const fields = await getFields()
  return <FieldsList fields={fields} />
}

// Client Component
'use client'
export function InteractiveMap() {
  const [selected, setSelected] = useState(null)
  return <Map onSelect={setSelected} />
}
```

## Map Components

### MapLibre Integration

```typescript
'use client'
import Map from 'react-map-gl/maplibre'
import 'maplibre-gl/dist/maplibre-gl.css'

export function FieldMap() {
  const { viewport, setViewport } = useMapStore()

  return (
    <Map
      {...viewport}
      onMove={evt => setViewport(evt.viewState)}
      mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
    >
      {/* Map layers here */}
    </Map>
  )
}
```

### PMTiles for Large Datasets

```typescript
import { Protocol } from 'pmtiles';

// Initialize PMTiles protocol once
const protocol = new Protocol();
maplibregl.addProtocol('pmtiles', protocol.tile);

// Use in map source
const source = {
  type: 'vector',
  url: 'pmtiles://https://storage.googleapis.com/landbruget-data/fields.pmtiles',
};
```

## Forms & Validation

### React Hook Form + Zod

```typescript
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'

const formSchema = z.object({
  fieldName: z.string().min(1, 'Field name is required'),
  cropType: z.enum(['wheat', 'barley', 'corn']),
  area: z.number().positive('Area must be positive'),
})

type FormData = z.infer<typeof formSchema>

export function FieldForm() {
  const form = useForm<FormData>({
    resolver: zodResolver(formSchema),
    defaultValues: { fieldName: '', cropType: 'wheat', area: 0 }
  })

  async function onSubmit(data: FormData) {
    // Handle submission
  }

  return (
    <form onSubmit={form.handleSubmit(onSubmit)}>
      {/* Form fields */}
    </form>
  )
}
```

## Testing with Playwright

### Test Structure

```typescript
// e2e/field-search.spec.ts
import { test, expect } from '@playwright/test';

test.describe('Field Search', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display search results', async ({ page }) => {
    await page.fill('[data-testid="search-input"]', '12345');
    await page.click('[data-testid="search-button"]');

    await expect(page.locator('[data-testid="results"]')).toBeVisible();
    await expect(page.locator('.field-card')).toHaveCount(1);
  });
});
```

### Testing Best Practices

1. **Use data-testid**: Add `data-testid` attributes for reliable selectors
2. **Test User Flows**: Focus on critical user journeys, not implementation details
3. **Accessibility**: Use `page.getByRole()`, `page.getByLabel()` when possible
4. **Mock External APIs**: Use Playwright's route mocking for external services
5. **Keep Tests Fast**: Use `test:smoke:fast` for quick feedback

## Performance Optimization

### Code Splitting

```typescript
// Lazy load heavy components
import dynamic from 'next/dynamic'

const HeavyChart = dynamic(() => import('@/components/charts/HeavyChart'), {
  loading: () => <Skeleton />,
  ssr: false
})
```

### Image Optimization

```typescript
import Image from 'next/image'

<Image
  src="/field-photo.jpg"
  alt="Field description"
  width={800}
  height={600}
  priority // For above-the-fold images
  loading="lazy" // For below-the-fold images
/>
```

### Memoization

```typescript
import { useMemo, useCallback } from 'react';

// Expensive calculations
const filteredFields = useMemo(
  () => fields.filter((f) => f.crop === selectedCrop),
  [fields, selectedCrop]
);

// Stable function references
const handleSelect = useCallback(
  (id: string) => setSelectedFieldId(id),
  [setSelectedFieldId]
);
```

## Common Patterns

### Loading States

```typescript
export function FieldList() {
  const [loading, setLoading] = useState(true)
  const [fields, setFields] = useState([])

  useEffect(() => {
    async function loadFields() {
      try {
        setLoading(true)
        const data = await getFields()
        setFields(data)
      } catch (error) {
        console.error('Failed to load fields:', error)
      } finally {
        setLoading(false)
      }
    }
    loadFields()
  }, [])

  if (loading) return <Skeleton />
  if (fields.length === 0) return <EmptyState />

  return <div>{/* Render fields */}</div>
}
```

### Error Boundaries

```typescript
// components/ErrorBoundary.tsx
'use client'
import { useEffect } from 'react'

export function ErrorBoundary({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  useEffect(() => {
    console.error(error)
  }, [error])

  return (
    <div className="flex flex-col items-center justify-center p-8">
      <h2 className="text-2xl font-bold">Something went wrong!</h2>
      <button onClick={reset} className="mt-4">
        Try again
      </button>
    </div>
  )
}
```

## Environment Variables

All environment variables must be prefixed with `NEXT_PUBLIC_` to be accessible in the browser:

```typescript
// ✅ Good - available in browser
const apiUrl = process.env.NEXT_PUBLIC_API_URL;

// ❌ Bad - only available server-side
const apiUrl = process.env.API_URL;
```

## Linting & Formatting

### Before Committing

```bash
npm run lint     # Run oxlint (fast!)
npm run format   # Format with Prettier
npm test         # Run E2E tests
```

### oxlint Configuration

See `.oxlintrc.json` for custom rules. oxlint is configured to:

- Enforce TypeScript best practices
- Catch React anti-patterns
- Ensure accessibility standards
- Validate import statements

## Common Issues & Solutions

### Issue: "Hydration mismatch" errors

**Solution**: Ensure server and client render the same HTML. Check for:

- Date/time formatting (use UTC consistently)
- Random values (move to client component)
- Browser-specific APIs (use `useEffect`)

### Issue: "Module not found" errors

**Solution**: Check import paths. Use `@/` alias for `src/`:

```typescript
// ✅ Good
import { Button } from '@/components/ui/button';

// ❌ Bad
import { Button } from '../../../components/ui/button';
```

### Issue: Slow map rendering

**Solution**:

- Use PMTiles for large datasets
- Implement proper memoization
- Lazy load map component
- Use vector tiles, not raster

### Issue: Type errors with Supabase

**Solution**: Generate types from database:

```bash
supabase gen types typescript --local > src/types/supabase.ts
```

## Quick Commands

```bash
npm run dev              # Start dev server (http://localhost:3000)
npm run build            # Build for production
npm run start            # Start production server
npm test                 # Run E2E tests
npm run test:ui          # Playwright UI mode
npm run test:headed      # Run tests with browser visible
npm run lint             # Run oxlint
npm run format           # Format code
npm run format:check     # Check formatting
```

---

_For backend integration, see `../backend/CLAUDE.md`_
_For general project guidelines, see `../CLAUDE.md`_
