# Components Directory - Development Guidelines

## Component Rules

These rules apply to **all** components in this directory and subdirectories.

### 1. Component Structure

**Always use functional components with explicit prop interfaces**:

```typescript
interface ComponentNameProps {
  children?: ReactNode
  className?: string
  // Explicit, named props (never use ...rest blindly)
}

export function ComponentName({ children, className }: ComponentNameProps) {
  return <div className={cn('base-classes', className)}>{children}</div>
}
```

### 2. No Business Logic

Components must be **presentational only**:

- ❌ No direct Supabase imports (enforced by oxlint)
- ❌ No API calls
- ❌ No complex calculations
- ❌ No data transformations
- ✅ Accept data via props
- ✅ Emit events via callbacks
- ✅ Focus on rendering and user interaction

**Business logic belongs in**:

- Server Components (`app/page.tsx`)
- Custom hooks (`hooks/useFields.ts`)
- Services (`services/fields.ts`)

### 3. Styling

**Use Tailwind utility classes exclusively**:

```typescript
<div className={cn(
  'flex items-center gap-2',         // Layout
  'rounded-lg border p-4',           // Spacing & borders
  'bg-white dark:bg-gray-900',       // Colors with dark mode
  'hover:bg-gray-50',                // Interactions
  'transition-colors duration-200',  // Animations
  isActive && 'border-blue-500',     // Conditional
  className                          // Allow override
)}>
```

**Rules**:

- No inline `style` prop (except dynamic values like `left: ${position}px`)
- No CSS modules or styled-components
- Always use `cn()` helper for conditional classes
- Mobile-first: `sm:`, `md:`, `lg:`, `xl:` breakpoints
- Dark mode: `dark:` prefix for all color values

### 4. Accessibility

**Every interactive element must be accessible**:

```typescript
<button
  type="button"
  aria-label="Close dialog"
  onClick={handleClose}
  className={...}
>
  <X className="h-4 w-4" />
</button>
```

**Requirements**:

- Semantic HTML (`button` not `div onClick`)
- ARIA labels where text isn't visible
- Keyboard navigation support
- Focus indicators (don't remove `:focus`)
- Color contrast ratios (WCAG AA minimum)

### 5. Testing

**Every component must be testable**:

```typescript
<div data-testid="field-card" className={...}>
  <h3 data-testid="field-card-title">{title}</h3>
  <p data-testid="field-card-description">{description}</p>
</div>
```

**Rules**:

- Add `data-testid` to key elements
- Use descriptive, unique IDs
- Test user interactions, not implementation
- Write E2E tests in `frontend/e2e/`

### 6. Data Source Transparency

**Components displaying data MUST accept `source` prop** (enforced by oxlint):

```typescript
interface DataComponentProps {
  data: FieldData
  source: 'api' | 'owner' | 'mixed'  // REQUIRED
}

export function DataComponent({ data, source }: DataComponentProps) {
  return (
    <div>
      {/* Display data */}
      <SourceBadge source={source} />
    </div>
  )
}
```

**Components requiring source prop**:

- Anything with "Data", "Metric", "Stat" in the name
- Components in `fields/`, `companies/`, `farms/`
- Any component displaying agricultural data

### 7. TypeScript Strict Mode

**No escape hatches**:

- ❌ No `any` types
- ❌ No `@ts-ignore` comments
- ❌ No non-null assertions (`!`) without good reason
- ✅ Explicit types for all props
- ✅ Type imported from `@/types`
- ✅ Narrow types (unions, not wide types)

### 8. Component Organization

**File structure**:

```
components/
├── ui/              # Base primitives (Button, Input, Dialog)
│   ├── button.tsx
│   └── button.spec.ts
│
├── fields/          # Field-specific components
│   ├── FieldCard.tsx
│   ├── FieldCard.spec.ts
│   ├── FieldList.tsx
│   └── ...
│
├── map/             # Map components
├── charts/          # Chart components
└── shared/          # Cross-domain components
```

**Naming**:

- PascalCase for components: `FieldCard.tsx`
- Kebab-case for non-components: `use-fields.ts`
- Descriptive, specific names: `FieldDataTable` not `Table`

### 9. Radix UI Primitives

**Use Radix for interactive components**:

Already installed primitives:

- `@radix-ui/react-dialog` - Modals, popovers
- `@radix-ui/react-dropdown-menu` - Dropdowns
- `@radix-ui/react-tabs` - Tabs
- `@radix-ui/react-tooltip` - Tooltips
- `@radix-ui/react-select` - Selects
- [Full list in `package.json`]

**Don't build from scratch** if Radix provides it.

### 10. Performance

**Optimize renders**:

```typescript
// Memoize expensive components
const ExpensiveComponent = memo(function ExpensiveComponent({ data }) {
  // Heavy rendering
});

// Memoize callbacks
const handleClick = useCallback(() => {
  doSomething(id);
}, [id]);

// Memoize computed values
const filteredData = useMemo(() => data.filter((item) => item.active), [data]);
```

**When to optimize**:

- Large lists (100+ items)
- Complex calculations
- Frequent re-renders
- Animation loops

**When NOT to optimize**:

- Simple components
- Static data
- Premature optimization

### 11. Map Components

**Special rules for map components**:

```typescript
'use client'; // Map must be client component
import Map from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';

export function FieldMap() {
  // Use PMTiles for large datasets
  // Memoize layers
  // Handle loading states
  // Clean up on unmount
}
```

**Requirements**:

- Lazy load map component
- Show loading skeleton
- Handle geolocation errors
- Support touch gestures
- Optimize for mobile

### 12. Form Components

**Use React Hook Form + Zod**:

```typescript
const schema = z.object({
  fieldName: z.string().min(1),
  area: z.number().positive(),
});

type FormData = z.infer<typeof schema>;

export function FieldForm() {
  const form = useForm<FormData>({
    resolver: zodResolver(schema),
  });

  // ...
}
```

### 13. Error Boundaries

**Wrap risky components**:

- Map components (third-party library errors)
- Chart components (malformed data)
- Dynamic imports (loading failures)

```typescript
<ErrorBoundary fallback={<ErrorState />}>
  <RiskyComponent />
</ErrorBoundary>
```

## Quick Checklist

Before committing a component:

- [ ] Functional component with explicit props interface
- [ ] No business logic (data fetching, calculations)
- [ ] Tailwind CSS only (no inline styles)
- [ ] Accessible (semantic HTML, ARIA labels)
- [ ] Testable (data-testid attributes)
- [ ] `source` prop if displaying data
- [ ] No `any` types or `@ts-ignore`
- [ ] Proper file location and naming
- [ ] Performance optimized (if complex)
- [ ] E2E test written
- [ ] Lint passes (`npm run lint`)
- [ ] Formatted (`npm run format`)

## Common Patterns

### Loading State

```typescript
if (loading) return <Skeleton />
if (error) return <ErrorState error={error} />
if (!data) return <EmptyState />
return <DataDisplay data={data} />
```

### Conditional Rendering

```typescript
{condition && <Component />}
{condition ? <ComponentA /> : <ComponentB />}
```

### List Rendering

```typescript
{items.map(item => (
  <Item key={item.id} data={item} />
))}
```

---

**When in doubt, check existing components for patterns before creating new ones.**
