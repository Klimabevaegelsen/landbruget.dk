# oxlint-plugin-landbruget

Custom oxlint plugin for enforcing landbruget.dk architectural rules.

## Purpose

This plugin transforms architectural guidelines from CLAUDE.md into **executable specifications** that agents can use for self-correction. "Lint green" becomes the definition of "architecturally compliant."

## Architecture Rules Enforced

### 1. `landbruget.dk/no-direct-db-import-in-ui`

**Problem**: React components directly importing Supabase client creates tight coupling and makes components non-reusable.

**Rule**: Prevent components in `src/components/` from importing the Supabase client.

**Enforcement**:

```javascript
// ❌ Bad - will fail lint
import { supabase } from '@/lib/supabase'

export function FieldCard() {
  const [data, setData] = useState()
  useEffect(() => {
    supabase.from('fields').select('*').then(setData)
  }, [])
}

// ✅ Good - passes lint
interface FieldCardProps {
  data: Field[]
}

export function FieldCard({ data }: FieldCardProps) {
  // Render data passed as props
}
```

**Rationale**: Components should be presentational. Data fetching happens in:

- Server Components (page.tsx)
- Custom hooks (useFields.ts)
- Service layers (services/fields.ts)

### 2. `landbruget.dk/require-source-tag-prop`

**Problem**: Users need to know if data comes from official APIs or user-submitted sources for trust and transparency.

**Rule**: Components displaying agricultural data must accept and render a `source: 'api' | 'owner' | 'mixed'` prop.

**Enforcement**:

```typescript
// ❌ Bad - will fail lint
interface FieldDataRowProps {
  label: string
  value: string | number
}

// ✅ Good - passes lint
interface FieldDataRowProps {
  label: string
  value: string | number
  source: 'api' | 'owner' | 'mixed'
}

export function FieldDataRow({ label, value, source }: FieldDataRowProps) {
  return (
    <div>
      <span>{label}: {value}</span>
      <SourceBadge source={source} />
    </div>
  )
}
```

**Components requiring source prop**:

- `*DataRow`, `*DataTable`, `*MetricCard`
- Any component in `src/components/fields/`, `src/components/companies/`
- Components using data from Supabase tables

### 3. `landbruget.dk/enforce-status-lifecycle-enum`

**Problem**: Magic strings for status values lead to typos and inconsistent handling.

**Rule**: Status values must use the `PropertyStatus` enum from the data model.

**Enforcement**:

```typescript
// ❌ Bad - will fail lint
if (field.status === 'draft') { ... }
if (field.status === 'awaiting_owner') { ... }  // Typo!

// ✅ Good - passes lint
import { PropertyStatus } from '@/types'

if (field.status === PropertyStatus.Draft) { ... }
if (field.status === PropertyStatus.AwaitingOwner) { ... }
```

**Enums to enforce**:

- `PropertyStatus`: Draft, AwaitingOwner, Published, Archived
- `DataSource`: API, Owner, Mixed
- `ComplianceStatus`: Compliant, NonCompliant, Unknown, Pending

### 4. `landbruget.dk/no-insecure-functions`

**Problem**: `eval()`, `Function()`, and similar constructs create security vulnerabilities.

**Rule**: Disallow use of:

- `eval()`
- `new Function()`
- `setTimeout/setInterval` with string arguments
- Dynamic `import()` with user input

**Enforcement**:

```javascript
// ❌ Bad - will fail lint
const result = eval(userInput);
const fn = new Function('x', 'return x * 2');
setTimeout('doSomething()', 1000);

// ✅ Good - passes lint
const result = parseUserInput(userInput);
const fn = (x) => x * 2;
setTimeout(() => doSomething(), 1000);
```

### 5. `landbruget.dk/geospatial-crs-validation`

**Problem**: Mixing coordinate systems causes incorrect map rendering.

**Rule**: Geospatial data must specify CRS and convert appropriately:

- Danish sources: EPSG:25832
- Storage/Supabase: EPSG:4326 (WGS84)
- Display: EPSG:3857 (Web Mercator)

**Enforcement**:

```typescript
// ❌ Bad - ambiguous CRS
const point = { lat: 56.26, lon: 9.5 };

// ✅ Good - explicit CRS
const point = {
  lat: 56.26,
  lon: 9.5,
  crs: 'EPSG:4326',
};

// Or use typed GeoJSON
const feature: Feature<Point> = {
  type: 'Feature',
  geometry: {
    type: 'Point',
    coordinates: [9.5, 56.26], // Note: lon, lat order in GeoJSON!
  },
  properties: { crs: 'EPSG:4326' },
};
```

## Implementation Status

### Phase 1: Documentation (✅ Complete)

- [x] Define architectural rules
- [x] Document enforcement patterns
- [x] Create examples and rationale

### Phase 2: Plugin Development (🔴 TODO)

- [ ] Set up oxlint JS plugin boilerplate
- [ ] Implement `no-direct-db-import-in-ui` rule
- [ ] Implement `require-source-tag-prop` rule
- [ ] Implement `enforce-status-lifecycle-enum` rule
- [ ] Implement `no-insecure-functions` rule
- [ ] Implement `geospatial-crs-validation` rule
- [ ] Write tests for each rule
- [ ] Configure in `.oxlintrc.json`

### Phase 3: Integration (🔴 TODO)

- [ ] Add to npm scripts
- [ ] Configure CI/CD to fail on violations
- [ ] Update CLAUDE.md with rule documentation
- [ ] Train agents on rule meanings

## How Agents Use This

1. **Write code** following architectural guidelines
2. **Run lint** (`npm run lint`)
3. **See violations** with clear error messages
4. **Self-correct** based on rules
5. **Re-run lint** until green
6. **Commit** when lint passes

This creates a **rapid feedback loop** where agents learn architectural patterns through immediate, deterministic feedback rather than human code review cycles.

## Development Guide

### Creating a New Rule

1. **Define the problem** in this README
2. **Document examples** (bad vs good code)
3. **Implement rule** in `rules/` directory
4. **Write tests** in `tests/` directory
5. **Add to configuration** in `.oxlintrc.json`
6. **Update CLAUDE.md** with rule guidance

### Testing Rules

```bash
cd frontend/oxlint-plugin-landbruget
npm test
```

### Building Plugin

```bash
npm run build
```

## Resources

- [oxlint Documentation](https://oxc.rs/docs/guide/usage/linter)
- [oxlint JS Plugins](https://oxc.rs/docs/guide/usage/linter/js-plugins)
- [ESLint Rule API](https://eslint.org/docs/latest/extend/custom-rules) (compatible)
- [Factory.ai: Using Linters to Direct Agents](https://www.factory.ai/using-linters-to-direct-agents)

---

_This plugin is part of the agent-native development infrastructure for landbruget.dk, transforming natural language architectural guidelines into machine-enforceable rules._
