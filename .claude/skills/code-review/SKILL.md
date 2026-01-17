---
name: code-review
description: |
  Activates when reviewing code, checking for security issues, or validating best practices.
  Use this skill for: security audits, performance reviews, React 19 pattern validation,
  TypeScript strict mode compliance, and OWASP vulnerability checks.
  Keywords: review, security, performance, audit, OWASP, vulnerability, best practice, lint
---

# Code Review Skill

This skill provides guidance for reviewing code quality, security, and best practices in Landbruget.dk.

## Activation Context

This skill activates when:
- Reviewing code for security vulnerabilities
- Checking performance patterns
- Validating React 19 best practices
- Ensuring TypeScript strict mode compliance
- Auditing for OWASP top 10 vulnerabilities

## Security Checklist (OWASP Top 10)

### 1. Injection Prevention

**SQL Injection:**
```typescript
// ❌ BAD - String interpolation
const { data } = await supabase.rpc('search', { query: `%${userInput}%` });

// ✅ GOOD - Parameterized queries
const { data } = await supabase
  .from('table')
  .select('*')
  .ilike('column', `%${userInput}%`);
```

**Command Injection:**
```typescript
// ❌ BAD - Direct command execution
exec(`ls ${userInput}`);

// ✅ GOOD - Never pass user input to shell commands
// Use safe APIs instead
```

### 2. XSS Prevention

```typescript
// ❌ BAD - dangerouslySetInnerHTML with user input
<div dangerouslySetInnerHTML={{ __html: userContent }} />

// ✅ GOOD - React escapes by default
<div>{userContent}</div>

// If HTML is needed, sanitize first
import DOMPurify from 'dompurify';
<div dangerouslySetInnerHTML={{ __html: DOMPurify.sanitize(content) }} />
```

### 3. Sensitive Data Exposure

```typescript
// ❌ BAD - Logging sensitive data
console.log('User data:', userData);

// ✅ GOOD - Redact sensitive fields
console.log('User ID:', userData.id);

// ❌ BAD - Exposing API keys in client
const apiKey = process.env.SUPABASE_KEY; // This might be service key!

// ✅ GOOD - Only use public keys in client
const apiKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
```

### 4. Authentication Issues

```typescript
// ❌ BAD - Storing tokens in localStorage
localStorage.setItem('token', authToken);

// ✅ GOOD - Use Supabase session management
const { data: { session } } = await supabase.auth.getSession();
```

### 5. Access Control

```sql
-- ❌ BAD - No RLS
SELECT * FROM sensitive_data;

-- ✅ GOOD - RLS enabled
ALTER TABLE sensitive_data ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Users can only see own data"
  ON sensitive_data FOR SELECT
  USING (auth.uid() = user_id);
```

## React 19 Best Practices

### Component Patterns

```typescript
// ✅ GOOD - Function declaration (not arrow)
export function FeatureComponent({ data }: Props) {
  return <div>{data}</div>;
}

// ❌ BAD - Arrow function component
export const FeatureComponent = ({ data }: Props) => <div>{data}</div>;
```

### Server vs Client Components

```typescript
// Server Component (default) - for data fetching
// frontend/src/app/page.tsx
export default async function Page() {
  const data = await fetchData(); // Direct async
  return <ClientComponent data={data} />;
}

// Client Component - for interactivity
// frontend/src/components/Feature.tsx
'use client';
export function Feature() {
  const [state, setState] = useState();
  return <div onClick={() => setState(...)}>...</div>;
}
```

### Hooks Rules

```typescript
// ❌ BAD - Conditional hooks
if (condition) {
  useEffect(() => {}, []);
}

// ✅ GOOD - Hooks at top level
useEffect(() => {
  if (condition) {
    // effect logic
  }
}, [condition]);

// ❌ BAD - Missing dependencies
useEffect(() => {
  fetchData(userId);
}, []); // Missing userId

// ✅ GOOD - All dependencies listed
useEffect(() => {
  fetchData(userId);
}, [userId]);
```

### Memoization

```typescript
// ✅ GOOD - Memoize expensive computations
const filteredData = useMemo(
  () => data.filter(item => item.type === filter),
  [data, filter]
);

// ✅ GOOD - Stable callback references
const handleClick = useCallback(
  (id: string) => setSelected(id),
  [setSelected]
);

// ❌ BAD - Over-memoization (simple operations)
const sum = useMemo(() => a + b, [a, b]); // Overkill
```

## TypeScript Strict Mode

### No `any` Types

```typescript
// ❌ BAD
function process(data: any) { ... }

// ✅ GOOD
interface DataType {
  id: string;
  value: number;
}
function process(data: DataType) { ... }

// If type is truly unknown
function process(data: unknown) {
  if (isDataType(data)) { ... }
}
```

### Explicit Return Types

```typescript
// ❌ BAD - Implicit return type
function getData() {
  return fetch('/api/data');
}

// ✅ GOOD - Explicit return type
function getData(): Promise<Response> {
  return fetch('/api/data');
}
```

### Null Handling

```typescript
// ❌ BAD - Non-null assertion
const value = data!.field;

// ✅ GOOD - Proper null check
const value = data?.field ?? defaultValue;

// Or with type guard
if (data) {
  const value = data.field;
}
```

## Performance Patterns

### Data Fetching

```typescript
// ❌ BAD - Fetching in loop
for (const id of ids) {
  const data = await fetch(`/api/${id}`);
}

// ✅ GOOD - Parallel fetching
const results = await Promise.all(
  ids.map(id => fetch(`/api/${id}`))
);
```

### List Rendering

```typescript
// ❌ BAD - No key or index as key
{items.map((item, index) => <Item key={index} />)}

// ✅ GOOD - Stable unique key
{items.map(item => <Item key={item.id} />)}

// For large lists, use virtualization
import { useVirtualizer } from '@tanstack/react-virtual';
```

### Image Optimization

```typescript
// ❌ BAD - Regular img tag
<img src="/large-image.jpg" />

// ✅ GOOD - Next.js Image component
import Image from 'next/image';
<Image
  src="/image.jpg"
  alt="Description"
  width={800}
  height={600}
  loading="lazy"
/>
```

## Data Pipeline Review (Backend Python)

### CRS/Geospatial Checks

**CRITICAL: Buffer operations must use metric CRS!**

```python
# ❌ WRONG - Buffer in degrees (1000 degrees = wraps Earth!)
ST_Buffer(geometry, 1000)

# ✅ CORRECT - Use crs_utils for metric buffers
from common.crs_utils import sql_buffer_meters
buffer_sql = sql_buffer_meters("geometry", 1000)  # 1000 meters
```

**Check for these patterns in PR reviews:**
- `ST_Buffer` without `ST_Transform` on EPSG:4326 data = **BUG**
- `ST_Distance` comparisons with large numbers on degree data = **BUG**
- Buffer distances > 1 on WGS84 data = likely wrong CRS

### DuckDB Spatial Patterns

```sql
-- ✅ CORRECT: Transform to UTM, buffer in meters
ST_Intersects(
    ST_Transform(geom1, 'EPSG:4326', 'EPSG:25832'),
    ST_Buffer(ST_Transform(geom2, 'EPSG:4326', 'EPSG:25832'), 1000)
)

-- ❌ WRONG: Buffer on degree data
ST_Intersects(geom1, ST_Buffer(geom2, 1000))
```

## Code Review Checklist

### Security
- [ ] No SQL injection vulnerabilities
- [ ] No XSS vulnerabilities
- [ ] No sensitive data in logs
- [ ] Environment variables properly scoped (NEXT_PUBLIC_ for client)
- [ ] RLS enabled on new tables
- [ ] Input validation at boundaries

### Data Pipeline (Backend)
- [ ] ST_Buffer operations use UTM (EPSG:25832) not WGS84 degrees
- [ ] CVR/CHR format validation present
- [ ] Geometry stored in EPSG:4326
- [ ] No silent transformation failures

### Performance
- [ ] No N+1 queries
- [ ] Proper memoization where needed
- [ ] Images use Next.js Image component
- [ ] Large lists virtualized
- [ ] Lazy loading for heavy components

### React 19
- [ ] Function declarations for components
- [ ] Proper server/client component split
- [ ] Hooks follow rules
- [ ] Keys are stable and unique

### TypeScript
- [ ] No `any` types
- [ ] Explicit return types on functions
- [ ] Proper null handling (no `!` assertions)
- [ ] Interfaces defined for data shapes

### Code Quality
- [ ] No commented-out code
- [ ] No console.log in production code
- [ ] Meaningful variable names
- [ ] Components under 200 lines
- [ ] Single responsibility principle

## Running Automated Checks

```bash
# TypeScript type checking
cd frontend && npm run build

# Linting (oxlint - uses default configuration, no config file needed)
cd frontend && npm run lint

# All checks
cd frontend && npm run lint && npm run build && npm test
```

**Note**: This project uses **oxlint** (50-100x faster than ESLint) with default configuration. No `.oxlintrc.json` file is needed.
