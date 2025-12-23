# Security Rules

## Absolute Prohibitions

1. **Never commit `.env` files** - Only `.env.example`
2. **Never commit secrets or credentials**
3. **Never log sensitive data** - No API keys, tokens, passwords
4. **Never use `dangerouslySetInnerHTML`** without sanitization

## Environment Variables

### Client-Side (Browser)
Must be prefixed with `NEXT_PUBLIC_`:
```typescript
// ✅ OK - exposed to browser
process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

// ❌ NEVER - service key in browser
process.env.SUPABASE_SERVICE_ROLE_KEY
```

### Server-Side Only
No prefix, never exposed to client:
```typescript
// Server components and API routes only
process.env.SUPABASE_SERVICE_ROLE_KEY
```

## Database Security

### RLS Required
Every table must have Row Level Security enabled:
```sql
ALTER TABLE [table] ENABLE ROW LEVEL SECURITY;
```

### Default Policy
Most Landbruget.dk tables are public read:
```sql
CREATE POLICY "Allow public read"
  ON [table] FOR SELECT
  USING (true);
```

### Sensitive Tables
Restrict access as needed:
```sql
CREATE POLICY "Users see own data"
  ON [table] FOR SELECT
  USING (auth.uid() = user_id);
```

## Input Validation

### At System Boundaries
Validate all user input:
```typescript
// Use Zod for schema validation
const schema = z.object({
  cvr: z.string().regex(/^\d{8}$/),
  email: z.string().email(),
});
```

### SQL Injection Prevention
Use parameterized queries (Supabase handles this):
```typescript
// ✅ Safe
supabase.from('table').select('*').eq('column', userInput)

// ❌ Dangerous
supabase.rpc('search', { query: `%${userInput}%` })
```

## XSS Prevention

React escapes by default. If HTML is needed:
```typescript
import DOMPurify from 'dompurify';
<div dangerouslySetInnerHTML={{ __html: DOMPurify.sanitize(content) }} />
```

## Authentication

Use Supabase session management:
```typescript
// ✅ Correct
const { data: { session } } = await supabase.auth.getSession();

// ❌ Don't store tokens manually
localStorage.setItem('token', token); // No!
```

## Code Review Security Checklist

- [ ] No hardcoded secrets
- [ ] Environment variables properly scoped
- [ ] RLS enabled on new tables
- [ ] User input validated
- [ ] No SQL injection vectors
- [ ] No XSS vectors
- [ ] No sensitive data in logs
