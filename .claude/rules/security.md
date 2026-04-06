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
// ✅ OK - exposed to browser (public CDN URL)
process.env.NEXT_PUBLIC_DATA_URL

// ❌ NEVER - secret keys in browser
process.env.R2_SECRET_ACCESS_KEY
```

### Server-Side Only
No prefix, never exposed to client:
```typescript
// Server components and API routes only
process.env.R2_SECRET_ACCESS_KEY
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

### Path Traversal Prevention
When constructing R2 CDN paths from user input:
```typescript
// ✅ Safe - encode user input
const safeMuni = encodeURIComponent(municipality);
const url = `${DATA_URL}/municipalities/details/${safeMuni}_${category}.json`;

// ❌ Dangerous - raw interpolation
const url = `${DATA_URL}/municipalities/details/${municipality}_${category}.json`;
```

## XSS Prevention

React escapes by default. If HTML is needed:
```typescript
import DOMPurify from 'dompurify';
<div dangerouslySetInnerHTML={{ __html: DOMPurify.sanitize(content) }} />
```

## Code Review Security Checklist

- [ ] No hardcoded secrets
- [ ] Environment variables properly scoped
- [ ] User input validated
- [ ] No path traversal in R2 URL construction
- [ ] No XSS vectors
- [ ] No sensitive data in logs
