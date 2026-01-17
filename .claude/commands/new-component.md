# New Component Command

Scaffold a new React component with all required files following project conventions.

## Usage

`/new-component ComponentName`

## What This Does

1. **Create Component File** (`frontend/src/components/ComponentName.tsx`)
   - Functional component with TypeScript
   - Props interface
   - Proper imports (React, cn utility)
   - Basic JSX structure with className support

2. **Create Test File** (`frontend/e2e/ComponentName.spec.ts`)
   - Playwright E2E test structure
   - Basic rendering test
   - data-testid attributes

3. **Update Exports** (if in a shared directory)
   - Add to index.ts if present

## Template Structure

### Component (`ComponentName.tsx`)
```typescript
import { ReactNode } from 'react'
import { cn } from '@/lib/utils'

interface ComponentNameProps {
  children?: ReactNode
  className?: string
}

export function ComponentName({
  children,
  className,
}: ComponentNameProps) {
  return (
    <div className={cn('component-name', className)} data-testid="component-name">
      {children}
    </div>
  )
}
```

### Test (`ComponentName.spec.ts`)
```typescript
import { test, expect } from '@playwright/test'

test.describe('ComponentName', () => {
  test('should render successfully', async ({ page }) => {
    await page.goto('/')

    const component = page.locator('[data-testid="component-name"]')
    await expect(component).toBeVisible()
  })
})
```

## Steps

1. Prompt for component name if not provided
2. Determine component location (ask if ambiguous)
3. Create component file with template
4. Create test file with template
5. Run `npm run lint` to verify
6. Create a Beads issue for implementation if complex
7. Report what was created

## Conventions to Follow

- Use functional components (not arrow functions)
- Include explicit Props interface
- Use `cn()` for className merging
- Add `data-testid` for testing
- Mobile-first responsive design
- Support dark mode with `dark:` prefix
- No business logic in components
