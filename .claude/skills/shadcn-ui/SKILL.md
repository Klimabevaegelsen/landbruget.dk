---
name: shadcn-ui
description: shadcn/ui component patterns — installation, usage, customization, cn utility, and CVA variants
---

# shadcn/ui Expert

> shadcn/ui is NOT an npm package — components are copied into your project and owned by you.

## Installation

```bash
# Initialize (first time)
pnpm dlx shadcn@latest init

# Add a component
pnpm dlx shadcn@latest add button
pnpm dlx shadcn@latest add dialog
pnpm dlx shadcn@latest add form
```

Components land in `src/components/ui/`. Import from there:

```typescript
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
```

## cn() Utility

Always at `@/lib/utils`. Merges Tailwind classes safely (handles conflicts):

```typescript
import { cn } from '@/lib/utils'

// cn combines clsx + tailwind-merge
<div className={cn('px-4 py-2', isActive && 'bg-primary', className)} />
```

## Button

```tsx
import { Button } from '@/components/ui/button'

// Variants: default | destructive | outline | secondary | ghost | link
// Sizes: default | sm | lg | icon | xs | icon-xs | icon-sm | icon-lg

<Button variant="default">Click me</Button>
<Button variant="outline" size="sm">Cancel</Button>
<Button variant="ghost" size="icon"><TrashIcon /></Button>

// asChild — render as a different element (e.g. Link)
import Link from 'next/link'
<Button asChild>
  <Link href="/dashboard">Go to Dashboard</Link>
</Button>
```

## Form Pattern (react-hook-form + zod)

```tsx
'use client'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'

const schema = z.object({ email: z.string().email() })

export function MyForm() {
  const form = useForm({ resolver: zodResolver(schema) })

  return (
    <Form {...form}>
      <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
        <FormField
          control={form.control}
          name="email"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Email</FormLabel>
              <FormControl>
                <Input placeholder="you@example.com" {...field} />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />
        <Button type="submit">Submit</Button>
      </form>
    </Form>
  )
}
```

## Dialog

```tsx
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'

<Dialog>
  <DialogTrigger asChild>
    <Button variant="outline">Open</Button>
  </DialogTrigger>
  <DialogContent>
    <DialogHeader>
      <DialogTitle>Title</DialogTitle>
    </DialogHeader>
    <p>Content here</p>
  </DialogContent>
</Dialog>
```

## Custom Variants with CVA

When extending a shadcn component, use `cva`:

```typescript
import { cva, type VariantProps } from 'class-variance-authority'
import { cn } from '@/lib/utils'

const badgeVariants = cva(
  'inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium transition-colors',
  {
    variants: {
      variant: {
        default: 'bg-primary text-primary-foreground',
        secondary: 'bg-secondary text-secondary-foreground',
        success: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200',
      },
    },
    defaultVariants: { variant: 'default' },
  }
)

interface BadgeProps extends React.HTMLAttributes<HTMLDivElement>, VariantProps<typeof badgeVariants> {}

export function Badge({ className, variant, ...props }: BadgeProps) {
  return <div className={cn(badgeVariants({ variant }), className)} {...props} />
}
```

## Theming

shadcn/ui uses CSS variables defined in `globals.css`. With Tailwind v4:

```css
@theme {
  --background: oklch(1 0 0);
  --foreground: oklch(0.145 0 0);
  --card: oklch(1 0 0);
  --card-foreground: oklch(0.145 0 0);
  --primary: oklch(0.205 0 0);
  --primary-foreground: oklch(0.985 0 0);
  --secondary: oklch(0.97 0 0);
  --secondary-foreground: oklch(0.205 0 0);
  --muted: oklch(0.97 0 0);
  --muted-foreground: oklch(0.556 0 0);
  --accent: oklch(0.97 0 0);
  --accent-foreground: oklch(0.205 0 0);
  --destructive: oklch(0.577 0.245 27.325);
  --border: oklch(0.922 0 0);
  --input: oklch(0.922 0 0);
  --ring: oklch(0.708 0 0);
  --radius: 0.625rem;
}
```

## Common Components Reference

| Component | Import |
|-----------|--------|
| Button | `@/components/ui/button` |
| Input | `@/components/ui/input` |
| Textarea | `@/components/ui/textarea` |
| Select | `@/components/ui/select` |
| Checkbox | `@/components/ui/checkbox` |
| Dialog | `@/components/ui/dialog` |
| Sheet | `@/components/ui/sheet` |
| Dropdown Menu | `@/components/ui/dropdown-menu` |
| Toast / Toaster | `@/components/ui/toast` |
| Card | `@/components/ui/card` |
| Badge | `@/components/ui/badge` |
| Skeleton | `@/components/ui/skeleton` |
| Separator | `@/components/ui/separator` |
| Avatar | `@/components/ui/avatar` |
| Tabs | `@/components/ui/tabs` |
| Form | `@/components/ui/form` |

## Rules

- Always use `cn()` when composing class names — never string concatenate Tailwind classes
- Pass `className` as last prop to allow callers to extend styles
- Use `asChild` to compose with Next.js `<Link>` or other elements
- Components are yours — edit them freely, don't reinstall to override
- Use `pnpm dlx shadcn@latest add` (not `shadcn-ui` — that's the old package name)
