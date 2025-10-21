# Fix Lint Command

Run oxlint, analyze errors, and systematically fix all linting issues.

## Usage

`/fix-lint`

## What This Does

1. **Run oxlint** in frontend directory
2. **Parse errors** and categorize by severity
3. **Create checklist** of issues to fix
4. **Fix each issue** one by one
5. **Re-run lint** after each fix to verify
6. **Report summary** when complete

## Process

### Step 1: Run Lint
```bash
cd frontend && npm run lint
```

### Step 2: Analyze Output
- Count total errors and warnings
- Group by rule type
- Identify patterns

### Step 3: Systematic Fixes
For each error:
1. Read the file and understand context
2. Fix the specific issue
3. Run lint again to verify fix
4. Move to next error

### Step 4: Final Verification
```bash
npm run lint        # Must pass with 0 errors
npm run format      # Format code
npm test            # Ensure tests still pass
```

## Common oxlint Rules to Fix

### TypeScript Issues
- `@typescript-eslint/no-explicit-any` - Replace `any` with proper types
- `@typescript-eslint/no-unused-vars` - Remove or use variables
- `@typescript-eslint/no-non-null-assertion` - Add proper null checks

### React Issues
- `react/no-unescaped-entities` - Escape quotes in JSX
- `react-hooks/exhaustive-deps` - Add missing dependencies
- `react/jsx-key` - Add keys to list items

### Import Issues
- `import/order` - Organize imports correctly
- `import/no-duplicates` - Merge duplicate imports

### Security Issues
- `no-eval` - Never use eval()
- `no-implied-eval` - Avoid Function() constructor

## Priority Order

1. **Critical security issues** (eval, XSS vulnerabilities)
2. **Type safety issues** (any, non-null assertions)
3. **React best practices** (hooks, keys, escaping)
4. **Code organization** (imports, unused vars)
5. **Stylistic issues** (formatting, naming)

## Notes

- **Never skip errors** - fix all issues
- **Don't disable rules** - fix the underlying problem
- **Test after fixing** - ensure nothing breaks
- **Commit atomically** - one logical group of fixes per commit
