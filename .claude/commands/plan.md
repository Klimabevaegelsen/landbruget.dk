# /plan - Technical Architecture

Convert the active specification into an implementation plan. This is Phase 2 of the Spec-Driven Development workflow.

## Usage

`/plan`

Requires: Active spec in `.claude/specs/active/`

## Process

1. **Read Active Spec**
   ```bash
   ls .claude/specs/active/
   ```
   Read the most recent or specified spec file.

2. **Analyze Codebase**
   - Identify affected files
   - Find existing patterns to follow
   - Check for potential conflicts

3. **Design Technical Approach**
   - Choose architecture patterns
   - Define data models
   - Design API contracts
   - Plan test strategy

4. **Validate Against Conventions**
   - Check CLAUDE.md rules
   - Ensure React 19 patterns
   - Verify TypeScript strict mode compliance

5. **Update Spec File**
   Add the Implementation Plan section to the active spec.

## Implementation Plan Template

Add this section to the spec file:

```markdown
## Implementation Plan

### Technical Approach

[High-level description of how the feature will be built]

### Affected Files

| File | Change Type | Description |
|------|-------------|-------------|
| `frontend/src/components/X.tsx` | New | Component for... |
| `frontend/src/hooks/useX.ts` | New | Hook for... |
| `backend/pipelines/x/main.py` | Modify | Add support for... |
| `supabase/migrations/xxx.sql` | New | Add table for... |

### Data Model Changes

```sql
-- New tables or modifications
CREATE TABLE IF NOT EXISTS ...
```

### API Contract

```typescript
// Types and interfaces
interface FeatureRequest {
  // ...
}

interface FeatureResponse {
  // ...
}
```

### Component Architecture

```
ParentComponent
├── ChildComponent1
│   └── SubComponent
└── ChildComponent2
```

### State Management

- Local state: [what uses useState]
- Global state: [what uses Zustand]
- Server state: [what uses Supabase queries]

### Test Strategy

**Unit Tests:**
- [ ] Test case 1
- [ ] Test case 2

**E2E Tests:**
- [ ] User flow 1
- [ ] User flow 2

### Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| [Risk 1] | Medium | High | [Strategy] |

### Dependencies

- [ ] Dependency 1 must be completed first
- [ ] External API access required

### Estimated Complexity

- **Overall**: S / M / L / XL
- **Frontend**: [estimate]
- **Backend**: [estimate]
- **Database**: [estimate]
```

## Validation Checklist

Before completing `/plan`:
- [ ] All acceptance criteria have implementation approach
- [ ] Affected files identified
- [ ] Test strategy covers all criteria
- [ ] No CLAUDE.md rule violations
- [ ] Dependencies identified
- [ ] Risks assessed

## Output

After running `/plan`:
1. Implementation Plan section added to spec
2. Affected files listed
3. Test strategy defined
4. Complexity estimated

## Next Step

Once plan is approved, run `/tasks` to break down into atomic tasks.
