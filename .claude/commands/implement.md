# /implement - Execute with Validation

Execute tasks with TDD and continuous validation. This is Phase 4 of the Spec-Driven Development workflow.

## Usage

`/implement [task-number]`
`/implement` (continues with next incomplete task)

Example: `/implement 3`

Requires: Active spec with Tasks in `.claude/specs/active/`

## Process

### 1. Load Task

Read the spec and identify the target task:
- Check task dependencies are complete
- Load task description and files
- Understand acceptance criteria

### 2. Write Tests FIRST (TDD)

**For Frontend (Playwright E2E):**
```typescript
// e2e/feature-name.spec.ts
import { test, expect } from '@playwright/test';

test.describe('Feature Name', () => {
  test('should [acceptance criterion 1]', async ({ page }) => {
    // Arrange
    await page.goto('/');

    // Act
    await page.click('[data-testid="feature-button"]');

    // Assert
    await expect(page.locator('[data-testid="result"]')).toBeVisible();
  });
});
```

**For Backend (Pytest):**
```python
# tests/test_feature.py
import pytest
from feature import process_data

def test_acceptance_criterion_1():
    # Arrange
    input_data = {...}

    # Act
    result = process_data(input_data)

    # Assert
    assert result['status'] == 'success'
```

### 3. Run Tests - Confirm They Fail

```bash
# Frontend
cd frontend && npm test -- --grep "Feature Name"

# Backend
uv run --all-packages --group dev pytest backend/tests/test_feature.py -v
```

**IMPORTANT**: Tests MUST fail before implementation. If they pass, the test is invalid.

### 4. Implement Minimum Code

Write only the code needed to make tests pass:
- No premature optimization
- No extra features
- Follow existing patterns in codebase

### 5. Run Tests - Confirm They Pass

```bash
# Frontend
cd frontend && npm test

# Backend
uv run --all-packages --group dev pytest
```

### 6. Verify Acceptance Criteria

Cross-check each criterion from the spec:
- [ ] Criterion 1: Implemented and tested
- [ ] Criterion 2: Implemented and tested
- [ ] Criterion N: Implemented and tested

### 7. Run Full Validation

```bash
# Linting
cd frontend && npm run lint

# Type checking (implicit in build)
cd frontend && npm run build

# All tests
cd frontend && npm test
uv run --all-packages --group dev pytest
```

### 8. Update Spec

Mark task as complete in the spec file:
```markdown
- [x] **Task 3** (M) - [Bead: xxx] ✅ Completed [date]
```

### 9. Commit with Spec Reference

```bash
git add .
git commit -m "feat(scope): implement [task description]

Implements Task 3 from spec: [feature-name]
- Added [component/function]
- Tests: [number] passing

Closes Bead: [bead-id]

Co-Authored-By: Claude <noreply@anthropic.com>"
```

### 10. Mark Bead Complete

```bash
bd done [task-bead-id]
```

## Validation Gates

Every `/implement` must pass these gates:

| Gate | Command | Required |
|------|---------|----------|
| Tests written first | - | Yes |
| Tests fail initially | `npm test` / `uv run --all-packages --group dev pytest` | Yes |
| Implementation complete | - | Yes |
| Tests pass | `npm test` / `uv run --all-packages --group dev pytest` | Yes |
| Lint passes | `npm run lint` | Yes |
| No regressions | Full test suite | Yes |
| Spec criteria met | Manual check | Yes |

## TDD Workflow Diagram

```
Write Test → Run Test → Test Fails?
                            ↓ Yes
                     Write Min Code
                            ↓
                     Run Test → Test Passes?
                                    ↓ Yes
                             Refactor (optional)
                                    ↓
                             Commit & Update Spec
```

## Common Patterns

### Frontend Component
```typescript
// 1. Test first
test('renders feature component', async ({ page }) => {
  await page.goto('/feature');
  await expect(page.locator('[data-testid="feature"]')).toBeVisible();
});

// 2. Implement
export function Feature({ data }: FeatureProps) {
  return <div data-testid="feature">{/* ... */}</div>;
}
```

### Backend Pipeline
```python
# 1. Test first
def test_transform_valid_data():
    result = transform(valid_input)
    assert result['cvr'] == '12345678'

# 2. Implement
def transform(data: pd.DataFrame) -> pd.DataFrame:
    data['cvr'] = data['cvr'].str.zfill(8)
    return data
```

## Output

After running `/implement`:
1. Tests written and passing
2. Implementation complete
3. Lint passing
4. Spec updated with completion
5. Commit created with spec reference
6. Bead marked done

## Next Task

Run `/implement` again to continue with next incomplete task, or `/implement [number]` for a specific task.

## Completing the Feature

When all tasks are done:
1. All Bead tasks marked complete
2. Parent Bead marked complete
3. Move spec to `.claude/specs/archive/`
4. Create PR with `/create-pr`
