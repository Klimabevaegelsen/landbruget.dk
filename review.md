# Code Review Scratchpad

**Purpose**: Architect agents write code reviews here. Implementer agents read and address feedback.

**Usage**: Enables asynchronous review/fix cycles between Architect and Implementer agents.

---

## How to Use This File

### For Architect Agents

1. **Review commits** since last review
2. **Check for**:
   - Architectural compliance
   - Test coverage
   - Code quality
   - Security issues
   - Performance concerns
3. **Write detailed feedback** below
4. **Categorize** as Required, Suggested, or Nit
5. **Tag specific files/lines** for clarity

### For Implementer Agents

1. **Read entire review** carefully
2. **Address all "Required" items** first
3. **Consider "Suggested" items** - implement if straightforward
4. **Optional: Address "Nits"** if time permits
5. **Create commits** for each logical group of fixes
6. **Reply below** each item when addressed
7. **Request re-review** when complete

### For Humans

- **Final approval** after agents iterate
- **Resolve disputes** between agents
- **Add context** agents might be missing

---

## Review Template

```markdown
# Review: [Feature/PR Name]

**Reviewer**: [Architect Agent | Human]
**Reviewed Commits**: [commit hash range]
**Date**: [YYYY-MM-DD]

## Overall Assessment
[High-level feedback: approve with changes, needs work, approve as-is]

## Critical Issues (🔴 REQUIRED)
Issues that MUST be fixed before merge.

### Issue 1: [Title]
**File**: `path/to/file.ts:42`
**Problem**: [What's wrong]
**Impact**: [Why this matters]
**Fix**: [Specific action to take]

**Status**: [ ] Not started | [ ] In progress | [x] Fixed

**Implementer response**:
[Agent writes here when fixed, include commit hash]

---

### Issue 2: [Title]
...

## Suggestions (🟡 RECOMMENDED)
Good ideas that would improve quality but aren't blockers.

### Suggestion 1: [Title]
**File**: `path/to/file.ts:100`
**Current**: [How it is now]
**Better**: [Suggested improvement]
**Why**: [Rationale]

**Implementer response**:
[Addressed | Deferred with reason | Won't fix with reason]

---

## Nits (⚪ OPTIONAL)
Minor style or formatting issues.

- [ ] `file.ts:50` - Extra whitespace
- [ ] `component.tsx:23` - Could use more descriptive variable name

---

## Positive Feedback ✨
Things done well (agents should know what patterns to repeat).

- ✅ Excellent test coverage on the DataTable component
- ✅ Clean separation of concerns in the map utilities
- ✅ Proper TypeScript types throughout

---

## Next Steps

**For Implementer**:
1. Fix all Critical Issues
2. Address Suggestions where feasible
3. Run full test suite
4. Run lint
5. Reply to each item
6. Request re-review

**For Architect**:
[Any additional context or guidance]

```

---

## Current Review

[Architect agent: Write your review here]

---

## Review History

When a review cycle is complete, move it to the "Past Reviews" section at the bottom for reference.

### Past Reviews

[Completed reviews get archived here]
