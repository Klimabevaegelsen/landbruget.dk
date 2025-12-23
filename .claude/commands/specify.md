# /specify - Capture Feature Intent

Transform a feature description into a structured specification. This is Phase 1 of the Spec-Driven Development workflow.

## Usage

`/specify <feature-description>`

Example: `/specify Add a map filter for crop types`

## Process

1. **Create Feature Branch**
   ```bash
   git checkout -b feat/$ARGUMENTS
   ```

2. **Clarify Requirements**
   Ask questions to understand:
   - What problem does this solve?
   - Who are the users?
   - What are the success criteria?
   - What are the constraints?

3. **Generate Specification**
   Create `.claude/specs/active/<feature-name>.md` using the template below.

4. **Mark Uncertainties**
   Use `[NEEDS CLARIFICATION]` for any ambiguous requirements.

5. **Create Parent Bead**
   ```bash
   bd new "feat: $ARGUMENTS"
   ```

## Specification Template

Create the spec file with this structure:

```markdown
# Feature: [Feature Name]

**Status**: Draft | In Review | Approved
**Created**: [Date]
**Spec ID**: [auto-generated]

## Problem Statement

[What problem does this solve? Why is it important?]

## User Stories

- As a [user type], I want [goal] so that [benefit]
- As a [user type], I want [goal] so that [benefit]

## Acceptance Criteria

- [ ] Given [context], when [action], then [expected result]
- [ ] Given [context], when [action], then [expected result]
- [ ] Given [context], when [action], then [expected result]

## Constraints

- Technical: [e.g., Must work with React 19, Supabase]
- Performance: [e.g., <200ms response time]
- Security: [e.g., RLS policies required]
- Compatibility: [e.g., Mobile-responsive]

## Out of Scope

- [Explicitly excluded functionality]
- [Future enhancements not in this spec]

## Open Questions

- [NEEDS CLARIFICATION] [Question 1]
- [NEEDS CLARIFICATION] [Question 2]

## Dependencies

- [Other features or systems this depends on]

## Risks

- [Potential risks and mitigation strategies]

---

## Implementation Plan

_To be filled in Phase 2: /plan_

## Tasks

_To be filled in Phase 3: /tasks_
```

## Output

After running `/specify`:
1. Feature branch created
2. Spec file in `.claude/specs/active/`
3. Parent Bead issue created
4. Questions asked for any unclear requirements

## Next Step

Once spec is approved, run `/plan` to create the technical implementation plan.
