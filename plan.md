# Planning Scratchpad

**Purpose**: Draft detailed, human-readable implementation plans before breaking them down into Beads issues.

**Usage**: Architect agents write plans here. Human reviews and approves. Then convert to Beads issues.

---

## How to Use This File

### For Architect Agents

1. **Understand the feature request** from the user
2. **Ask clarifying questions** one at a time (200-300 word responses)
3. **Draft a comprehensive plan** in this file
4. **Wait for human approval**
5. **Convert plan to Beads issues** with dependencies

### For Implementer Agents

- **Don't write here** - this is for planning only
- **Read completed plans** for context if referenced
- **Execute tasks** from Beads, not from this file

### For Humans

- **Review plans** before approving
- **Provide feedback** and request changes
- **Approve** when ready
- **Signal agent** to convert to Beads

---

## Plan Template

```markdown
# Feature: [Name]

## Overview
[2-3 sentence description of what we're building and why]

## User Story
As a [user type]
I want to [action]
So that [benefit]

## Acceptance Criteria
- [ ] Criterion 1
- [ ] Criterion 2
- [ ] Criterion 3

## Technical Approach

### Architecture
[How this fits into existing system]

### Components
[New components or modifications needed]

### Data Model
[Database changes, if any]

### API Endpoints
[New or modified endpoints]

## Implementation Steps

### Phase 1: [Name]
1. Task 1
   - Details
   - Dependencies
2. Task 2
   - Details
   - Dependencies

### Phase 2: [Name]
1. Task 1
2. Task 2

## Testing Strategy
- Unit tests: [coverage areas]
- E2E tests: [user flows to test]
- Manual testing: [specific scenarios]

## Risks & Mitigations
- Risk 1: [description] → Mitigation: [approach]
- Risk 2: [description] → Mitigation: [approach]

## Timeline Estimate
- Phase 1: [X hours/days]
- Phase 2: [X hours/days]
- Total: [X hours/days]

## Dependencies
- Existing issue: [beads ID]
- External system: [name]
- Required data: [source]

## Success Metrics
[How we'll know this is working well]
```

---

## Current Plan

[When starting a new plan, clear everything below this line and start fresh]

---

## Plan History

When a plan is completed and converted to Beads, move it to the "Completed Plans" section at the bottom of this file for reference.
