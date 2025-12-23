# Spec-Driven Development (SDD) Workflow

This directory contains feature specifications following the Spec-Driven Development methodology.

## Philosophy

> Specifications don't serve code—code serves specifications.

The spec becomes the source of truth that generates implementation. This ensures:
- Clear requirements before coding
- Traceable changes to user needs
- Living documentation that stays current
- Consistent quality across features

## Directory Structure

```
.claude/specs/
├── README.md           # This file
├── templates/          # Templates for each phase
│   ├── specify.md      # Feature specification template
│   ├── plan.md         # Implementation plan template
│   └── tasks.md        # Task breakdown template
├── active/             # Specs currently being implemented
└── archive/            # Completed specs (for reference)
```

## Four-Phase Workflow

### Phase 1: `/specify` - Capture Intent
Transform a feature description into a structured specification.

```bash
/specify <feature-description>
```

**Creates:**
- Feature branch
- Spec file in `active/`
- Parent Bead issue

### Phase 2: `/plan` - Technical Architecture
Convert the specification into an implementation plan.

```bash
/plan
```

**Adds to spec:**
- Affected files
- Data model changes
- API contracts
- Test strategy

### Phase 3: `/tasks` - Break Down Work
Derive executable tasks from the plan.

```bash
/tasks
```

**Creates:**
- Atomic task list
- Bead issues for each task
- Dependency graph
- Execution order

### Phase 4: `/implement` - Execute with Validation
Execute tasks using TDD with continuous validation.

```bash
/implement [task-number]
```

**Ensures:**
- Tests written first
- Tests fail before implementation
- Tests pass after implementation
- Lint passes
- Spec criteria met

## Workflow Diagram

```
    ┌─────────────────────────────────────────────────────┐
    │                  SPECIFY PHASE                       │
    │  /specify "Add crop filter"                         │
    │  → Creates spec file + feature branch + Bead        │
    └─────────────────────────────────────────────────────┘
                            │
                            ▼
    ┌─────────────────────────────────────────────────────┐
    │                   PLAN PHASE                         │
    │  /plan                                               │
    │  → Analyzes codebase, designs architecture          │
    │  → Adds Implementation Plan to spec                 │
    └─────────────────────────────────────────────────────┘
                            │
                            ▼
    ┌─────────────────────────────────────────────────────┐
    │                  TASKS PHASE                         │
    │  /tasks                                              │
    │  → Breaks down into atomic tasks                    │
    │  → Creates Bead issues for tracking                 │
    └─────────────────────────────────────────────────────┘
                            │
                            ▼
    ┌─────────────────────────────────────────────────────┐
    │                IMPLEMENT PHASE                       │
    │  /implement 1   (repeat for each task)              │
    │  → TDD: Test first → Fail → Implement → Pass       │
    │  → Updates spec, commits, marks Bead done          │
    └─────────────────────────────────────────────────────┘
                            │
                            ▼
    ┌─────────────────────────────────────────────────────┐
    │                  COMPLETION                          │
    │  → All tasks done                                   │
    │  → Move spec to archive/                            │
    │  → /create-pr                                       │
    └─────────────────────────────────────────────────────┘
```

## Constitutional Rules

These rules are enforced via hooks and validation:

1. **Test-First Imperative**
   No implementation code before tests exist and fail.

2. **Spec Traceability**
   All code changes must reference a spec.

3. **No Premature Abstraction**
   YAGNI - implement only what's needed now.

4. **Living Documentation**
   Specs update as implementation reveals new information.

5. **Gated Progression**
   Cannot advance to next phase until current phase is validated.

## Beads Integration

SDD integrates with the Beads task management system:

| SDD Phase | Bead Action |
|-----------|-------------|
| `/specify` | Creates parent feature issue |
| `/tasks` | Creates child task issues |
| `/implement` | Links commits to tasks |
| Completion | Auto-marks Beads done |

## Quick Reference

| Command | Phase | Purpose |
|---------|-------|---------|
| `/specify <desc>` | 1 | Create specification |
| `/plan` | 2 | Design architecture |
| `/tasks` | 3 | Break into tasks |
| `/implement [n]` | 4 | Execute with TDD |

## Tips

1. **Start small**: First few specs should be simple features
2. **Iterate**: Update specs as you learn more
3. **Be specific**: Vague specs lead to vague implementations
4. **Test first**: Never skip the TDD step
5. **Reference specs**: Include spec ID in commits

## Example

```bash
# Start a new feature
/specify "Add filter for crop types on map"

# After spec is written and approved
/plan

# After plan is approved
/tasks

# Implement each task
/implement 1
/implement 2
/implement 3

# When all tasks done
/create-pr
```
