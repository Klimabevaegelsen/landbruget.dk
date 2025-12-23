# /tasks - Break Down Work

Derive executable tasks from the implementation plan. This is Phase 3 of the Spec-Driven Development workflow.

## Usage

`/tasks`

Requires: Active spec with Implementation Plan in `.claude/specs/active/`

## Process

1. **Read Implementation Plan**
   Load the spec file and parse the Implementation Plan section.

2. **Break Down into Atomic Tasks**
   Each task should be:
   - **Atomic**: Single responsibility
   - **Testable**: Clear pass/fail criteria
   - **Independent**: Minimal dependencies on other tasks
   - **Small**: Completable in one session (~1-2 hours)

3. **Identify Parallelizable Work**
   Group tasks that can be done concurrently.

4. **Estimate Complexity**
   - **S (Small)**: < 30 min, single file change
   - **M (Medium)**: 30-90 min, 2-3 files
   - **L (Large)**: 90+ min, multiple files/components

5. **Create Beads Issues**
   ```bash
   bd new "task: [description]"
   bd blocks [parent-id] [task-id]
   ```

6. **Update Spec File**
   Add the Tasks section to the spec.

## Tasks Template

Add this section to the spec file:

```markdown
## Tasks

### Phase 1: Foundation (Can run in parallel)

- [ ] **Task 1** (S) - [Bead: xxx]
  - Create base component structure
  - Add TypeScript interfaces
  - _Files: `Component.tsx`, `types.ts`_

- [ ] **Task 2** (S) - [Bead: xxx]
  - Set up Zustand store
  - Add initial state and actions
  - _Files: `store.ts`_

### Phase 2: Core Implementation (Sequential)

- [ ] **Task 3** (M) - [Bead: xxx]
  - Implement main feature logic
  - Connect to Supabase
  - _Depends on: Task 1, Task 2_
  - _Files: `useFeature.ts`, `service.ts`_

- [ ] **Task 4** (L) - [Bead: xxx]
  - Build UI components
  - Add styling and responsiveness
  - _Depends on: Task 3_
  - _Files: `FeatureView.tsx`, `FeatureList.tsx`_

### Phase 3: Testing & Polish (Can run in parallel)

- [ ] **Task 5** (M) - [Bead: xxx]
  - Write E2E tests
  - Cover all acceptance criteria
  - _Files: `feature.spec.ts`_

- [ ] **Task 6** (S) - [Bead: xxx]
  - Add loading states
  - Error handling
  - _Files: `Component.tsx`_

### Phase 4: Database (If needed)

- [ ] **Task 7** (M) - [Bead: xxx]
  - Create migration
  - Add RLS policies
  - _Files: `migrations/xxx.sql`_

### Dependency Graph

```
Task 1 ─┬─► Task 3 ─► Task 4 ─► Task 5
Task 2 ─┘                      └─► Task 6
Task 7 (independent)
```

### Execution Order

1. Start Task 1, Task 2, Task 7 in parallel
2. After Task 1 + 2 complete → Task 3
3. After Task 3 → Task 4
4. After Task 4 → Task 5, Task 6 in parallel
```

## Beads Integration

For each task, create a Bead issue:

```bash
# Create parent feature issue (if not exists)
bd new "feat: [feature name]"

# Create task issues
bd new "task: [task description]"

# Set dependencies
bd blocks [parent-id] [task-id]
```

## Task Quality Criteria

Good tasks have:
- Clear description of what to do
- Specific files to modify/create
- Defined dependencies
- Testable outcome
- Size estimate

## Output

After running `/tasks`:
1. Tasks section added to spec
2. Bead issues created for each task
3. Dependencies mapped
4. Execution order defined

## Next Step

Run `/implement [task-number]` to execute tasks with TDD validation.
