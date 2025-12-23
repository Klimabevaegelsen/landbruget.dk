# Tasks Template

_Add this section to the spec file after Phase 3: /tasks_

---

## Tasks

### Summary

| Phase | Tasks | Est. Time | Parallelizable |
|-------|-------|-----------|----------------|
| Foundation | X | Xh | Yes |
| Core | X | Xh | Partial |
| Testing | X | Xh | Yes |
| **Total** | **X** | **Xh** | |

### Phase 1: Foundation

_Tasks that set up the structure. Can often run in parallel._

- [ ] **Task 1** (S) - [Bead: xxx]
  - **Description**: [What to do]
  - **Files**:
    - `[file1.ts]` - New
    - `[file2.ts]` - Modify
  - **Acceptance**: [How to verify completion]
  - **Dependencies**: None

- [ ] **Task 2** (S) - [Bead: xxx]
  - **Description**: [What to do]
  - **Files**: `[file.ts]`
  - **Acceptance**: [How to verify]
  - **Dependencies**: None

### Phase 2: Core Implementation

_Main feature logic. Usually sequential._

- [ ] **Task 3** (M) - [Bead: xxx]
  - **Description**: [What to do]
  - **Files**:
    - `[file1.ts]`
    - `[file2.ts]`
  - **Acceptance**: [How to verify]
  - **Dependencies**: Task 1, Task 2

- [ ] **Task 4** (L) - [Bead: xxx]
  - **Description**: [What to do]
  - **Files**: [list files]
  - **Acceptance**: [How to verify]
  - **Dependencies**: Task 3

### Phase 3: Testing & Polish

_E2E tests and refinements. Can often run in parallel._

- [ ] **Task 5** (M) - [Bead: xxx]
  - **Description**: Write E2E tests for all acceptance criteria
  - **Files**: `e2e/[feature].spec.ts`
  - **Acceptance**: All AC covered, tests pass
  - **Dependencies**: Task 4

- [ ] **Task 6** (S) - [Bead: xxx]
  - **Description**: Add loading states and error handling
  - **Files**: [components]
  - **Acceptance**: [How to verify]
  - **Dependencies**: Task 4

### Phase 4: Database (If Applicable)

- [ ] **Task 7** (M) - [Bead: xxx]
  - **Description**: Create migration with tables and RLS
  - **Files**: `supabase/migrations/[timestamp].sql`
  - **Acceptance**: Migration applies, RLS works
  - **Dependencies**: None (can start early)

---

### Dependency Graph

```
           ┌──────────┐
           │  Task 7  │ (Database - independent)
           └──────────┘

┌──────────┐    ┌──────────┐
│  Task 1  │    │  Task 2  │  (Foundation - parallel)
└────┬─────┘    └────┬─────┘
     │               │
     └───────┬───────┘
             ▼
       ┌──────────┐
       │  Task 3  │  (Core - sequential)
       └────┬─────┘
            ▼
       ┌──────────┐
       │  Task 4  │
       └────┬─────┘
            │
     ┌──────┴──────┐
     ▼             ▼
┌──────────┐ ┌──────────┐
│  Task 5  │ │  Task 6  │  (Testing - parallel)
└──────────┘ └──────────┘
```

### Execution Order

**Recommended sequence for solo implementation:**

1. Start Task 7 (Database) - can run independently
2. Task 1 → Task 2 (or parallel if possible)
3. Task 3 → Task 4
4. Task 5 → Task 6 (or parallel)

**For parallel agents:**

| Agent 1 | Agent 2 |
|---------|---------|
| Task 1 | Task 2 |
| Task 3 | Task 7 |
| Task 4 | - |
| Task 5 | Task 6 |

---

### Task Size Guide

| Size | Time | Scope | Example |
|------|------|-------|---------|
| S | < 30 min | Single file, simple logic | Add type interface |
| M | 30-90 min | 2-3 files, moderate logic | Create component + hook |
| L | 90+ min | Multiple files, complex logic | Full feature implementation |

---

### Progress Tracking

| Task | Status | Started | Completed | Notes |
|------|--------|---------|-----------|-------|
| 1 | ⬜ Pending | | | |
| 2 | ⬜ Pending | | | |
| 3 | ⬜ Pending | | | |
| 4 | ⬜ Pending | | | |
| 5 | ⬜ Pending | | | |
| 6 | ⬜ Pending | | | |
| 7 | ⬜ Pending | | | |

**Status Legend:**
- ⬜ Pending
- 🔄 In Progress
- ✅ Complete
- ⚠️ Blocked

---

_Tasks created: [date]_
_Beads linked: [list bead IDs]_
