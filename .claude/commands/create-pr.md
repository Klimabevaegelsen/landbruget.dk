# Create Pull Request Command

Automate pull request creation with comprehensive description and proper linking.

## Usage

`/create-pr`

## What This Does

1. **Verify clean state**
   - All changes committed
   - Tests passing
   - Lint passing

2. **Analyze commits**
   - Read commit messages since branching
   - Identify Beads issues referenced
   - Categorize changes (feat, fix, refactor, etc.)

3. **Generate PR description**
   - Summary of changes
   - Link to Beads issues
   - Test plan
   - Screenshots (if UI changes)
   - Breaking changes (if any)

4. **Create PR**
   - Push branch to remote
   - Use `gh pr create` with generated description
   - Link to parent Beads issue
   - Add appropriate labels

## Pre-flight Checks

```bash
# 1. Verify tests pass
cd frontend && npm test
cd .. && uv run --all-packages --group dev pytest

# 2. Verify lint passes
cd frontend && npm run lint

# 3. Verify all changes committed
git status

# 4. Get commit history
git log main..HEAD --oneline
```

## PR Description Template

```markdown
## Summary
[Brief 2-3 sentence overview of changes]

## Changes
[Bullet list of key changes from commits]
- feat: Added X feature
- fix: Corrected Y bug
- refactor: Improved Z implementation

## Related Issues
[Link to Beads issues]
- Closes angkor-v1-{id}
- Relates to angkor-v1-{id}

## Test Plan
[How to test these changes]
- [ ] Run `npm test` - all tests pass
- [ ] Manual testing: [specific steps]
- [ ] Verified on [browser/device]

## Screenshots
[If UI changes, add screenshots]

## Breaking Changes
[If any, describe migration path]

## Checklist
- [x] Tests added/updated
- [x] Documentation updated
- [x] Lint passes
- [x] No secrets committed
- [x] Conventional commit messages

---
🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

## GitHub CLI Command

```bash
gh pr create \
  --title "feat: descriptive title from commits" \
  --body "$(cat pr-description.md)" \
  --base main \
  --head $(git branch --show-current)
```

## Post-Creation

1. **Copy PR URL** to clipboard
2. **Update Beads issues** with PR link
3. **Notify in chat** with PR URL
4. **Add reviewers** if specified

## Notes

- **Title**: Use conventional commit format (feat:, fix:, etc.)
- **Body**: Be comprehensive but concise
- **Links**: Always link related Beads issues
- **Context**: Provide enough for reviewer to understand changes
- **Testing**: Clear instructions for verification
