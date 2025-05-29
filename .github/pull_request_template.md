## 🛠️ Issue Fix
<!-- Describe the issue this PR fixes -->
Fixes #[ISSUE_NUMBER]  
<!-- If there's no issue, you can write "No issue linked" -->

## 💡 Solution
<!-- Briefly describe the solution or changes introduced in this PR -->
- [ ] Explain what was changed and why
- [ ] Mention any relevant decisions or trade-offs

## ✅ Code Quality Checklist
Before submitting this PR, please ensure you've completed the following:

### Backend Changes (if applicable)
- [ ] **Formatted code**: Run `make format` in the relevant pipeline directory
- [ ] **Linting passed**: Run `make lint` with no errors  
- [ ] **Type checking**: Run `make mypy` (warnings are OK for now)
- [ ] **Tests pass**: Run `make test` if tests exist
- [ ] **All quality checks**: Run `make check` for comprehensive validation

### Frontend Changes (if applicable)
- [ ] **Code formatted**: Run `npm run format` in the `frontend/` directory
- [ ] **Linting passed**: Run `npm run lint` in the `frontend/` directory
- [ ] **TypeScript compiles**: Run `npm run build` to check for type errors
- [ ] **Code follows conventions**: Ensure consistent formatting and naming

### General
- [ ] **No console.log/print statements**: Remove debugging statements
- [ ] **Meaningful commit messages**: Follow conventional commit format if used
- [ ] **Documentation updated**: Update relevant README or docs if needed

_These checks will also run automatically in CI, but running them locally saves time!_

## ✅ How to Do Self-Test
<!-- Provide instructions for how to test the changes, can be copied from README -->

## 📷 Screenshots (if applicable)
<!-- Add any relevant screenshots for UI changes -->

## 📝 Additional Notes
<!-- Add any other context or information reviewers should know -->
