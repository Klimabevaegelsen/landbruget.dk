# Testing Checklist - Natural Language to SQL Feature

## Pre-Testing Setup

- [ ] Create `.env.local` file from `.env.local.example`
- [ ] Add valid `GOOGLE_API_KEY` from [Google AI Studio](https://aistudio.google.com/app/apikey)
- [ ] Verify API key has access to Gemini 2.0 models
- [ ] Start development server: `npm run dev`

## Component Testing

### AskInput Component

- [ ] Component renders without errors
- [ ] Input field accepts text
- [ ] Placeholder text is visible
- [ ] "Ask" button is initially disabled (no text)
- [ ] "Ask" button enables when text is entered
- [ ] Enter key submits the form
- [ ] Shift+Enter creates a new line
- [ ] Example questions are displayed
- [ ] Component is disabled when no dataset is selected

### Loading States

- [ ] Loading spinner appears when question is submitted
- [ ] "Ask" button shows "Thinking..." during loading
- [ ] Input is disabled during loading
- [ ] Button is disabled during loading

### Success Flow

- [ ] Green success message appears after successful generation
- [ ] SQL query is displayed in the success message
- [ ] Explanation is displayed in the success message
- [ ] Success message is dismissible (× button)
- [ ] Generated SQL appears in the SQL editor below
- [ ] User can edit the generated SQL before running

### Error Handling

- [ ] Red error message appears on API failure
- [ ] Error message is dismissible (× button)
- [ ] Rate limit errors (429) show user-friendly message
- [ ] Network errors show appropriate message
- [ ] Invalid API key (401) shows configuration error
- [ ] Missing environment variable (503) shows setup error

## API Route Testing

### Manual Testing with curl

```bash
# Test successful query
curl -X POST http://localhost:3000/api/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "Show me the top 10 farms"}'

# Expected response:
# {
#   "sql": "SELECT * FROM farms ORDER BY ...",
#   "explanation": "..."
# }

# Test missing question
curl -X POST http://localhost:3000/api/ask \
  -H "Content-Type: application/json" \
  -d '{}'

# Expected: 400 error

# Test empty question
curl -X POST http://localhost:3000/api/ask \
  -H "Content-Type: application/json" \
  -d '{"question": ""}'

# Expected: 400 error
```

### Response Validation

- [ ] API returns 200 for valid questions
- [ ] Response has `sql` field with valid SQL
- [ ] Response has `explanation` field with text
- [ ] SQL starts with SELECT keyword
- [ ] SQL is properly formatted
- [ ] API rejects non-SELECT queries (if generated)

### Error Scenarios

- [ ] Missing API key returns 503
- [ ] Invalid API key returns 401
- [ ] Malformed JSON returns 400
- [ ] Empty question returns 400
- [ ] Rate limit exceeded returns 429

## Integration Testing

### End-to-End User Flow

1. [ ] Navigate to `/example` page
2. [ ] Select a dataset from the left sidebar
3. [ ] Type a natural language question
4. [ ] Click "Ask" or press Enter
5. [ ] Wait for response (1-3 seconds)
6. [ ] Verify SQL appears in editor
7. [ ] Verify explanation is clear
8. [ ] Edit SQL if needed
9. [ ] Click "Run Query"
10. [ ] Verify results appear in table

### Example Questions to Test

- [ ] "Show me the top 10 farms by land area"
- [ ] "What are the most common crop types?"
- [ ] "List all farms in Region Midtjylland"
- [ ] "How many organic farms are there?"
- [ ] "Count farms by region"
- [ ] "Find all fields larger than 100 hectares"
- [ ] "Show me farms with CVR number 12345678"
- [ ] Ambiguous question: "Tell me about farms"
- [ ] Geographic query: "Find farms near Copenhagen"

### Edge Cases

- [ ] Very long question (200+ characters)
- [ ] Question with special characters
- [ ] Question in Danish language
- [ ] Question requesting non-SELECT operations
- [ ] Question about non-existent tables
- [ ] Rapid successive queries (rate limiting)

## Performance Testing

- [ ] Initial load time < 500ms
- [ ] API response time 1-3 seconds (typical)
- [ ] No memory leaks after multiple queries
- [ ] UI remains responsive during API calls
- [ ] Multiple queries in quick succession handled gracefully

## Browser Compatibility

- [ ] Chrome (latest)
- [ ] Firefox (latest)
- [ ] Safari (latest)
- [ ] Edge (latest)
- [ ] Mobile Safari (iOS)
- [ ] Chrome Mobile (Android)

## Accessibility

- [ ] Keyboard navigation works
- [ ] Enter key submits form
- [ ] Tab order is logical
- [ ] Error messages are announced (screen readers)
- [ ] Success messages are announced (screen readers)
- [ ] Focus management after submission

## Security Testing

- [ ] API key not exposed in browser DevTools
- [ ] API key not in Network tab requests
- [ ] Generated SQL is sanitized
- [ ] No XSS vulnerabilities in error messages
- [ ] No sensitive data in error messages
- [ ] Non-SELECT queries are rejected

## Documentation Review

- [ ] README instructions are clear
- [ ] Setup steps are complete
- [ ] Environment variables documented
- [ ] Example questions provided
- [ ] Error messages explained
- [ ] Limitations documented

## Known Issues to Verify

1. **Schema Awareness**
   - [ ] Model suggests table names that don't exist
   - [ ] Model guesses column names
   - [ ] User must verify SQL before execution

2. **Rate Limiting**
   - [ ] Free tier: 15 requests/minute enforced
   - [ ] Clear error message on limit reached
   - [ ] Suggestion to wait provided

3. **Query Accuracy**
   - [ ] Some queries may need manual refinement
   - [ ] Complex joins may be incorrect
   - [ ] Geographic queries may need adjustment

## Regression Testing

### Existing Features Still Work

- [ ] Dataset browser loads datasets
- [ ] SQL editor accepts manual queries
- [ ] "Run Query" button executes queries
- [ ] Results table displays data
- [ ] Manual SQL editing works
- [ ] Keyboard shortcuts (Cmd+Enter) work

## Production Readiness Checklist

- [ ] All tests above pass
- [ ] No console errors
- [ ] No console warnings (except known ones)
- [ ] API costs estimated and acceptable
- [ ] Rate limiting strategy defined
- [ ] Error monitoring configured
- [ ] User feedback collection planned

## Post-Deployment Monitoring

- [ ] API success rate tracked
- [ ] API latency monitored
- [ ] Error rates logged
- [ ] User satisfaction measured
- [ ] Common query patterns identified
- [ ] Failed queries analyzed

## Notes

- Test with actual data when available
- Document any new issues discovered
- Update system instruction based on failures
- Consider A/B testing different prompts
