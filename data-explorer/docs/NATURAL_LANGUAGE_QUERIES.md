# Natural Language to SQL Conversion

This feature allows users to ask questions about the agricultural data in natural language, and the system will automatically generate and execute appropriate SQL queries.

## Architecture

### Components

1. **AskInput Component** (`src/components/AskInput.tsx`)
   - Chat-style input interface for natural language questions
   - Handles loading states and error display
   - Shows generated SQL and explanation
   - Passes generated SQL to the SQL editor for review/editing

2. **API Route** (`src/app/api/ask/route.ts`)
   - Next.js API route handler (POST `/api/ask`)
   - Uses Google Gemini 2.0 Flash model
   - Converts natural language to DuckDB SQL
   - Returns both SQL query and explanation

### Data Flow

```
User Question → AskInput → /api/ask → Gemini API → SQL Generation → SQLEditor → DuckDB → Results
```

## Setup

### Environment Variables

Add to your `.env.local`:

```bash
GOOGLE_API_KEY=your-google-api-key-here
```

Get your API key from [Google AI Studio](https://aistudio.google.com/app/apikey).

### Integration

The `AskInput` component is designed to work alongside the `SQLEditor`:

```tsx
import { AskInput } from "@/components/AskInput";
import { SQLEditor } from "@/components/SQLEditor";

function DataExplorer() {
  const [currentQuery, setCurrentQuery] = useState("");

  return (
    <>
      <AskInput onSqlGenerated={setCurrentQuery} />
      <SQLEditor initialQuery={currentQuery} onExecute={handleExecute} />
    </>
  );
}
```

## Usage

### Example Questions

- "Show me the top 10 farms by land area"
- "What are the most common crop types?"
- "List all farms in Region Midtjylland"
- "How many organic farms are there?"
- "Find all fields larger than 100 hectares"

### Response Format

The API returns:

```json
{
  "sql": "SELECT * FROM farms ORDER BY land_area DESC LIMIT 10;",
  "explanation": "This query selects all farms and sorts them by land area in descending order, limiting to the top 10 results."
}
```

## Security

### Query Restrictions

The system enforces several security measures:

1. **SELECT-only queries**: Only `SELECT` statements are allowed. Any other SQL operations (INSERT, UPDATE, DELETE, DROP, CREATE) are rejected.
2. **Server-side validation**: The API validates that generated queries start with SELECT before returning them to the client.
3. **API key security**: The Google API key is stored server-side and never exposed to the browser.

### Rate Limiting

The API handles rate limiting gracefully:

- Returns HTTP 429 for quota/rate limit errors
- Provides user-friendly error messages
- Suggests retry timing

## Technical Details

### Gemini Model Configuration

- **Model**: `gemini-2.0-flash-exp`
- **Tools**: Google Search Retrieval with dynamic mode
- **System Instruction**: Comprehensive prompt engineering for SQL generation

### Key Features

1. **Context-aware**: Understands Danish agricultural data structure (CVR, CHR, BFE identifiers)
2. **DuckDB dialect**: Generates SQL compatible with DuckDB's PostgreSQL-like syntax
3. **Safety defaults**: Always includes LIMIT clauses for performance
4. **Explanation**: Provides human-readable explanation of what the query does

### Error Handling

The API handles multiple error scenarios:

- **400**: Invalid request (missing question, non-SELECT query)
- **401**: Authentication failed (invalid API key)
- **429**: Rate limit exceeded
- **500**: AI response parsing errors or generation failures
- **503**: Missing environment variables

## Development

### Testing the API

Use curl to test the API endpoint:

```bash
curl -X POST http://localhost:3000/api/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "Show me the top 10 farms"}'
```

### Debugging

Enable detailed logging:

```typescript
// In route.ts
console.log("Gemini response:", text);
console.log("Parsed response:", parsedResponse);
```

## Limitations

### Current Version

1. **No schema awareness**: The model doesn't have access to the actual table schemas. It relies on:
   - Google Search for public information about Danish agricultural data
   - Context provided in the system instruction
   - Common patterns in agricultural datasets

2. **Table name guessing**: The model may suggest table names that don't exist. Users should review generated SQL before execution.

### Future Enhancements

1. **Schema injection**: Pass actual table schemas from the manifest to the AI
2. **Query validation**: Pre-validate against DuckDB before returning
3. **Query history**: Remember user's query patterns for better suggestions
4. **Multi-turn conversations**: Allow follow-up questions to refine queries
5. **Query optimization**: Suggest indexes or optimization hints

## Best Practices

### For Users

1. **Review before executing**: Always check the generated SQL in the editor
2. **Be specific**: Include details like limits, filters, or specific columns
3. **Use domain terms**: Mention CVR, CHR, BFE when referring to identifiers
4. **Iterate**: If the first query isn't right, refine your question

### For Developers

1. **Update system instruction**: Keep the prompt in sync with available tables
2. **Monitor costs**: Gemini API calls cost money; consider caching common queries
3. **Error feedback**: Log failed queries to improve the system instruction
4. **Schema documentation**: Maintain clear documentation of available tables

## References

- [Google Gemini API Documentation](https://ai.google.dev/docs)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [Next.js API Routes](https://nextjs.org/docs/app/building-your-application/routing/route-handlers)
