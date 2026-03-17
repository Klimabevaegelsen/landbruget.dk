# Natural Language to SQL - Feature Flow

## Visual Component Layout

```
┌─────────────────────────────────────────────────────────────┐
│ Data Explorer                                                │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐  ┌────────────────────────────────────┐  │
│  │              │  │ ╔════════════════════════════════╗ │  │
│  │  Dataset     │  │ ║ Ask in Natural Language       ║ │  │
│  │  Browser     │  │ ╚════════════════════════════════╝ │  │
│  │              │  │                                     │  │
│  │ • farms      │  │ [Text input box for questions]     │  │
│  │ • crops      │  │                                     │  │
│  │ • livestock  │  │ Press Enter to ask   [Ask Button]  │  │
│  │ • ...        │  │                                     │  │
│  │              │  │ ┌────────────────────────────────┐ │  │
│  │              │  │ │ ✓ SQL Generated                │ │  │
│  │              │  │ │ Explanation: ...                │ │  │
│  │              │  │ │ SELECT * FROM farms LIMIT 10   │ │  │
│  │              │  │ └────────────────────────────────┘ │  │
│  │              │  │                                     │  │
│  │              │  │ ╔════════════════════════════════╗ │  │
│  │              │  │ ║ SQL Query                      ║ │  │
│  │              │  │ ╚════════════════════════════════╝ │  │
│  │              │  │                                     │  │
│  │              │  │ [SQL Editor - generated query]     │  │
│  │              │  │ SELECT * FROM farms                │  │
│  │              │  │ ORDER BY land_area DESC            │  │
│  │              │  │ LIMIT 10;                          │  │
│  │              │  │                                     │  │
│  │              │  │            [Run Query Button]      │  │
│  │              │  │                                     │  │
│  │              │  │ ╔════════════════════════════════╗ │  │
│  │              │  │ ║ Results                        ║ │  │
│  │              │  │ ╚════════════════════════════════╝ │  │
│  │              │  │                                     │  │
│  │              │  │ [Results Table with data]          │  │
│  └──────────────┘  └────────────────────────────────────┘  │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow Diagram

```
┌─────────────┐
│    User     │
└──────┬──────┘
       │
       │ 1. Types question
       ├─────────────────────────────────────┐
       │                                     │
       │ 2. Clicks "Ask"                     │
       ↓                                     │
┌─────────────┐                              │
│  AskInput   │                              │
│  Component  │                              │
└──────┬──────┘                              │
       │                                     │
       │ 3. POST /api/ask                    │
       ↓                                     │
┌─────────────────────┐                      │
│   API Route         │                      │
│   /api/ask          │                      │
└──────┬──────────────┘                      │
       │                                     │
       │ 4. Call Gemini API                  │
       ↓                                     │
┌─────────────────────┐                      │
│  Google Gemini      │                      │
│  2.0 Flash          │                      │
│  + Search Retrieval │                      │
└──────┬──────────────┘                      │
       │                                     │
       │ 5. Return {sql, explanation}        │
       ↓                                     │
┌─────────────────────┐                      │
│   API Route         │                      │
│   Validates SQL     │                      │
└──────┬──────────────┘                      │
       │                                     │
       │ 6. Return JSON                      │
       ↓                                     │
┌─────────────┐                              │
│  AskInput   │                              │
│  Component  │                              │
└──────┬──────┘                              │
       │                                     │
       │ 7. Display success message          │
       │ 8. Call onSqlGenerated()            │
       ↓                                     │
┌─────────────┐                              │
│  SQLEditor  │                              │
│  Component  │                              │
└──────┬──────┘                              │
       │                                     │
       │ 9. User reviews/edits SQL           │
       │ 10. Clicks "Run Query"              │
       ↓                                     │
┌─────────────────────┐                      │
│   DuckDB WASM       │←─────────────────────┘
│   Query Engine      │ 11. Shows results
└─────────────────────┘
```

## State Management Flow

```typescript
// Page-level state
const [currentQuery, setCurrentQuery] = useState("");
const [queryResults, setQueryResults] = useState([]);
const [selectedDataset, setSelectedDataset] = useState(null);

// Flow:
// 1. User selects dataset
//    → setSelectedDataset(dataset)
//    → setCurrentQuery(`SELECT * FROM ${dataset.name} LIMIT 100`)

// 2. User asks question
//    → AskInput calls /api/ask
//    → API returns SQL
//    → onSqlGenerated(sql)
//    → setCurrentQuery(sql)
//    → SQLEditor re-renders with new query

// 3. User executes query
//    → SQLEditor calls onExecute(query)
//    → DuckDB executes query
//    → setQueryResults(results)
//    → ResultsTable displays data
```

## Component Communication

```
┌────────────────────────────────────────────────────────┐
│ ExamplePage (Parent)                                   │
│                                                        │
│  const [currentQuery, setCurrentQuery] = useState('') │
│                                                        │
│  ┌──────────────────────────────────────────────────┐ │
│  │ AskInput                                         │ │
│  │   onSqlGenerated={setCurrentQuery} ────────┐    │ │
│  └───────────────────────────────────────────│──────┘ │
│                                              │        │
│  ┌───────────────────────────────────────────│──────┐ │
│  │ SQLEditor                                │      │ │
│  │   initialQuery={currentQuery} ←──────────┘      │ │
│  │   onExecute={handleExecuteQuery} ────────┐      │ │
│  └───────────────────────────────────────────│──────┘ │
│                                              │        │
│  ┌───────────────────────────────────────────│──────┐ │
│  │ ResultsTable                             │      │ │
│  │   data={queryResults} ←──────────────────┘      │ │
│  └──────────────────────────────────────────────────┘ │
│                                                        │
└────────────────────────────────────────────────────────┘
```

## API Request/Response Cycle

```
Request:
POST /api/ask
Content-Type: application/json

{
  "question": "Show me the top 10 farms by land area"
}

       ↓

Processing:
1. Validate environment (GOOGLE_API_KEY)
2. Parse request body
3. Initialize Gemini model
4. Send prompt with question
5. Parse AI response (JSON)
6. Validate SQL (must be SELECT)
7. Return response

       ↓

Success Response (200):
{
  "sql": "SELECT * FROM farms ORDER BY land_area DESC LIMIT 10;",
  "explanation": "This query selects all farms and sorts them by land area..."
}

       ↓

Error Response (400/401/429/500/503):
{
  "error": "Error message",
  "details": "Additional context"
}
```

## User Interaction Timeline

```
t=0s    User types question
t=0.1s  User presses Enter
t=0.1s  "Ask" button disabled
t=0.1s  Loading spinner appears
t=0.1s  "Thinking..." text shown

[Network delay]

t=0.5s  Request reaches server
t=0.5s  Server validates request
t=0.6s  Gemini API called

[AI processing]

t=2.0s  Gemini returns SQL
t=2.0s  Server validates SQL
t=2.1s  Response sent to client

[Network delay]

t=2.5s  Client receives response
t=2.5s  Loading spinner removed
t=2.5s  Success message displayed
t=2.5s  SQL loaded into editor
t=2.5s  "Ask" button re-enabled

[User reviews SQL]

t=5.0s  User clicks "Run Query"
t=5.0s  DuckDB executes query
t=5.1s  Results displayed in table
```

## Error Handling Flow

```
┌─────────────────┐
│ User Action     │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ Validation      │
│ • Question?     │
│ • Not empty?    │
└────┬────────┬───┘
     │        │
   YES       NO → Show error immediately
     │
     ↓
┌─────────────────┐
│ API Call        │
└────┬────────┬───┘
     │        │
   OK        ERROR
     │        │
     │        ├─→ 400: Invalid request
     │        ├─→ 401: Auth failed
     │        ├─→ 429: Rate limit
     │        ├─→ 500: AI error
     │        └─→ 503: Config error
     │
     ↓
┌─────────────────┐
│ SQL Validation  │
│ • Starts w/     │
│   SELECT?       │
└────┬────────┬───┘
     │        │
   YES       NO → Reject query
     │
     ↓
┌─────────────────┐
│ Display Success │
└─────────────────┘
```

## Security Layers

```
Layer 1: Client-side
├─ Input validation
├─ UI state management
└─ No sensitive data stored

Layer 2: API Route (Server-side)
├─ Environment validation
├─ Request parsing
├─ SQL validation (SELECT only)
└─ Error sanitization

Layer 3: Gemini API
├─ API key authentication
├─ Rate limiting
└─ Content safety filters

Layer 4: Response Validation
├─ JSON structure check
├─ SQL syntax check
└─ Security patterns check
```

## Performance Optimization

```
Client-side:
• Debounce input (future)
• Cache common queries (future)
• Optimistic UI updates

API Route:
• Fast JSON parsing
• Minimal processing
• Quick validation

Gemini API:
• Flash model (faster)
• Dynamic retrieval
• Streaming responses (future)

Total Time Budget:
• Client → Server: < 100ms
• Server → Gemini: < 500ms
• Gemini processing: 1-2s
• Gemini → Server: < 100ms
• Server → Client: < 100ms
─────────────────────────────
Total: ~2-3s typical
```
