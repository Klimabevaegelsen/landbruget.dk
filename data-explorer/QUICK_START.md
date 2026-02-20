# Quick Start - Natural Language Queries

Get started with the natural language to SQL feature in under 5 minutes.

## 1. Get a Google API Key

1. Go to [Google AI Studio](https://aistudio.google.com/app/apikey)
2. Sign in with your Google account
3. Click "Create API Key"
4. Copy the key (starts with `AIza...`)

## 2. Configure Environment

```bash
cd data-explorer

# Create .env.local file
cp .env.local.example .env.local

# Edit .env.local and add your API key
echo 'GOOGLE_API_KEY=AIza...' > .env.local
```

## 3. Start the Server

```bash
npm run dev
```

Open [http://localhost:3000/example](http://localhost:3000/example)

## 4. Try It Out

1. **Select a dataset** from the left sidebar
2. **Type a question** in the "Ask in Natural Language" box:
   ```
   Show me the top 10 farms by land area
   ```
3. **Press Enter** or click "Ask"
4. **Wait 1-3 seconds** for the AI to generate SQL
5. **Review the generated SQL** in the editor below
6. **Click "Run Query"** to execute it

## Example Questions

Try these questions to see the feature in action:

### Simple Queries
- "Show me the first 10 rows"
- "Count the total number of farms"
- "What are all the column names?"

### Filtering
- "Find farms in Region Midtjylland"
- "Show me organic farms only"
- "List farms with more than 100 hectares"

### Sorting & Aggregation
- "Top 10 farms by land area"
- "Count farms by region"
- "Average farm size per municipality"

### Geographic Queries
- "Find farms near Copenhagen"
- "Show farms within Jutland"
- "List all farms in postal code 8000"

### Data Analysis
- "What are the most common crop types?"
- "How many farms are organic?"
- "Distribution of farm sizes"

## Tips for Better Results

### Be Specific
❌ "Show me farms"
✅ "Show me the top 10 farms by land area"

### Use Identifiers
❌ "Find company 12345678"
✅ "Find farms with CVR number 12345678"

### Mention Columns
❌ "Sort by size"
✅ "Sort by land_area column"

### Set Limits
❌ "Show all farms"
✅ "Show me 100 farms"

## Troubleshooting

### "API key not configured" error
- Check `.env.local` exists in `data-explorer/` directory
- Verify `GOOGLE_API_KEY` is set correctly
- Restart the dev server after adding the key

### "Rate limit exceeded" error
- Free tier: 15 requests per minute, 1,500 per day
- Wait 60 seconds and try again
- Consider upgrading your API plan

### Generated SQL doesn't work
- **Review the SQL** before running
- The AI may suggest non-existent table/column names
- Edit the SQL manually to fix issues
- Report persistent problems for improvement

### Slow responses
- Normal: 1-3 seconds per query
- Network dependent
- Check your internet connection
- Google AI API status at [status.ai.google.dev](https://status.ai.google.dev)

## Next Steps

- Read [NATURAL_LANGUAGE_QUERIES.md](./docs/NATURAL_LANGUAGE_QUERIES.md) for detailed documentation
- See [TESTING_CHECKLIST.md](./TESTING_CHECKLIST.md) for comprehensive testing
- Review [IMPLEMENTATION_SUMMARY.md](./IMPLEMENTATION_SUMMARY.md) for technical details

## Cost Information

**Free Tier (as of 2026-01-10)**:
- 15 requests per minute
- 1,500 requests per day
- No credit card required

This is sufficient for development and testing. For production use, monitor your usage in [Google AI Studio](https://aistudio.google.com).

## Support

For issues or questions:
1. Check the [documentation](./docs/NATURAL_LANGUAGE_QUERIES.md)
2. Review [troubleshooting section](#troubleshooting)
3. Check browser console for errors
4. Verify API key is valid and has quota remaining
