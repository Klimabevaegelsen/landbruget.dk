# /db-migrate - Database Migration Workflow

Create and apply Supabase migrations safely.

## Usage

```
/db-migrate <migration-name>
```

Example:
```
/db-migrate add_farm_statistics
/db-migrate create_crop_types_table
/db-migrate add_index_cvr
```

## Process

### 1. Create Migration File

```bash
cd $CLAUDE_PROJECT_DIR
supabase migration new $ARGUMENTS
```

Creates: `supabase/migrations/[timestamp]_$ARGUMENTS.sql`

### 2. Write Migration SQL

Open the created file and write the migration:

```sql
-- Migration: $ARGUMENTS
-- Created: [date]

-- 1. Create table
CREATE TABLE IF NOT EXISTS [table_name] (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  -- columns
);

-- 2. Enable RLS
ALTER TABLE [table_name] ENABLE ROW LEVEL SECURITY;

-- 3. Create policies
CREATE POLICY "Allow public read"
  ON [table_name] FOR SELECT
  USING (true);

-- 4. Create indexes
CREATE INDEX idx_[table]_[column] ON [table_name] ([column]);
```

### 3. Validate SQL Syntax

```bash
supabase db lint
```

### 4. Apply to Local Database

```bash
supabase db push
```

### 5. Verify Migration

```bash
# Check migration status
supabase migration list

# Test query
supabase db query "SELECT * FROM [table_name] LIMIT 1"

# Check RLS is enabled
supabase db query "SELECT tablename, rowsecurity FROM pg_tables WHERE schemaname = 'public'"
```

### 6. Generate TypeScript Types

```bash
supabase gen types typescript --local > frontend/src/types/supabase.ts
```

### 7. Commit Migration

```bash
git add supabase/migrations/
git add frontend/src/types/supabase.ts
git commit -m "feat(db): $ARGUMENTS

- Added [table/column/index]
- Enabled RLS with [policy description]
- Generated TypeScript types

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Migration Templates

### New Table

```sql
CREATE TABLE IF NOT EXISTS [table_name] (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

  -- Business columns
  cvr_number VARCHAR(8) NOT NULL,
  name TEXT NOT NULL,

  -- Foreign keys
  related_id UUID REFERENCES other_table(id),

  -- Constraints
  CONSTRAINT valid_cvr CHECK (cvr_number ~ '^\d{8}$')
);

-- Enable RLS
ALTER TABLE [table_name] ENABLE ROW LEVEL SECURITY;

-- Public read policy (most common for Landbruget.dk)
CREATE POLICY "Allow public read"
  ON [table_name] FOR SELECT
  USING (true);

-- Indexes
CREATE INDEX idx_[table]_cvr ON [table_name] (cvr_number);

-- Updated_at trigger
CREATE TRIGGER [table]_updated_at
  BEFORE UPDATE ON [table_name]
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at();
```

### Add Column

```sql
ALTER TABLE [table_name]
ADD COLUMN [column_name] [type] [constraints];

-- If adding NOT NULL, provide default
ALTER TABLE [table_name]
ADD COLUMN [column_name] TEXT NOT NULL DEFAULT '';

-- Add index if frequently queried
CREATE INDEX idx_[table]_[column] ON [table_name] ([column_name]);
```

### Add Geometry Column

```sql
-- Add PostGIS geometry
ALTER TABLE [table_name]
ADD COLUMN geom GEOMETRY(Point, 4326);

-- Spatial index
CREATE INDEX idx_[table]_geom ON [table_name] USING GIST (geom);
```

### Add Index

```sql
-- B-tree (default)
CREATE INDEX idx_[table]_[column] ON [table_name] ([column]);

-- Spatial (GiST)
CREATE INDEX idx_[table]_geom ON [table_name] USING GIST (geom);

-- Composite
CREATE INDEX idx_[table]_[col1]_[col2] ON [table_name] ([col1], [col2]);

-- Partial
CREATE INDEX idx_[table]_active ON [table_name] (column) WHERE is_active = true;
```

## Output Format

```
## Migration: $ARGUMENTS

### Created
- File: `supabase/migrations/[timestamp]_$ARGUMENTS.sql`

### Changes
- Created table: [table_name]
- Added RLS policy: [policy_name]
- Created index: [index_name]

### Validation
- ✅ SQL syntax valid
- ✅ Migration applied to local
- ✅ RLS enabled
- ✅ TypeScript types generated

### Next Steps
1. Test queries against new schema
2. Update frontend components to use new types
3. Push to remote: `supabase db push --linked`
```

## Troubleshooting

### Migration Failed

```bash
# Check error details
supabase db push 2>&1

# Reset local database (caution!)
supabase db reset

# Apply migrations again
supabase db push
```

### RLS Blocking Queries

```sql
-- Check policies
SELECT * FROM pg_policies WHERE tablename = '[table]';

-- Temporarily bypass for testing
SET LOCAL ROLE postgres;
```

### Type Generation Failed

```bash
# Ensure schema is pulled
supabase db pull

# Regenerate types
supabase gen types typescript --local > frontend/src/types/supabase.ts
```

## Safety Checklist

Before applying to production:
- [ ] Migration tested locally
- [ ] RLS policies are correct
- [ ] Indexes added for query patterns
- [ ] TypeScript types updated
- [ ] No destructive operations (DROP) without backup
- [ ] Rollback plan documented
