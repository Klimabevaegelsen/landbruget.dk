# MCP Server Setup Guide

## Overview

Model Context Protocol (MCP) servers give agents direct access to external systems and tools. This project uses several MCP servers to accelerate development and reduce manual work.

## Installed & Configured MCP Servers

### 1. Supabase MCP Server (PRIMARY)

**Purpose**: Direct database access for schema management, queries, and migrations

**Installation**:
```bash
npx -y @supabase/mcp-server-supabase@latest --features=database,docs
```

**Features Enabled**:
- `database` - Query schema, run SQL, manage tables
- `docs` - Access Supabase documentation

**Capabilities**:
- Create and modify database tables
- Run SQL queries
- Generate migrations
- View schema information
- Access Supabase docs in context

**Authentication**:
- Uses OAuth (browser login on first use)
- No manual token management required

**Usage Example**:
```
Agent: Show me the schema for the fields table
Agent: Create a migration to add crop_rotation column
Agent: Query all fields in municipality 0101
```

**Configuration Location**:
See Claude Desktop app settings or `~/.config/claude/mcp.json`

---

## Recommended MCP Servers (Not Yet Installed)

### 2. shadcn/ui MCP Server

**Purpose**: Browse, search, and install UI components

**Installation**:
```bash
npx shadcn@latest add --mcp
```

**Capabilities**:
- Browse available components
- Search component library
- Install components directly
- View component documentation
- Get usage examples

**Why We Need This**:
- Speeds up UI development
- Ensures component consistency
- Reduces copy-paste errors
- Provides instant documentation

**Usage Example**:
```
Agent: Install the data-table component
Agent: Show me how to use the command component
Agent: Add a calendar picker to the project
```

---

### 3. Vercel MCP Server

**Purpose**: Deployment management and monitoring

**Installation**:
Follow https://vercel.com/docs/mcp/vercel-mcp

**Capabilities**:
- Manage deployments
- View deployment logs
- Configure project settings
- Analyze performance metrics
- Set environment variables

**Why We Need This**:
- Agents can diagnose deployment issues
- Automated deployment verification
- Environment variable management
- Performance monitoring

**Usage Example**:
```
Agent: Check latest deployment status
Agent: Show me errors from production logs
Agent: Deploy preview for current branch
```

**Requirements**:
- Vercel API token (from https://vercel.com/account/tokens)
- Project ID (from project settings)

---

### 4. GitHub MCP Server

**Purpose**: Pull request and issue management

**Installation**:
Via MCP marketplace or manual setup

**Capabilities**:
- Create/update pull requests
- Manage issues
- Review code changes
- Create releases
- Manage repository settings

**Why We Need This**:
- Automated PR creation (used by `/create-pr` command)
- Issue tracking integration with Beads
- Code review workflows
- Release automation

**Usage Example**:
```
Agent: Create PR for current branch
Agent: Add reviewers to PR #42
Agent: Close issue #123 as completed
```

---

### 5. Web Search MCP Server

**Purpose**: Research Danish agricultural regulations and APIs

**Installation**:
Built-in to Claude (may already be available)

**Capabilities**:
- Search for current information
- Research APIs and documentation
- Find regulatory requirements
- Discover new data sources

**Why We Need This**:
- Danish agricultural regulations change frequently
- Research new government data APIs
- Find documentation for external services
- Stay current with data formats

**Usage Example**:
```
Agent: Research latest Danish pesticide reporting requirements
Agent: Find API documentation for Geodatastyrelsen WFS service
Agent: Check current data format for CHR registry
```

---

## Future: Custom Project MCP Server

**Purpose**: Secure gateway to Danish government APIs and internal business logic

**Status**: Not yet implemented (Phase 5 in roadmap)

**Planned Features**:

1. **Secure API Gateway**
   - BDNB (French building database)
   - Cadastre (Danish land registry)
   - Géorisques (Environmental risks)
   - Abstracts away API keys and complexity

2. **Internal Business Logic**
   - Expose application-specific functions
   - Data validation utilities
   - Custom aggregations
   - Report generation

3. **Authentication**
   - OAuth via Clerk (@clerk/mcp-tools)
   - Per-tool permissions
   - Audit logging

**Implementation Plan**:
```
custom-mcp-server/
├── src/
│   ├── tools/
│   │   ├── bdnb.ts          # BDNB API tools
│   │   ├── cadastre.ts       # Cadastre API tools
│   │   └── internal.ts       # Internal business logic
│   ├── auth/
│   │   └── clerk-oauth.ts    # OAuth setup
│   └── index.ts
├── package.json
└── README.md
```

**Example Tools**:
- `fetch_cadastre_parcel(bfe_number)` - Get parcel data
- `calculate_field_emissions(field_id)` - Emission calculations
- `validate_cvr(cvr_number)` - Company ID validation
- `geocode_danish_address(address)` - Address to coordinates

---

## MCP Server Configuration

### Claude Desktop Configuration

MCP servers are configured in:
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Linux**: `~/.config/claude/claude_desktop_config.json`

### Example Configuration

```json
{
  "mcpServers": {
    "supabase": {
      "command": "npx",
      "args": ["-y", "@supabase/mcp-server-supabase@latest", "--features=database,docs"]
    },
    "shadcn": {
      "command": "npx",
      "args": ["shadcn@latest", "mcp"]
    },
    "github": {
      "command": "npx",
      "args": ["-y", "@github/github-mcp"],
      "env": {
        "GITHUB_TOKEN": "ghp_your_token_here"
      }
    }
  }
}
```

### Conductor MCP Setup

For Conductor workspaces, MCP servers are shared across all Claude Code instances but need to be configured once in Claude Desktop settings.

---

## Security Best Practices

### Do's ✅
- Use OAuth when available (Supabase, GitHub)
- Store tokens in environment variables
- Use read-only mode for production databases
- Limit MCP server access to specific projects
- Review MCP server logs regularly

### Don'ts ❌
- Don't commit MCP configuration with tokens
- Don't use production credentials in development
- Don't share MCP tokens between team members
- Don't bypass authentication mechanisms
- Don't expose internal APIs without authentication

---

## Troubleshooting

### MCP Server Not Appearing

1. **Check configuration**: Verify `claude_desktop_config.json` syntax
2. **Restart Claude**: Quit and restart Claude Desktop app
3. **Check logs**: Look for errors in Claude logs
4. **Verify installation**: Run MCP server command manually

### Authentication Failures

1. **Re-authenticate**: Remove cached tokens and re-login
2. **Check token expiry**: Tokens may need refreshing
3. **Verify permissions**: Ensure token has required scopes
4. **Check network**: VPN or firewall may block OAuth

### Performance Issues

1. **Limit concurrent requests**: Too many MCP calls at once
2. **Cache responses**: Some queries can be cached
3. **Optimize queries**: Reduce data transferred
4. **Check rate limits**: API may be throttling

---

## Testing MCP Servers

### Test Supabase MCP

```
Prompt: "Show me the database schema"
Expected: List of tables with columns

Prompt: "Query the first 5 rows from companies table"
Expected: Query results with data
```

### Test shadcn MCP

```
Prompt: "Show me available shadcn components"
Expected: List of components

Prompt: "Install the button component"
Expected: Files created and package updated
```

### Test GitHub MCP

```
Prompt: "Show me open pull requests"
Expected: List of PRs

Prompt: "Create a new issue for X"
Expected: Issue created confirmation
```

---

## Resources

- [MCP Documentation](https://modelcontextprotocol.io/)
- [Supabase MCP](https://supabase.com/docs/guides/getting-started/mcp)
- [shadcn MCP](https://ui.shadcn.com/docs/mcp)
- [Vercel MCP](https://vercel.com/docs/mcp/vercel-mcp)
- [Clerk MCP Tools](https://clerk.com/docs/guides/development/mcp/build-mcp-server)

---

*This guide is part of the agent-native development setup for landbruget.dk.*
