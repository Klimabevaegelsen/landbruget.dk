# MCP Server Setup Guide

This document contains instructions for setting up all MCP servers configured for the Landbruget.dk project.

## Quick Start

1. Copy the configuration from `claude_desktop_config.json` to your Claude Desktop config location:
   - **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
   - **Linux**: `~/.config/Claude/claude_desktop_config.json`

2. Complete the setup steps below for each server

3. Restart Claude Desktop

## Server Setup Instructions

### 1. Supabase MCP Server ✅

**Status**: No additional setup required

**Features**:
- Query database schema and data
- Create/modify tables
- Run SQL queries
- Manage migrations
- Access Supabase documentation

**Auto-updates**: Yes (via `npx -y`)

---

### 2. GitHub MCP Server

**Prerequisites**:
- Docker Desktop installed and running
- GitHub Personal Access Token

**Setup Steps**:

1. **Create GitHub Personal Access Token**:
   - Go to https://github.com/settings/tokens
   - Click "Generate new token (classic)"
   - Select scopes:
     - ✅ `repo` (Full control of private repositories)
     - ✅ `read:packages` (Download packages from GitHub Package Registry)
     - ✅ `read:org` (Read org and team membership, read org projects)
   - Generate token and copy it

2. **Update Configuration**:
   - Open `claude_desktop_config.json`
   - Replace `YOUR_GITHUB_PAT_HERE` with your actual token
   - Save the file

3. **Verify Docker is Running**:
   ```bash
   docker ps
   ```

**Features**:
- Browse and query code repositories
- Create, update, and manage issues
- Create, update, and manage pull requests
- Search files and commits
- Analyze project structure

**Troubleshooting**:
- Ensure Docker Desktop is running
- Verify your PAT has correct permissions
- Check Claude Desktop logs if connection fails

---

### 3. GCP MCP Server

**Prerequisites**:
- Node.js installed
- Google Cloud SDK (gcloud) installed
- GCP project access

**Setup Steps**:

1. **Install Google Cloud SDK** (if not already installed):
   ```bash
   # macOS
   brew install google-cloud-sdk

   # Or download from: https://cloud.google.com/sdk/docs/install
   ```

2. **Authenticate with GCP**:
   ```bash
   gcloud auth application-default login
   ```
   - This will open a browser window for authentication
   - Sign in with your Google account
   - Grant the requested permissions

3. **Set Default Project** (optional):
   ```bash
   gcloud config set project YOUR_PROJECT_ID
   ```

4. **Restart Claude Desktop** to activate the MCP server

**Features**:
- Query Compute Engine instances and resources
- Manage Cloud Storage buckets and objects
- Query BigQuery datasets and tables
- Manage Cloud Run services
- Interact with Cloud Functions
- Multi-project support
- Natural language resource queries

**Troubleshooting**:
- Run `gcloud auth application-default login` again if auth expires
- Verify you have access to the GCP projects you're querying
- Check that Node.js is in your PATH

---

### 4. Cloudflare MCP Servers

**Prerequisites**:
- Node.js installed
- Cloudflare account (for OAuth authentication)

**Setup Steps**:

1. **No Initial Setup Required**:
   - Cloudflare MCP servers use OAuth authentication
   - Authentication happens automatically on first use

2. **First Time Authentication**:
   - When you first use a Cloudflare MCP tool, Claude will prompt for authentication
   - A browser window will open for Cloudflare OAuth
   - Sign in with your Cloudflare account
   - Grant permissions to the MCP server
   - Token is stored securely for future use

**Available Cloudflare Servers**:

- **observability** - Monitoring, logs, and metrics
- **workers** - Workers bindings and configuration
- **radar** - Internet insights and traffic analytics
- **ai-gateway** - AI Gateway request management
- **dns-analytics** - DNS traffic and query analytics

**Additional Servers** (add to config if needed):
- `https://documentation.mcp.cloudflare.com/mcp` - Cloudflare docs
- `https://browser-rendering.mcp.cloudflare.com/mcp` - Browser automation
- `https://logpush.mcp.cloudflare.com/mcp` - Log export management
- `https://audit-logs.mcp.cloudflare.com/mcp` - Security audit logs
- `https://graphql.mcp.cloudflare.com/mcp` - GraphQL API access

**Features**:
- Access Cloudflare services via natural language
- Query analytics and metrics
- Manage Workers and configurations
- Monitor performance and security
- Access documentation contextually

**Troubleshooting**:
- Clear OAuth tokens if authentication fails
- Ensure you have proper Cloudflare account permissions
- Verify internet connectivity for OAuth flow

---

## Verifying Your Setup

After completing all setup steps and restarting Claude Desktop:

1. Open Claude Desktop
2. Start a new conversation
3. Check that MCP servers are connected (look for MCP indicator in the UI)
4. Test each server with a simple query:
   - **Supabase**: "Show me the database tables"
   - **GitHub**: "List my recent repositories"
   - **GCP**: "List my GCP projects"
   - **Cloudflare**: "Show Cloudflare Workers"

## Security Best Practices

### GitHub PAT
- ✅ Use minimum required scopes
- ✅ Set expiration dates on tokens
- ✅ Rotate tokens periodically
- ✅ Never commit tokens to version control
- ❌ Don't share tokens

### GCP Credentials
- ✅ Use application default credentials
- ✅ Follow principle of least privilege
- ✅ Review IAM permissions regularly
- ❌ Don't commit credential files

### Cloudflare OAuth
- ✅ Review OAuth permissions before granting
- ✅ Revoke unused OAuth tokens
- ✅ Use Cloudflare Access for additional security

## Updating MCP Servers

### Servers with Auto-Update (via npx -y):
- Supabase
- GCP
- Cloudflare servers (via mcp-remote)

**These automatically fetch the latest version on each use.**

### Servers Requiring Manual Update:

#### GitHub MCP Server:
```bash
docker pull ghcr.io/github/github-mcp-server:latest
```

## Troubleshooting Common Issues

### "MCP server failed to start"
- Check that all prerequisites are installed
- Verify authentication credentials
- Check Claude Desktop logs
- Restart Claude Desktop

### "Docker not found" (GitHub MCP)
- Install Docker Desktop
- Ensure Docker is running
- Add Docker to your PATH

### "Authentication failed" (GCP)
- Run `gcloud auth application-default login` again
- Check that you have project access
- Verify gcloud SDK is installed

### "OAuth flow failed" (Cloudflare)
- Clear browser cache and cookies
- Try authentication in incognito/private mode
- Check Cloudflare account permissions

## Additional Resources

- [Supabase MCP Documentation](https://github.com/supabase/mcp-server-supabase)
- [GitHub MCP Documentation](https://github.com/github/github-mcp-server)
- [GCP MCP Documentation](https://github.com/eniayomi/gcp-mcp)
- [Cloudflare MCP Documentation](https://developers.cloudflare.com/agents/model-context-protocol/)
- [Model Context Protocol Specification](https://modelcontextprotocol.io/)

## Support

For issues specific to:
- **This project**: Create issue in landbruget.dk repository
- **MCP servers**: Check respective GitHub repositories
- **Claude Desktop**: Contact Anthropic support
