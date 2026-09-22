# Sentinel Databricks MCP server

A small **MCP (Model Context Protocol)** server that lets a Claude client explore,
query, and (optionally) manage the Databricks workspace behind this project —
starting with the Unity Catalog tables the ingest notebook writes,
`workspace.sentinel.agg_trades` and `workspace.sentinel.klines_1m` — directly from
a chat, instead of clicking around the Databricks UI.

MCP is the open protocol Claude uses to talk to external tools. You run a
**server** that exposes **tools** (functions Claude can call); the Claude client
is the **host** that decides when to call them. The read tools wrap the Databricks
SDK's Statement Execution API against a SQL warehouse; the write tools call the
Databricks REST API to create, change, and delete workspace objects.

## What it can do

Read tools (always on):

| Tool | What it does |
| --- | --- |
| `list_schemas` | List schemas in a catalog (default `workspace`) |
| `list_tables` | List tables in a schema (default `workspace.sentinel`) |
| `describe_table` | Columns, types, comments and partitioning for a table |
| `preview_table` | First N rows of a table (capped) |
| `run_sql` | Run a read-only SQL query on the warehouse |
| `sentinel_data_summary` | Per-`(symbol, month)` row counts and time bounds across both tables |
| `list_jobs` | List Databricks Jobs (find your ingest job) |
| `recent_job_runs` | Recent run status for a job — "did my ingest succeed?" |

Write tools (off unless `DATABRICKS_MCP_ALLOW_WRITES=1`):

| Tool | What it does |
| --- | --- |
| `import_notebook` | Create/overwrite a notebook or Python file in the workspace |
| `delete_workspace_object` | Delete a notebook, file, or folder |
| `trigger_job` | Start a job now |
| `databricks_request` | Call any Databricks REST endpoint — the general lever for creating, changing, and deleting jobs, pipelines, and dashboards |

The last one is deliberately general: rather than a wrapper per resource, Claude
composes the documented REST calls (create a job, delete a pipeline, publish a
dashboard). `GET` is always allowed; anything that mutates needs writes enabled.

## Prerequisites

1. A **SQL warehouse** in your workspace (any size; serverless is easiest). Note
   its **warehouse id** — it's in the warehouse's URL.
2. A **personal access token**: user avatar -> Settings -> Developer -> Access
   tokens -> Generate. See the security note below on scoping it.
3. Python 3.10+ locally.

## Install

```bash
python -m venv .venv
.venv/bin/pip install -r databricks_mcp/requirements.txt   # Windows: .venv\Scripts\pip
```

## Configure

Set three environment variables (the rest have defaults):

| Variable | Example |
| --- | --- |
| `DATABRICKS_HOST` | `https://dbc-1234abcd-5678.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | `dapi...` |
| `DATABRICKS_WAREHOUSE_ID` | `abc123def456` |

Copy `databricks_mcp/.env.example` to `databricks_mcp/.env` for local testing, or
put them in the `env` block of your Claude client's config (below). `.env` is
already git-ignored — never commit a real token.

### Smoke-test it by hand (optional)

```bash
DATABRICKS_HOST=... DATABRICKS_TOKEN=... DATABRICKS_WAREHOUSE_ID=... \
  .venv/bin/python -m databricks_mcp.server
```

It will sit quietly waiting to speak MCP over stdin/stdout — that's correct.
Ctrl-C to exit. The real test is registering it with a client and asking Claude
to call a tool.

## Register with a Claude client

There are two ways to run this server, and which one you need depends only on
where you use Claude:

- **Local (stdio)** — the client launches the server as a subprocess on your
  machine. Works with **Claude Desktop** and **Claude Code**. Simplest; start here.
- **Remote (HTTP)** — the server runs as a long-lived HTTP service you host, and
  the client connects to its URL. This is the only option that also reaches
  **claude.ai in the browser** (a browser can't launch a local process), and it
  works for Desktop/Code too. See "Hosting it remotely" below.

The tool surface is identical either way — only the plumbing differs.

### Local — Claude Code (CLI) and Claude Desktop

Use the **absolute path** to the venv's Python — the client won't have your venv
activated. Replace `/home/user/Sentinel` with your repo path.

Either run:

```bash
claude mcp add sentinel-databricks \
  --env DATABRICKS_HOST=https://your-workspace.cloud.databricks.com \
  --env DATABRICKS_TOKEN=dapiXXXX \
  --env DATABRICKS_WAREHOUSE_ID=abc123 \
  -- /home/user/Sentinel/.venv/bin/python -m databricks_mcp.server
```

…or create `.mcp.json` in the repo root (checked in, but keep secrets out — see
below):

```json
{
  "mcpServers": {
    "sentinel-databricks": {
      "command": "/home/user/Sentinel/.venv/bin/python",
      "args": ["-m", "databricks_mcp.server"],
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapiXXXX",
        "DATABRICKS_WAREHOUSE_ID": "abc123"
      }
    }
  }
}
```

### Claude Desktop

Add the same server block to `claude_desktop_config.json`
(Settings -> Developer -> Edit Config), then restart the app:

```json
{
  "mcpServers": {
    "sentinel-databricks": {
      "command": "/home/user/Sentinel/.venv/bin/python",
      "args": ["-m", "databricks_mcp.server"],
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapiXXXX",
        "DATABRICKS_WAREHOUSE_ID": "abc123"
      }
    }
  }
}
```

Then just ask, e.g. *"Using the sentinel-databricks tools, summarise what data is
loaded"* or *"preview 10 rows of klines_1m for BTCUSDT"*.

### Remote — claude.ai in the browser (Connectors)

A browser can't spawn a local process, so for **claude.ai** the server has to run
somewhere reachable over HTTPS and be added as a **Connector** (Settings ->
Connectors -> Add custom connector -> the server's URL). The same code serves this
mode — run it with `MCP_TRANSPORT=http` instead of stdio:

```bash
MCP_TRANSPORT=http DATABRICKS_HOST=... DATABRICKS_TOKEN=... DATABRICKS_WAREHOUSE_ID=... \
  .venv/bin/python -m databricks_mcp.server        # serves on http://127.0.0.1:8000
```

For real use it needs a host. The natural home is a **Databricks App**: it runs
inside your workspace, authenticates callers with Databricks OAuth, and executes
under its own service principal (whose Unity Catalog grants become the safety
boundary), so no personal token travels anywhere. That deployment is workspace-
specific — package this as an app with an `app.yaml` whose command is
`python -m databricks_mcp.server` with `MCP_TRANSPORT=http`, `databricks apps deploy`,
then add the app's URL as a connector. Any HTTPS host works, but if you self-host
elsewhere you must add authentication yourself — an open endpoint that can delete
pipelines is exactly as bad as it sounds.

### Enabling writes

All three surfaces run the same tools, but the write tools stay dormant until you
opt in. Set `DATABRICKS_MCP_ALLOW_WRITES=1` in the server's environment (the `env`
block, the `.env`, or the app config) **and** use a token/service principal that
Unity Catalog actually permits to make the change. Both conditions must hold.

## Security notes (read this once)

- **The token is the boundary, not the code.** The read-only check in `run_sql`
  blocks a stray `DROP` typed into a chat, but it's a guardrail, not a wall.
  Create the token under a principal with **read-only Unity Catalog grants** on
  the `sentinel` schema, and writes are impossible no matter what. This is
  *defence in depth*: an app-level guard **and** a permissions boundary.
- **Writes are off by default.** `run_sql` refuses non-`SELECT` statements, and
  every mutating tool (`import_notebook`, `delete_workspace_object`, `trigger_job`,
  and any non-`GET` `databricks_request`) refuses until you set
  `DATABRICKS_MCP_ALLOW_WRITES=1`. Even then, a mutation only succeeds if the
  token's Unity Catalog / workspace permissions allow it.
- **Keep secrets out of git.** `databricks_mcp/.env` is git-ignored. If you use
  `.mcp.json`, either keep tokens out of it (rely on `.env` / a `~/.databrickscfg`
  profile) or don't commit it.

## Alternatives (so you know the landscape)

This custom server is the most tailored and the most educational option, but it
isn't the only one:

- **Databricks' own managed MCP servers.** Databricks hosts MCP endpoints for
  **Genie** (natural-language questions over a set of tables), **Vector Search**,
  and **Unity Catalog functions**, at URLs under your workspace with OAuth and UC
  governance — no server code to run. A Genie space over `sentinel.*` is the
  cleanest "ask questions in English" path and works with remote/browser clients.
  See the Databricks docs on managed MCP servers.
- **Community servers** such as `databrickslabs/mcp` give a generic
  catalog-explore-and-query tool surface with zero code — faster to adopt, but
  you inherit their design and trust surface.

The trade-off: managed/Genie is least effort and best governed but fixed in
shape; a custom server (this one) is exactly the tools *you* want and a good way
to learn how MCP and the Databricks SDK actually fit together.
