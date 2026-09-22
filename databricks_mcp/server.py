"""Databricks MCP server for Sentinel.

Exposes a small, read-first set of tools that let a Claude client explore and
query the Unity Catalog tables this project writes (``workspace.sentinel.*``)
without leaving the chat. Under the hood it wraps the Databricks SDK's Statement
Execution API against a SQL warehouse, so every query runs with the permissions
of the token this process is configured with.

The read-only guard in :func:`run_sql` is defence in depth, not the security
boundary. The real boundary is Unity Catalog: give the token least privilege
(read-only on the schemas you want reachable) and an accidental ``DROP`` in a
chat is impossible regardless of what this code allows.

Transport is stdio: the Claude client launches this process and talks to it over
stdin/stdout. See ``README.md`` for the client configuration.
"""

from __future__ import annotations

import base64
import os
import re
import time
from typing import Any, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
from mcp.server.mcpserver import MCPServer

# Optional: load a local .env when running this server by hand. Inside a Claude
# client the environment variables are normally injected by the MCP config, so
# this is purely a convenience for local testing.
try:
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


# --- Configuration (all via environment) ------------------------------------

DEFAULT_CATALOG = os.getenv("DATABRICKS_MCP_CATALOG", "workspace")
DEFAULT_SCHEMA = os.getenv("DATABRICKS_MCP_SCHEMA", "sentinel")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
MAX_ROWS = int(os.getenv("DATABRICKS_MCP_MAX_ROWS", "1000"))
ALLOW_WRITES = os.getenv("DATABRICKS_MCP_ALLOW_WRITES", "").lower() in {"1", "true", "yes"}

# A statement whose first keyword is not one of these is refused unless writes
# are explicitly enabled. A guardrail against a stray DELETE/DROP typed into a
# chat — not a substitute for a least-privilege token.
READ_ONLY_KEYWORDS = ("select", "with", "show", "describe", "desc", "explain")

# Poll budget after the 50s synchronous wait, for a warehouse that is still
# spinning up when the query is submitted.
POLL_BUDGET_SECONDS = 120
POLL_INTERVAL_SECONDS = 1.5


server = MCPServer("sentinel-databricks")

_client: Optional[WorkspaceClient] = None


# --- Helpers ----------------------------------------------------------------

def client() -> WorkspaceClient:
    """Build the workspace client lazily so importing this module never opens a
    network connection.

    Auth resolves through the SDK's default chain: explicit ``DATABRICKS_HOST`` /
    ``DATABRICKS_TOKEN`` if set, otherwise a ``~/.databrickscfg`` profile or any
    of the other unified-auth sources the SDK understands.
    """
    global _client
    if _client is None:
        _client = WorkspaceClient()
    return _client


def _require_warehouse() -> str:
    if not WAREHOUSE_ID:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID is not set. Point it at a SQL warehouse: in the "
            "Databricks UI go to SQL Warehouses, open your warehouse, and copy the id "
            "from its URL (or from Connection details)."
        )
    return WAREHOUSE_ID


def _qualify(table: str) -> str:
    """Expand a bare or half-qualified table name to ``catalog.schema.table``.

    ``agg_trades`` -> ``workspace.sentinel.agg_trades``;
    ``sentinel.agg_trades`` -> ``workspace.sentinel.agg_trades``;
    an already fully-qualified name is returned unchanged.
    """
    parts = table.strip().split(".")
    if len(parts) == 1:
        return f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{parts[0]}"
    if len(parts) == 2:
        return f"{DEFAULT_CATALOG}.{parts[0]}.{parts[1]}"
    return table.strip()


def _first_keyword(statement: str) -> str:
    """Return the first SQL keyword, ignoring leading line/block comments."""
    without_line_comments = re.sub(r"--[^\n]*", " ", statement)
    without_block_comments = re.sub(r"/\*.*?\*/", " ", without_line_comments, flags=re.S)
    match = re.search(r"[a-zA-Z]+", without_block_comments)
    return match.group(0).lower() if match else ""


def _run(statement: str) -> dict[str, Any]:
    """Execute one SQL statement and return a compact result dict.

    Returns ``{columns, rows, row_count, truncated}`` where ``rows`` is a list of
    ``{column: value}`` dicts. Values come back as strings — that is how the
    Statement Execution API serialises a JSON_ARRAY result — which is fine for a
    model to read.
    """
    warehouse_id = _require_warehouse()

    response = client().statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        catalog=DEFAULT_CATALOG,
        schema=DEFAULT_SCHEMA,
        format=sql.Format.JSON_ARRAY,
        disposition=sql.Disposition.INLINE,
        wait_timeout="50s",
        on_wait_timeout=sql.ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )

    # A cold warehouse can still be PENDING/RUNNING after the 50s synchronous
    # wait, so poll until it settles or the budget runs out.
    statement_id = response.statement_id
    deadline = time.monotonic() + POLL_BUDGET_SECONDS
    while response.status and response.status.state in (
        sql.StatementState.PENDING,
        sql.StatementState.RUNNING,
    ):
        if time.monotonic() > deadline:
            client().statement_execution.cancel_execution(statement_id)
            raise TimeoutError(
                "Query did not finish within the time budget and was cancelled. "
                "The warehouse may have been starting from cold — try again."
            )
        time.sleep(POLL_INTERVAL_SECONDS)
        response = client().statement_execution.get_statement(statement_id)

    state = response.status.state if response.status else None
    if state != sql.StatementState.SUCCEEDED:
        error = response.status.error if response.status else None
        message = error.message if error else f"statement ended in state {state}"
        raise RuntimeError(f"Query failed: {message}")

    columns: list[str] = []
    if response.manifest and response.manifest.schema and response.manifest.schema.columns:
        columns = [c.name for c in response.manifest.schema.columns]

    data = []
    if response.result and response.result.data_array:
        data = response.result.data_array

    rows = [dict(zip(columns, row)) for row in data]
    truncated = bool(response.manifest and response.manifest.truncated)
    return {"columns": columns, "rows": rows, "row_count": len(rows), "truncated": truncated}


def _require_writes() -> None:
    """Gate for any tool that changes the workspace.

    Raises unless writes are explicitly enabled. The token's Unity Catalog and
    workspace permissions are the real boundary; this flag just means a read-only
    setup can't mutate anything even by mistake.
    """
    if not ALLOW_WRITES:
        raise PermissionError(
            "This operation changes your Databricks workspace and is disabled. "
            "Set DATABRICKS_MCP_ALLOW_WRITES=1 and use a token permitted to make the change."
        )


def _api(method: str, path: str, body: Optional[dict] = None, query: Optional[dict] = None) -> Any:
    """Call the Databricks REST API directly and return the parsed JSON.

    This is the general lever the write tools are built on: the SDK's typed
    helpers cover reads nicely, but create/update payloads for jobs, pipelines
    and dashboards are large nested JSON, which the REST endpoints take as-is.
    """
    return client().api_client.do(method.upper(), path, body=body, query=query)


# --- Tools ------------------------------------------------------------------

@server.tool()
def list_schemas(catalog: str = DEFAULT_CATALOG) -> dict:
    """List the schemas (databases) in a Unity Catalog catalog.

    Defaults to the ``workspace`` catalog, where Sentinel's ``sentinel`` schema
    lives.
    """
    return _run(f"SHOW SCHEMAS IN {catalog}")


@server.tool()
def list_tables(schema: str = DEFAULT_SCHEMA, catalog: str = DEFAULT_CATALOG) -> dict:
    """List the tables in ``catalog.schema``. Defaults to ``workspace.sentinel``."""
    return _run(f"SHOW TABLES IN {catalog}.{schema}")


@server.tool()
def describe_table(table: str) -> dict:
    """Show columns, types, comments and partitioning for a table.

    ``table`` may be bare (``agg_trades``), schema-qualified
    (``sentinel.agg_trades``) or fully qualified (``workspace.sentinel.agg_trades``).
    """
    return _run(f"DESCRIBE TABLE EXTENDED {_qualify(table)}")


@server.tool()
def preview_table(table: str, limit: int = 20) -> dict:
    """Return the first ``limit`` rows of a table (default 20).

    ``limit`` is capped by ``DATABRICKS_MCP_MAX_ROWS`` (default 1000) so a preview
    can never pull an unbounded amount of data into the chat.
    """
    limit = max(1, min(limit, MAX_ROWS))
    return _run(f"SELECT * FROM {_qualify(table)} LIMIT {limit}")


@server.tool()
def run_sql(statement: str) -> dict:
    """Run a read-only SQL query against the configured SQL warehouse.

    By default only ``SELECT`` / ``WITH`` / ``SHOW`` / ``DESCRIBE`` / ``EXPLAIN``
    statements are allowed, and only one statement at a time. Set
    ``DATABRICKS_MCP_ALLOW_WRITES=1`` to lift the guard — but prefer a
    least-privilege token so writes are impossible regardless of this flag.
    """
    trimmed = statement.strip().rstrip(";").strip()
    if not ALLOW_WRITES and ";" in trimmed:
        raise ValueError(
            "Refusing to run more than one statement at a time. Send a single query."
        )

    keyword = _first_keyword(trimmed)
    if not ALLOW_WRITES and keyword not in READ_ONLY_KEYWORDS:
        raise ValueError(
            f"Refusing to run a '{keyword or 'blank'}' statement: this server is read-only. "
            "Set DATABRICKS_MCP_ALLOW_WRITES=1 to allow writes."
        )
    return _run(trimmed)


@server.tool()
def sentinel_data_summary() -> dict:
    """Per-(symbol, month) row counts and time bounds for the Sentinel tables.

    A quick "what data do I actually have loaded?" across ``agg_trades`` and
    ``klines_1m`` — the same shape as the validation cell in the ingest notebook.
    """
    query = f"""
        SELECT 'agg_trades' AS source_table, symbol, month, COUNT(*) AS rows,
               MIN(transact_time) AS first_event, MAX(transact_time) AS last_event
        FROM {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.agg_trades
        GROUP BY symbol, month
        UNION ALL
        SELECT 'klines_1m' AS source_table, symbol, month, COUNT(*) AS rows,
               MIN(open_time) AS first_event, MAX(open_time) AS last_event
        FROM {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.klines_1m
        GROUP BY symbol, month
        ORDER BY source_table, symbol, month
    """
    return _run(query)


@server.tool()
def list_jobs(limit: int = 20) -> dict:
    """List Databricks Jobs in the workspace (id and name).

    Useful for finding the ingest job so you can check its recent runs.
    """
    jobs = []
    for index, job in enumerate(client().jobs.list()):
        if index >= max(1, limit):
            break
        settings = getattr(job, "settings", None)
        jobs.append({"job_id": job.job_id, "name": getattr(settings, "name", None)})
    return {"jobs": jobs}


@server.tool()
def recent_job_runs(job_id: Optional[int] = None, limit: int = 10) -> dict:
    """Recent job runs with their status, optionally filtered to one ``job_id``.

    Answers "did my last ingest run succeed?" without opening the Databricks UI.
    """
    runs = []
    listing = client().jobs.list_runs(job_id=job_id, expand_tasks=False)
    for index, run in enumerate(listing):
        if index >= max(1, limit):
            break
        state = getattr(run, "state", None)
        runs.append(
            {
                "run_id": getattr(run, "run_id", None),
                "run_name": getattr(run, "run_name", None),
                "life_cycle_state": getattr(getattr(state, "life_cycle_state", None), "value", None),
                "result_state": getattr(getattr(state, "result_state", None), "value", None),
                "start_time": getattr(run, "start_time", None),
            }
        )
    return {"runs": runs}


@server.tool()
def trigger_job(job_id: int) -> dict:
    """Start a Databricks Job now and return its run id.

    This is a write operation, so it is disabled unless
    ``DATABRICKS_MCP_ALLOW_WRITES=1``. Use it to kick off the ingest job from a
    chat once you trust the setup.
    """
    if not ALLOW_WRITES:
        raise PermissionError(
            "Triggering jobs is disabled. Set DATABRICKS_MCP_ALLOW_WRITES=1 to enable it."
        )
    waiter = client().jobs.run_now(job_id=job_id)
    return {"job_id": job_id, "run_id": getattr(waiter, "run_id", None), "status": "started"}


@server.tool()
def import_notebook(
    workspace_path: str, source: str, language: str = "PYTHON", overwrite: bool = True
) -> dict:
    """Create or overwrite a notebook / Python file in the workspace.

    This is "add Python code directly inside Databricks": ``workspace_path`` is an
    absolute path such as ``/Workspace/Users/you@example.com/sentinel/new_notebook``,
    ``source`` is the file's text (a source-format ``.py`` notebook or plain
    Python), and ``language`` is one of PYTHON / SQL / SCALA / R. Requires writes.
    """
    _require_writes()
    encoded = base64.b64encode(source.encode("utf-8")).decode("ascii")
    _api(
        "POST",
        "/api/2.0/workspace/import",
        body={
            "path": workspace_path,
            "format": "SOURCE",
            "language": language,
            "content": encoded,
            "overwrite": overwrite,
        },
    )
    return {"status": "imported", "path": workspace_path, "language": language}


@server.tool()
def delete_workspace_object(workspace_path: str, recursive: bool = False) -> dict:
    """Delete a notebook, file or folder from the workspace.

    Set ``recursive=True`` to remove a non-empty folder. Requires writes.
    """
    _require_writes()
    _api("POST", "/api/2.0/workspace/delete", body={"path": workspace_path, "recursive": recursive})
    return {"status": "deleted", "path": workspace_path}


@server.tool()
def databricks_request(method: str, path: str, body: Optional[dict] = None) -> Any:
    """Call any Databricks REST endpoint — the general lever for building, changing
    and deleting jobs, pipelines and dashboards.

    ``GET`` is always allowed; any other method requires writes. ``path`` starts
    with ``/api/...`` and ``body`` is the JSON the endpoint documents. Examples:

    - Create a job:       POST   /api/2.1/jobs/create          {"name": ..., "tasks": [...]}
    - Update a job:       POST   /api/2.1/jobs/reset           {"job_id": ..., "new_settings": {...}}
    - Delete a job:       POST   /api/2.1/jobs/delete          {"job_id": 123}
    - Create a pipeline:  POST   /api/2.0/pipelines            {"name": ..., "libraries": [...]}
    - Delete a pipeline:  DELETE /api/2.0/pipelines/{id}
    - Create a dashboard: POST   /api/2.0/lakeview/dashboards  {"display_name": ..., "serialized_dashboard": ...}
    - Delete a dashboard: DELETE /api/2.0/lakeview/dashboards/{id}

    Prefer the specific tools (preview_table, import_notebook, ...) when one fits;
    reach for this when nothing else does.
    """
    if method.upper() != "GET":
        _require_writes()
    return _api(method, path, body=body)


if __name__ == "__main__":
    # Transport selection:
    #   stdio (default)  -> a local client (Claude Desktop, Claude Code) spawns
    #                       this process and speaks MCP over stdin/stdout.
    #   http             -> a long-running HTTP server, so the same code can be
    #                       hosted (e.g. as a Databricks App) and added to
    #                       claude.ai as a Connector, reachable from the browser.
    transport = os.getenv("MCP_TRANSPORT", "stdio").lower()
    if transport in ("http", "streamable-http"):
        server.run(transport="streamable-http")
    else:
        server.run(transport="stdio")
