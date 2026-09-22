"""HTTP hosting entry point for the MCP server (Databricks App, or any HTTPS host).

Two uses:

- **As a module** it exposes ``app``, the Starlette ASGI application, with the MCP
  endpoint mounted at ``/mcp``. Point an ASGI server at ``app_http:app``.
- **Run directly** (``python app_http.py``) it serves ``app`` on ``0.0.0.0`` and the
  port named by ``DATABRICKS_APP_PORT`` (default 8000). Databricks Apps always set
  that variable, so reading it here avoids depending on command-line substitution.

The URL a Claude connector points at is ``https://<host>/mcp``.
"""

from __future__ import annotations

import os

# server.py has no intra-package imports, so this works whether the file is
# imported as part of the databricks_mcp package (local: ``databricks_mcp.app_http``)
# or as a top-level module from a flat working directory — which is how a Databricks
# App runs it, since ``source_code_path`` uploads this folder's contents as the app root.
try:
    from databricks_mcp.server import server
except ModuleNotFoundError:  # flat working dir on Databricks Apps
    from server import server

# stateless_http keeps each request self-contained, so the endpoint stays correct
# even if the host runs more than one replica behind a load balancer.
app = server.streamable_http_app(stateless_http=True)


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("DATABRICKS_APP_PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
