#!/usr/bin/env python
"""
Multi-tenant entry point for mcp-datto-psa.

Run with:
    python server_multitenant.py

Environment variables (none of them are credentials):
    HOST                Bind host (default: 0.0.0.0)
    PORT                Bind port (default: 8000)
    LOG_LEVEL           Log level (default: INFO)
    MULTI_TENANT_ONLY   true|false (default: true)
                        When true, requests without a tenant block are rejected.
                        When false, the server falls back to AUTOTASK_* env vars.
    DATTO_POOL_MAX      Max pooled clients (default: 50)
    DATTO_POOL_TTL_SEC  Idle TTL for pooled clients (default: 1800)

Per-request tenant block (sent as JSON-RPC tools/call argument named "tenant"):
    {
        "tenantId":         "<unique id, e.g. orgId-userId>",
        "username":         "<Autotask API username>",
        "secret":           "<Autotask API secret>",
        "integrationCode":  "<Autotask integration code>",
        "apiUrl":           "<optional, e.g. https://webservicesN.autotask.net/atservicesrest>",
        "impersonationResourceId": <optional int>
    }

This entry point:
    1. Imports the existing FastMCP server from server.py (zero modifications).
    2. Preserves the original env-var _get_client as _get_client_orig.
    3. Replaces server._get_client with the per-tenant pooled version.
    4. Builds the streamable-HTTP ASGI app and wraps it with TenantMiddleware.
    5. Starts uvicorn.
"""

from __future__ import annotations

import logging
import os


def _configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


_configure_logging()

# Imports must come AFTER logging is configured so module-init logs are formatted.
import server  # noqa: E402
import multitenant  # noqa: E402


# Preserve the original env-var _get_client so MULTI_TENANT_ONLY=false can fall
# back to it (legacy single-tenant mode, useful only for nostalgia/tests).
server._get_client_orig = server._get_client

# Replace _get_client with the multi-tenant variant. Tools resolve _get_client
# by name at each call, so this takes effect immediately for every tool.
server._get_client = multitenant.multitenant_get_client


def _build_fastmcp_http_app():
    """Return the FastMCP streamable-HTTP ASGI app, with version tolerance."""
    mcp = server.mcp
    if hasattr(mcp, "streamable_http_app"):
        return mcp.streamable_http_app()
    if hasattr(mcp, "http_app"):
        return mcp.http_app()
    raise RuntimeError(
        "FastMCP does not expose a known HTTP app builder "
        "(streamable_http_app or http_app). Check your `mcp` package version."
    )


def build_app():
    """Build the ASGI app: HealthzMiddleware ▸ TenantMiddleware ▸ FastMCP.

    Health checks short-circuit before any tenant logic so probes don't need
    credentials and don't pollute the rpc_in/rpc_out logs.
    """
    inner = _build_fastmcp_http_app()
    tenant_layer = multitenant.TenantMiddleware(inner)
    return multitenant.HealthzMiddleware(tenant_layer)


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    log_level = os.getenv("LOG_LEVEL", "info").lower()

    log = logging.getLogger("mcp_datto_psa.boot")
    log.info(
        "starting host=%s port=%s multi_tenant_only=%s pool_max=%s pool_ttl=%s",
        host,
        port,
        multitenant.MULTI_TENANT_ONLY,
        multitenant.POOL_MAX,
        multitenant.POOL_TTL_SEC,
    )
    if not multitenant.MULTI_TENANT_ONLY:
        log.warning(
            "MULTI_TENANT_ONLY=false: requests without a tenant block will fall "
            "back to AUTOTASK_* env vars. Do NOT use in production."
        )

    access_log = os.getenv("ACCESS_LOG", "true").lower() == "true"

    app = build_app()
    uvicorn.run(app, host=host, port=port, log_level=log_level, access_log=access_log)
