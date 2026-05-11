"""
Local smoke test for the multi-tenant mcp-datto-psa server.

This client reads Autotask credentials from your shell env and sends them as
the per-request `tenant` block to the server. The server itself does NOT need
AUTOTASK_* env vars — only this client does, because it's pretending to be
Darcy injecting credentials per call.

Usage:
    # 1) In one terminal, start the server (no env vars needed):
    cd mcp-datto-psa
    python server_multitenant.py

    # 2) In another terminal, run this smoke test:
    cd mcp-datto-psa
    AUTOTASK_USERNAME=...   \\
    AUTOTASK_SECRET=...     \\
    AUTOTASK_INTEGRATION_CODE=... \\
        python tools/local_smoke.py

Optional env vars:
    SERVER_URL              default: http://localhost:8000/mcp
    TENANT_ID               default: local-dev-tenant
    AUTOTASK_API_URL        Autotask zone URL (e.g. https://webservicesN.autotask.net/atservicesrest)
    SMOKE_TOOL              tool to call (default: search_resources)
    SMOKE_ARGS_JSON         extra args as JSON (default: {"active_only": true})

Examples:
    # call analyze_hours instead
    SMOKE_TOOL=analyze_hours \\
    SMOKE_ARGS_JSON='{"date_from":"2025-01-01","date_to":"2025-01-31"}' \\
        python tools/local_smoke.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys


def _tenant_from_env() -> dict:
    required = ("AUTOTASK_USERNAME", "AUTOTASK_SECRET", "AUTOTASK_INTEGRATION_CODE")
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        sys.exit(
            "Missing env vars on the CLIENT side (the server is multi-tenant "
            f"and does not need them): {missing}"
        )
    tenant = {
        "tenantId": os.getenv("TENANT_ID", "local-dev-tenant"),
        "username": os.environ["AUTOTASK_USERNAME"],
        "secret": os.environ["AUTOTASK_SECRET"],
        "integrationCode": os.environ["AUTOTASK_INTEGRATION_CODE"],
    }
    if os.getenv("AUTOTASK_API_URL"):
        tenant["apiUrl"] = os.environ["AUTOTASK_API_URL"]
    if os.getenv("AUTOTASK_IMPERSONATION_RESOURCE_ID"):
        try:
            tenant["impersonationResourceId"] = int(
                os.environ["AUTOTASK_IMPERSONATION_RESOURCE_ID"]
            )
        except ValueError:
            sys.exit("AUTOTASK_IMPERSONATION_RESOURCE_ID must be an integer")
    return tenant


async def _run() -> int:
    # Local import so missing deps surface a clear error.
    try:
        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client
    except ImportError as e:
        sys.exit(
            f"mcp client SDK not available ({e}). Install with: pip install mcp"
        )

    server_url = os.getenv("SERVER_URL", "http://localhost:8000/mcp")
    tool_name = os.getenv("SMOKE_TOOL", "search_resources")

    extra_args_raw = os.getenv("SMOKE_ARGS_JSON", '{"active_only": true}')
    try:
        extra_args = json.loads(extra_args_raw)
    except json.JSONDecodeError as e:
        sys.exit(f"SMOKE_ARGS_JSON is not valid JSON: {e}")
    if not isinstance(extra_args, dict):
        sys.exit("SMOKE_ARGS_JSON must decode to a JSON object")

    tenant = _tenant_from_env()
    arguments = {**extra_args, "tenant": tenant}

    print(f"→ connecting to {server_url}")
    async with streamablehttp_client(server_url) as (read, write, _get_session_id):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = await session.list_tools()
            print(f"✓ connected; server exposes {len(tools.tools)} tools")
            names = ", ".join(t.name for t in tools.tools)
            print(f"  tools: {names}")

            print(f"\n→ calling tool: {tool_name}")
            print(f"  args (sans tenant): {json.dumps(extra_args)}")
            result = await session.call_tool(tool_name, arguments=arguments)

            if getattr(result, "isError", False):
                print("✗ tool returned an error")
                for c in result.content:
                    text = getattr(c, "text", None)
                    if text:
                        print(text)
                return 1

            print("✓ tool returned successfully\n")
            for c in result.content:
                text = getattr(c, "text", None)
                if text:
                    print(text[:4000])
                    if len(text) > 4000:
                        print(f"\n... ({len(text)-4000} more chars truncated)")
            return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_run()))
