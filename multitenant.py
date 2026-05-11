"""
Multi-tenant adapter layer for mcp-datto-psa.

This module is a self-contained "sidecar" that turns the existing single-tenant
MCP server into a multi-tenant server WITHOUT modifying any of the original
files (server.py, api_client.py, formatters.py).

How it works
------------
1. A pure ASGI middleware (``TenantMiddleware``) extracts a ``tenant`` block from
   each JSON-RPC ``tools/call`` request, stores it in a ``ContextVar``, and
   strips it from the JSON body before forwarding the request to FastMCP.
2. ``server._get_client`` is monkey-patched to return ``_PooledHandle()``. The
   handle reads the tenant from the ``ContextVar`` at ``__aenter__`` time and
   returns a per-tenant ``AutotaskClient`` from a process-wide LRU pool.
3. When ``MULTI_TENANT_ONLY`` is true (default), requests without valid tenant
   credentials are rejected with a 401 JSON-RPC error. No env-var fallback.

Safety properties
-----------------
- Pool key includes ``tenantId`` + ``username`` + ``integrationCode`` +
  ``impersonationResourceId`` to prevent cross-tenant collision.
- Pooled clients are NEVER closed by tool code (handle ``__aexit__`` is a no-op
  beyond decrementing a refcount).
- Stored client credentials are verified against the requested tenant on every
  pool hit; mismatch evicts the entry (defense in depth).
- Body bytes are read once and replayed exactly once to the downstream app.
- Logs use a SHA-256 fingerprint, never raw credentials.
"""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("mcp_datto_psa.multitenant")


# ─── Tenant context ───────────────────────────────────────────────────

_tenant_var: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "datto_tenant", default=None
)


def current_tenant() -> dict | None:
    """Return the tenant for the current request, or None."""
    return _tenant_var.get()


# ─── Configuration (env-driven, but not secrets) ──────────────────────

MULTI_TENANT_ONLY = os.getenv("MULTI_TENANT_ONLY", "true").lower() == "true"
POOL_MAX = int(os.getenv("DATTO_POOL_MAX", "50"))
POOL_TTL_SEC = int(os.getenv("DATTO_POOL_TTL_SEC", "1800"))  # 30 minutes


# ─── Pool ──────────────────────────────────────────────────────────────

@dataclass
class _Entry:
    client: Any  # AutotaskClient
    last_used: datetime
    refcount: int = 0
    closing: bool = False


class _ClientPool:
    """LRU + TTL pool of ``AutotaskClient`` instances, keyed by tenant fingerprint."""

    def __init__(self, max_size: int = POOL_MAX, ttl_seconds: int = POOL_TTL_SEC):
        self._max = max_size
        self._ttl = ttl_seconds
        self._entries: dict[str, _Entry] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def fingerprint(tenant: dict) -> str:
        """Stable, non-reversible identifier for a tenant credential set.

        Includes ``tenantId`` so two different Darcy orgs that happen to share
        Autotask credentials still get separate pool entries (and isolated
        per-instance caches).
        """
        raw = "|".join(
            [
                str(tenant.get("tenantId") or ""),
                str(tenant["username"]),
                str(tenant["integrationCode"]),
                str(tenant.get("impersonationResourceId") or ""),
            ]
        )
        return hashlib.sha256(raw.encode()).hexdigest()

    async def acquire(self, tenant: dict) -> tuple[Any, str, _Entry]:
        # Local import keeps this module importable even without aiohttp installed
        # at parse time (e.g. for static analysis / tests).
        from api_client import AutotaskClient

        key = self.fingerprint(tenant)
        now = datetime.now(timezone.utc)

        async with self._lock:
            entry = self._entries.get(key)

            # Defense in depth: pool entries should never serve a different tenant.
            if entry is not None:
                c = entry.client
                if (
                    c.username != tenant["username"]
                    or c.secret != tenant["secret"]
                    or c.integration_code != tenant["integrationCode"]
                ):
                    logger.error(
                        "pool_entry_credential_mismatch fp=%s — evicting",
                        key[:12],
                    )
                    entry.closing = True
                    entry = None  # force re-create below

            # Honor TTL
            if entry is not None and not entry.closing:
                age = (now - entry.last_used).total_seconds()
                if age >= self._ttl:
                    entry.closing = True
                    entry = None

            if entry is not None:
                entry.last_used = now
                entry.refcount += 1
                return entry.client, key, entry

            # Cold path: create a fresh client.
            if len(self._entries) >= self._max:
                self._evict_oldest_unused_locked()

            client = AutotaskClient(
                tenant["username"],
                tenant["secret"],
                tenant["integrationCode"],
                tenant.get("apiUrl") or "",
            )
            new_entry = _Entry(client=client, last_used=now, refcount=1)
            self._entries[key] = new_entry
            logger.info(
                "pool_create fp=%s size=%d", key[:12], len(self._entries)
            )
            return client, key, new_entry

    async def release(self, key: str, entry: _Entry) -> None:
        async with self._lock:
            entry.refcount = max(0, entry.refcount - 1)
            if entry.closing and entry.refcount == 0:
                try:
                    await entry.client.close()
                finally:
                    self._entries.pop(key, None)
                    logger.info("pool_drained fp=%s", key[:12])

    def _evict_oldest_unused_locked(self) -> None:
        """Mark the oldest idle entry for closure. Caller holds ``_lock``."""
        candidates = [
            (k, e) for k, e in self._entries.items() if e.refcount == 0 and not e.closing
        ]
        if not candidates:
            # All entries are in use. Allow temporary overflow rather than
            # cancel an in-flight request.
            logger.warning("pool_overflow size=%d", len(self._entries))
            return
        candidates.sort(key=lambda kv: kv[1].last_used)
        k, e = candidates[0]
        e.closing = True
        # Schedule the actual close outside the lock.
        asyncio.create_task(self._close_evicted(k, e))

    async def _close_evicted(self, key: str, entry: _Entry) -> None:
        async with self._lock:
            if entry.refcount != 0:
                return  # someone is still using it; release() will close it
            try:
                await entry.client.close()
            except Exception:
                logger.exception("pool_close_error fp=%s", key[:12])
            finally:
                self._entries.pop(key, None)
                logger.info("pool_evict fp=%s", key[:12])

    async def close_all(self) -> None:
        async with self._lock:
            entries = list(self._entries.items())
            self._entries.clear()
        for key, entry in entries:
            try:
                await entry.client.close()
            except Exception:
                logger.exception("pool_close_error fp=%s", key[:12])

    def stats(self) -> dict:
        return {
            "size": len(self._entries),
            "max": self._max,
            "ttl_sec": self._ttl,
        }


_pool = _ClientPool()


# ─── Pooled async-context handle ──────────────────────────────────────

class _PooledHandle:
    """Returned by ``multitenant_get_client``. Behaves like an
    ``async with AutotaskClient(...) as c:`` block but delegates lifecycle
    to the pool.

    Tenant is read at ``__aenter__`` time (latest possible moment) so no
    stale ``ContextVar`` value can be captured at construction.
    """

    __slots__ = ("_key", "_entry")

    def __init__(self) -> None:
        self._key: str | None = None
        self._entry: _Entry | None = None

    async def __aenter__(self):
        tenant = _tenant_var.get()
        if tenant is None:
            raise RuntimeError(
                "No tenant in context. Send a 'tenant' block in tool arguments "
                "(username, secret, integrationCode required)."
            )
        client, key, entry = await _pool.acquire(tenant)
        self._key = key
        self._entry = entry
        return client

    async def __aexit__(self, exc_type, exc, tb):
        if self._entry is not None and self._key is not None:
            await _pool.release(self._key, self._entry)
        # Don't suppress exceptions
        return False


def multitenant_get_client():
    """Drop-in replacement for ``server._get_client``.

    - In multi-tenant mode (default), returns a ``_PooledHandle`` regardless of
      env vars. Resolution happens at ``__aenter__`` time.
    - When ``MULTI_TENANT_ONLY=false`` AND no tenant is in context, falls back
      to the original env-var-based ``_get_client_orig`` (for legacy single-
      tenant deployments). The original is preserved in
      ``server._get_client_orig`` by ``server_multitenant.py``.
    """
    if not MULTI_TENANT_ONLY and _tenant_var.get() is None:
        import server as _server_mod  # late import; module exists once patched
        return _server_mod._get_client_orig()
    return _PooledHandle()


# ─── ASGI middleware ──────────────────────────────────────────────────

_REQUIRED_TENANT_KEYS = ("username", "secret", "integrationCode")
_TENANT_ARG_KEYS = ("tenant", "_tenant", "credentials")


def _pop_tenant(arguments: Any) -> dict | None:
    """Pop a tenant block from ``arguments`` (mutates in place). Returns the
    tenant dict if it has the required keys, else ``None`` (and the bad block
    is dropped).
    """
    if not isinstance(arguments, dict):
        return None
    for k in _TENANT_ARG_KEYS:
        if k in arguments:
            t = arguments.pop(k)
            if isinstance(t, dict) and all(t.get(r) for r in _REQUIRED_TENANT_KEYS):
                return t
            logger.warning("tenant_block_invalid key=%s", k)
            return None
    return None


def _process_rpc(rpc: dict) -> tuple[dict | None, str | None]:
    """Extract & strip tenant from one JSON-RPC envelope.

    Returns ``(tenant_or_None, error_message_or_None)``. A non-None error
    message means the caller should reject the entire request.
    """
    if rpc.get("method") != "tools/call":
        return None, None
    params = rpc.get("params")
    if not isinstance(params, dict):
        return None, None
    args = params.get("arguments")
    tenant = _pop_tenant(args)
    if tenant is None:
        if MULTI_TENANT_ONLY:
            return None, (
                "Missing or invalid tenant credentials. Include a 'tenant' block "
                "in arguments with username, secret, and integrationCode."
            )
        return None, None
    return tenant, None


def _error_response_bytes(request_id: Any, message: str) -> bytes:
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32001, "message": message},
        }
    ).encode("utf-8")


# ─── Logging helpers ──────────────────────────────────────────────────

# Keys whose values must NEVER appear verbatim in logs, even though tenant
# blocks are stripped before this point.
_REDACT_KEYS = {"secret", "password", "token", "apiintegrationcode", "authorization"}
_MAX_VALUE_LEN = 200


def _sanitize_args(args: Any) -> Any:
    """Return a log-safe copy of tool arguments.

    - Redacts known sensitive keys (defense-in-depth; tenant is already stripped).
    - Truncates long string values.
    - Replaces large lists/dicts with shape descriptors.
    """
    if not isinstance(args, dict):
        return args
    out: dict[str, Any] = {}
    for k, v in args.items():
        if isinstance(k, str) and k.lower() in _REDACT_KEYS:
            out[k] = "***"
        elif isinstance(v, str):
            out[k] = (v[:_MAX_VALUE_LEN] + "…") if len(v) > _MAX_VALUE_LEN else v
        elif isinstance(v, (int, float, bool)) or v is None:
            out[k] = v
        elif isinstance(v, list):
            out[k] = f"[len={len(v)}]"
        elif isinstance(v, dict):
            out[k] = f"{{keys={list(v.keys())[:8]}}}"
        else:
            out[k] = type(v).__name__
    return out


def _tenant_label(tenant: dict | None) -> str:
    """Human-readable tenant label for logs (tenantId or username)."""
    if not tenant:
        return "(none)"
    return str(tenant.get("tenantId") or tenant.get("username") or "(unknown)")


def _parse_jsonrpc_from_body(body_bytes: bytes, content_type: str) -> list[dict]:
    """Best-effort extraction of JSON-RPC envelopes from a response body.

    Handles both ``application/json`` and ``text/event-stream`` (SSE) formats.
    Returns a (possibly empty) list of envelope dicts.
    """
    if not body_bytes:
        return []
    text = body_bytes.decode("utf-8", errors="replace")

    if "event-stream" in content_type:
        envelopes: list[dict] = []
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("data:"):
                payload = line[len("data:"):].strip()
                if not payload:
                    continue
                try:
                    parsed = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict):
                    envelopes.append(parsed)
                elif isinstance(parsed, list):
                    envelopes.extend(p for p in parsed if isinstance(p, dict))
        return envelopes

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, list):
        return [p for p in parsed if isinstance(p, dict)]
    if isinstance(parsed, dict):
        return [parsed]
    return []


def _summarize_response_envelope(env: dict) -> str:
    """One-token summary of a JSON-RPC response envelope, suitable for a log line."""
    rpc_id = env.get("id", "?")
    if "error" in env:
        err = env["error"] if isinstance(env["error"], dict) else {}
        msg = (err.get("message") or "")[:80]
        return f"id={rpc_id} status=error code={err.get('code', '?')} msg={msg!r}"
    if "result" in env:
        result = env["result"]
        if isinstance(result, dict):
            if isinstance(result.get("content"), list):
                content = result["content"]
                first_text = ""
                if content and isinstance(content[0], dict):
                    first_text = (content[0].get("text") or "")[:120]
                return (
                    f"id={rpc_id} status=ok content_items={len(content)} "
                    f"is_error={result.get('isError', False)} first={first_text!r}"
                )
            if isinstance(result.get("tools"), list):
                return f"id={rpc_id} status=ok tools_count={len(result['tools'])}"
            if "protocolVersion" in result:
                return f"id={rpc_id} status=ok handshake=initialize"
            return f"id={rpc_id} status=ok keys={list(result.keys())[:6]}"
        return f"id={rpc_id} status=ok"
    return f"id={rpc_id} status=unknown"


class _ResponseCapture:
    """Wraps an ASGI ``send`` callable to capture status + content-type + body
    while still forwarding every message to the real send (so the downstream
    consumer is not delayed)."""

    __slots__ = ("_send", "status", "content_type", "_chunks")

    def __init__(self, send):
        self._send = send
        self.status: int | None = None
        self.content_type: str = ""
        self._chunks: list[bytes] = []

    async def __call__(self, message):
        if message["type"] == "http.response.start":
            self.status = message.get("status")
            for k, v in message.get("headers") or []:
                if k.lower() == b"content-type":
                    self.content_type = v.decode("latin-1", errors="replace").lower()
                    break
        elif message["type"] == "http.response.body":
            self._chunks.append(message.get("body") or b"")
        await self._send(message)

    @property
    def body(self) -> bytes:
        return b"".join(self._chunks)


class TenantMiddleware:
    """Pure-ASGI middleware that extracts & strips a per-request tenant block
    from JSON-RPC ``tools/call`` arguments, sets it on a ``ContextVar``, and
    forwards the cleaned body to the downstream app.

    Wrap any ASGI app::

        app = TenantMiddleware(fastmcp_streamable_http_app)
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("method") != "POST":
            await self.app(scope, receive, send)
            return

        # Read the full body (MCP messages are small).
        chunks: list[bytes] = []
        more_body = True
        while more_body:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            if message["type"] != "http.request":
                # Unknown message type; pass through with an empty receive.
                break
            chunks.append(message.get("body") or b"")
            more_body = message.get("more_body", False)
        raw_body = b"".join(chunks)

        try:
            payload = json.loads(raw_body) if raw_body else None
        except json.JSONDecodeError:
            payload = None

        rpc_envelopes: list[dict]
        if isinstance(payload, dict):
            rpc_envelopes = [payload]
        elif isinstance(payload, list):
            rpc_envelopes = [r for r in payload if isinstance(r, dict)]
        else:
            rpc_envelopes = []

        tenants: list[dict] = []
        for rpc in rpc_envelopes:
            tenant, err = _process_rpc(rpc)
            if err is not None:
                body = _error_response_bytes(rpc.get("id"), err)
                logger.warning(
                    "rpc_rejected method=%s id=%s reason=%s",
                    rpc.get("method"),
                    rpc.get("id"),
                    err,
                )
                await self._send_error(send, 401, body)
                return
            if tenant is not None:
                tenants.append(tenant)

        # Disallow mixed-tenant batches (different fingerprints in one POST).
        if len({_ClientPool.fingerprint(t) for t in tenants}) > 1:
            first_id = rpc_envelopes[0].get("id") if rpc_envelopes else None
            body = _error_response_bytes(
                first_id, "Mixed-tenant JSON-RPC batches are not allowed."
            )
            logger.warning("rpc_rejected reason=mixed_tenant_batch")
            await self._send_error(send, 400, body)
            return

        token: contextvars.Token | None = None
        fp_short = ""
        tenant_label = _tenant_label(tenants[0] if tenants else None)
        if tenants:
            token = _tenant_var.set(tenants[0])
            fp_short = _ClientPool.fingerprint(tenants[0])[:12]

        # One ``rpc_in`` line per inbound RPC envelope.
        for rpc in rpc_envelopes:
            method = rpc.get("method", "?")
            rpc_id = rpc.get("id", "?")
            if method == "tools/call":
                params = rpc.get("params") or {}
                tool = params.get("name", "?")
                args = params.get("arguments") or {}
                logger.info(
                    "rpc_in method=tools/call tool=%s id=%s tenant=%s fp=%s args=%s",
                    tool,
                    rpc_id,
                    tenant_label,
                    fp_short or "(none)",
                    _sanitize_args(args),
                )
            else:
                logger.info(
                    "rpc_in method=%s id=%s tenant=%s",
                    method,
                    rpc_id,
                    tenant_label,
                )

        # If we extracted a tenant, the payload was mutated in place; re-serialize.
        # Otherwise pass the original bytes through untouched (cheaper + safer:
        # avoids any whitespace/key-order drift that might surprise downstream).
        if tenants:
            new_body = json.dumps(payload).encode("utf-8")
        else:
            new_body = raw_body

        replayed = {"done": False}

        async def replay_receive():
            if not replayed["done"]:
                replayed["done"] = True
                return {"type": "http.request", "body": new_body, "more_body": False}
            # After our body, forward to the real receive() so the downstream
            # handler can wait on a genuine http.disconnect from the client.
            # Returning http.disconnect ourselves cancels FastMCP's session
            # background task and triggers a TaskGroup error.
            return await receive()

        capture = _ResponseCapture(send)
        t0 = time.monotonic()
        try:
            await self.app(scope, replay_receive, capture)
        finally:
            if token is not None:
                _tenant_var.reset(token)

        # Emit one ``rpc_out`` line per JSON-RPC envelope in the response body.
        # If the body cannot be parsed as JSON-RPC (e.g. opaque streaming or empty
        # 202 ack), emit a single shape-only summary instead.
        duration_ms = int((time.monotonic() - t0) * 1000)
        envelopes = _parse_jsonrpc_from_body(capture.body, capture.content_type)
        if envelopes:
            for env in envelopes:
                logger.info(
                    "rpc_out tenant=%s http=%s duration_ms=%d %s",
                    tenant_label,
                    capture.status,
                    duration_ms,
                    _summarize_response_envelope(env),
                )
        else:
            logger.info(
                "rpc_out tenant=%s http=%s duration_ms=%d body_len=%d ctype=%s (no jsonrpc parsed)",
                tenant_label,
                capture.status,
                duration_ms,
                len(capture.body),
                capture.content_type,
            )

    async def _send_error(self, send, status: int, body: bytes) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body, "more_body": False})


# ─── Health endpoint ──────────────────────────────────────────────────


_HEALTH_PATHS = frozenset({"/healthz", "/health", "/ready"})


class HealthzMiddleware:
    """Minimal ASGI shim that answers ``GET /healthz`` (and ``/health``,
    ``/ready``) with a tiny JSON body before the request reaches the rest of
    the stack. Intended to sit OUTSIDE ``TenantMiddleware`` so health probes
    never need a tenant block.

    Response body includes pool size for cheap operational visibility.
    """

    __slots__ = ("app",)

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if (
            scope["type"] == "http"
            and scope.get("method") == "GET"
            and scope.get("path") in _HEALTH_PATHS
        ):
            payload = {
                "status": "ok",
                "multi_tenant_only": MULTI_TENANT_ONLY,
                "pool": _pool.stats(),
            }
            body = json.dumps(payload).encode("utf-8")
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(body)).encode()),
                        (b"cache-control", b"no-store"),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": body, "more_body": False})
            return
        await self.app(scope, receive, send)
