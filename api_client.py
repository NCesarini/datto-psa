# Autotask REST API Client with pagination, caching, and name resolution

import asyncio
import json
import urllib.parse
from typing import Any, Optional
import aiohttp


# Default timeout for individual API requests (seconds)
REQUEST_TIMEOUT = 30

# Autotask enforces a 3-thread limit per API integration.  Thread cleanup on
# their side is not instant, so even semaphore=2 can cause brief 3-thread
# overlaps.  We serialize to 1 concurrent request and retry on 429.
MAX_CONCURRENT_REQUESTS = 1
_API_SEMAPHORE: Optional[asyncio.Semaphore] = None

# Retry settings for 429 (thread threshold) responses
MAX_RETRIES = 4
RETRY_BASE_DELAY = 1.0  # seconds; doubles each attempt


def _get_shared_semaphore() -> asyncio.Semaphore:
    """Return the process-wide API semaphore, creating it on first use."""
    global _API_SEMAPHORE
    if _API_SEMAPHORE is None:
        _API_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    return _API_SEMAPHORE


def merge_paging_status(aggregate: Optional[dict], partial: Optional[dict]) -> None:
    """Merge pagination metadata from one query_all_pages call into a cumulative dict for LLM-facing summaries."""
    if aggregate is None or not partial:
        return
    aggregate["pages_fetched"] = aggregate.get("pages_fetched", 0) + partial.get("pages_fetched", 0)
    aggregate["complete"] = aggregate.get("complete", True) and partial.get("complete", True)
    mrp = max(aggregate.get("max_records_per_page", 0), partial.get("max_records_per_page", 0))
    if mrp:
        aggregate["max_records_per_page"] = mrp
    if partial.get("failure"):
        aggregate.setdefault("failures", []).append(partial["failure"])
    if partial.get("failures"):
        aggregate.setdefault("failures", []).extend(partial["failures"])


class AutotaskClient:
    """Async client for the Autotask / Datto PSA REST API.

    Uses a shared aiohttp session for connection pooling.
    Call async close() or use as an async context manager to release resources.
    """

    def __init__(self, username: str, secret: str, integration_code: str, api_url: str = ""):
        self.username = username
        self.secret = secret
        self.integration_code = integration_code
        self.api_url = api_url.rstrip("/") if api_url else ""
        self.base_url = ""  # Set after zone detection

        # Shared session – created lazily on first request
        self._session: Optional[aiohttp.ClientSession] = None
        self._timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

        # Process-wide semaphore so concurrent tool calls share the same throttle
        self._semaphore = _get_shared_semaphore()

        # Caches for name resolution (per-request lifetime)
        self._resource_cache: dict[int, dict] = {}
        self._company_cache: dict[int, dict] = {}
        self._project_cache: dict[int, dict] = {}
        self._billing_code_cache: dict[int, dict] = {}
        self._contract_cache: dict[int, dict] = {}
        self._role_cache: dict[int, dict] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        """Return the shared session, creating it if necessary."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "UserName": self.username,
                    "Secret": self.secret,
                    "ApiIntegrationCode": self.integration_code,
                    "Content-Type": "application/json",
                },
                timeout=self._timeout,
            )
        return self._session

    async def close(self):
        """Close the shared HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    # Allow usage as async context manager
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def _ensure_base_url(self):
        """Detect the correct zone URL if not already set."""
        if self.base_url:
            return
        if self.api_url:
            self.base_url = f"{self.api_url}/v1.0"
            return
        # Auto-detect zone
        zone_url = f"https://webservices.autotask.net/atservicesrest/v1.0/zoneInformation?user={urllib.parse.quote(self.username)}"
        session = await self._get_session()
        async with session.get(zone_url) as resp:
            if resp.status == 200:
                data = await resp.json()
                self.base_url = data["url"].rstrip("/") + "/v1.0"
            else:
                raise Exception(f"Failed to detect Autotask zone. Status: {resp.status}")

    def _normalize_next_page_url(self, next_url: Optional[str]) -> Optional[str]:
        """Ensure nextPageUrl is absolute. Autotask may return a path-only URL; aiohttp needs a full URL."""
        if not next_url:
            return next_url
        u = next_url.strip()
        lu = u.lower()
        if lu.startswith("http://") or lu.startswith("https://"):
            return u
        base = (self.base_url or "").rstrip("/")
        if not base:
            return u
        return f"{base}/{u.lstrip('/')}"

    async def _request(self, method: str, path: str, body: dict = None) -> dict:
        """Make an authenticated request to the Autotask API.

        Serialized by the process-wide semaphore and retried with exponential
        backoff on 429 (thread threshold exceeded) responses.

        Zone discovery (_ensure_base_url) runs under the same semaphore so it
        cannot overlap another Autotask call — Autotask counts concurrent HTTP
        connections as "threads" (limit 3 per integration).
        """
        session = await self._get_session()
        kwargs: dict[str, Any] = {}
        if body:
            kwargs["json"] = body

        for attempt in range(MAX_RETRIES + 1):
            async with self._semaphore:
                await self._ensure_base_url()
                url = f"{self.base_url}/{path}"
                async with session.request(method, url, **kwargs) as resp:
                    status = resp.status
                    if status == 200:
                        return await resp.json()
                    text = await resp.text()
            # Backoff and retries happen outside the semaphore so we do not hold
            # the connection or block other API work during sleep.
            if status == 429 and attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                continue
            if status == 401:
                raise Exception("Authentication failed. Check AUTOTASK_USERNAME, AUTOTASK_SECRET, and AUTOTASK_INTEGRATION_CODE.")
            raise Exception(f"API error {status}: {text[:500]}")
        raise Exception(f"Max retries ({MAX_RETRIES}) exceeded for {method} {path}")

    async def get_entity(self, entity: str, entity_id: int) -> Optional[dict]:
        """Get a single entity by ID."""
        try:
            result = await self._request("GET", f"{entity}/{entity_id}")
            return result.get("item", result)
        except Exception:
            return None

    async def query_all_pages(self, entity: str, filters: list, max_records: int = 500, include_fields: list = None, paging_status: Optional[dict] = None) -> list:
        """Query an entity with automatic pagination. Returns all matching records.

        If paging_status is provided, it is cleared and filled with metadata for LLM-facing
        completeness notes: pages_fetched, complete, max_records_per_page, optional failures.
        """
        all_items = []
        search_body = {"filter": filters}
        if max_records:
            search_body["MaxRecords"] = max_records
        if include_fields:
            search_body["IncludeFields"] = include_fields

        pages_fetched_count = 0

        if paging_status is not None:
            paging_status.clear()
            paging_status["entity"] = entity
            paging_status["complete"] = True
            paging_status["max_records_per_page"] = max_records or 0

        # First request via POST for complex queries (throttled by semaphore)
        result = await self._request("POST", f"{entity}/query", search_body)
        items = result.get("items", [])
        all_items.extend(items)
        pages_fetched_count = 1

        # Follow pagination — each page must also respect the semaphore
        # to stay within Autotask's 3-thread limit
        page_details = result.get("pageDetails", {})
        next_url = page_details.get("nextPageUrl")
        if next_url:
            next_url = self._normalize_next_page_url(next_url)
        session = await self._get_session()

        while next_url:
            page_fetched = False
            for attempt in range(MAX_RETRIES + 1):
                status = 0
                async with self._semaphore:
                    async with session.get(next_url) as resp:
                        status = resp.status
                        if status == 200:
                            result = await resp.json()
                            items = result.get("items", [])
                            all_items.extend(items)
                            page_details = result.get("pageDetails", {})
                            next_url = page_details.get("nextPageUrl")
                            if next_url:
                                next_url = self._normalize_next_page_url(next_url)
                            page_fetched = True
                            pages_fetched_count += 1
                            break
                        await resp.text()
                if status == 429 and attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                    continue
                status_code = status
                next_url = None
                if paging_status is not None:
                    paging_status["complete"] = False
                    paging_status["failure"] = {
                        "phase": "get_next_page",
                        "http_status": status_code,
                        "entity": entity,
                    }
                break
            if not page_fetched:
                if paging_status is not None and paging_status.get("complete", True):
                    paging_status["complete"] = False
                    paging_status.setdefault("failure", {"phase": "get_next_page", "reason": "exhausted_retries", "entity": entity})
                break

        if paging_status is not None:
            paging_status["pages_fetched"] = pages_fetched_count
            paging_status["items_returned"] = len(all_items)

        return all_items

    async def query_count(self, entity: str, filters: list) -> int:
        """Get the count of records matching a filter."""
        result = await self._request("POST", f"{entity}/query/count", {"filter": filters})
        return result.get("queryCount", 0)

    # ─── NAME RESOLUTION ────────────────────────────────────────────

    async def resolve_resource_name(self, resource_id: int) -> str:
        """Resolve a resource ID to a display name."""
        if not resource_id:
            return "Unassigned"
        if resource_id not in self._resource_cache:
            r = await self.get_entity("Resources", resource_id)
            self._resource_cache[resource_id] = r or {}
        r = self._resource_cache[resource_id]
        return f"{r.get('firstName', '')} {r.get('lastName', '')}".strip() or f"Resource #{resource_id}"

    async def resolve_company_name(self, company_id: int) -> str:
        """Resolve a company ID to a name."""
        if not company_id:
            return "N/A"
        if company_id not in self._company_cache:
            c = await self.get_entity("Companies", company_id)
            self._company_cache[company_id] = c or {}
        return self._company_cache[company_id].get("companyName", f"Company #{company_id}")

    async def resolve_project_name(self, project_id: int) -> str:
        """Resolve a project ID to a name."""
        if not project_id:
            return "N/A"
        if project_id not in self._project_cache:
            p = await self.get_entity("Projects", project_id)
            self._project_cache[project_id] = p or {}
        return self._project_cache[project_id].get("projectName", f"Project #{project_id}")

    async def resolve_billing_code_name(self, billing_code_id: int) -> str:
        """Resolve a billing code ID to a name."""
        if not billing_code_id:
            return "N/A"
        if billing_code_id not in self._billing_code_cache:
            bc = await self.get_entity("BillingCodes", billing_code_id)
            self._billing_code_cache[billing_code_id] = bc or {}
        return self._billing_code_cache[billing_code_id].get("name", f"BillingCode #{billing_code_id}")

    async def resolve_contract_name(self, contract_id: int) -> str:
        """Resolve a contract ID to a name."""
        if not contract_id:
            return "N/A"
        if contract_id not in self._contract_cache:
            c = await self.get_entity("Contracts", contract_id)
            self._contract_cache[contract_id] = c or {}
        return self._contract_cache[contract_id].get("contractName", f"Contract #{contract_id}")

    async def resolve_role_name(self, role_id: int) -> str:
        """Resolve a role ID to a name."""
        if not role_id:
            return "N/A"
        if role_id not in self._role_cache:
            r = await self.get_entity("Roles", role_id)
            self._role_cache[role_id] = r or {}
        return self._role_cache[role_id].get("name", f"Role #{role_id}")

    async def resolve_resource_by_name(self, name: str) -> Optional[int]:
        """Find a resource ID by partial name match."""
        resources = await self.query_all_pages("Resources", [{
            "op": "or",
            "items": [
                {"op": "contains", "field": "firstName", "value": name},
                {"op": "contains", "field": "lastName", "value": name},
            ]
        }])
        if resources:
            # Cache them
            for r in resources:
                self._resource_cache[r["id"]] = r
            return resources[0]["id"]
        return None

    async def resolve_company_by_name(self, name: str) -> Optional[int]:
        """Find a company ID by partial name match."""
        companies = await self.query_all_pages("Companies", [
            {"op": "contains", "field": "companyName", "value": name}
        ])
        if companies:
            for c in companies:
                self._company_cache[c["id"]] = c
            return companies[0]["id"]
        return None

    # ─── ENRICHMENT (add resolved names to entities) ────────────────

    async def _prefetch_for_time_entries(self, entries: list):
        """Bulk-prefetch all entities referenced by time entries using batch 'in' queries.
        Each entity type is fetched in a single API call instead of one call per ID.
        Three passes handle the dependency chain: entries -> tasks/tickets -> projects -> companies.
        """
        resource_ids = {e.get("resourceID", 0) for e in entries} - {0}
        billing_ids = {e.get("billingCodeID", 0) for e in entries} - {0}
        role_ids = {e.get("roleID", 0) for e in entries} - {0}
        task_ids = {e.get("taskID", 0) for e in entries} - {0}
        ticket_ids = {e.get("ticketID", 0) for e in entries} - {0}
        contract_ids = {e.get("contractID", 0) for e in entries} - {0}

        # Pass 1: Fetch resources, billing codes, tasks, tickets, contracts in parallel
        #         Each is a single batch query using the 'in' operator
        async def _prefetch_tasks(tids):
            uncached = {tid for tid in tids if f"task_{tid}" not in self._project_cache}
            if not uncached:
                return
            items = await self._batch_query("Tasks", uncached)
            for item in items:
                self._project_cache[f"task_{item['id']}"] = item

        async def _prefetch_tickets(tids):
            uncached = {tid for tid in tids if f"ticket_{tid}" not in self._project_cache}
            if not uncached:
                return
            items = await self._batch_query("Tickets", uncached)
            for item in items:
                self._project_cache[f"ticket_{item['id']}"] = item

        await asyncio.gather(
            self._prefetch_ids(self._resource_cache, "Resources", resource_ids),
            self._prefetch_ids(self._billing_code_cache, "BillingCodes", billing_ids),
            self._prefetch_ids(self._role_cache, "Roles", role_ids),
            _prefetch_tasks(task_ids),
            _prefetch_tickets(ticket_ids),
            self._prefetch_ids(self._contract_cache, "Contracts", contract_ids),
        )

        # Pass 2: Collect project and company IDs referenced by tasks/tickets, then batch-fetch
        project_ids: set[int] = set()
        company_ids: set[int] = set()
        for tid in task_ids:
            task = self._project_cache.get(f"task_{tid}")
            if task and task.get("projectID"):
                project_ids.add(task["projectID"])
        for tid in ticket_ids:
            ticket = self._project_cache.get(f"ticket_{tid}")
            if ticket and ticket.get("companyID"):
                company_ids.add(ticket["companyID"])

        await self._prefetch_ids(self._project_cache, "Projects", project_ids)

        # Pass 3: Companies from projects
        for pid in project_ids:
            proj = self._project_cache.get(pid, {})
            if proj.get("companyID"):
                company_ids.add(proj["companyID"])

        await self._prefetch_ids(self._company_cache, "Companies", company_ids)

    async def enrich_time_entries(self, entries: list):
        """Add resolved names to time entry records.
        Uses bulk prefetching so most lookups hit the cache.
        """
        # Prefetch all referenced entities in parallel
        await self._prefetch_for_time_entries(entries)

        for e in entries:
            e["_resourceName"] = await self.resolve_resource_name(e.get("resourceID", 0))
            e["_workTypeName"] = await self.resolve_billing_code_name(e.get("billingCodeID", 0))
            if e.get("taskID"):
                task = self._project_cache.get(f"task_{e['taskID']}")
                if not task:
                    task = await self.get_entity("Tasks", e["taskID"])
                    if task:
                        self._project_cache[f"task_{e['taskID']}"] = task
                if task:
                    e["_taskTitle"] = task.get("title", "N/A")
                    e["_taskID"] = task.get("id", 0)
                    e["_taskRemainingHours"] = float(task.get("remainingHours") or 0)
                    e["_taskEstimatedHours"] = float(task.get("estimatedHours") or 0)
                    e["_taskAssignedResourceID"] = task.get("assignedResourceID", 0)
                    e["_projectID"] = task.get("projectID", 0)
                    if task.get("projectID"):
                        e["_projectName"] = await self.resolve_project_name(task["projectID"])
                        proj = self._project_cache.get(task["projectID"], {})
                        if proj.get("companyID"):
                            e["_companyName"] = await self.resolve_company_name(proj["companyID"])
            if e.get("ticketID") and not e.get("_companyName"):
                ticket = self._project_cache.get(f"ticket_{e['ticketID']}")
                if not ticket:
                    ticket = await self.get_entity("Tickets", e["ticketID"])
                if ticket:
                    e["_ticketTitle"] = ticket.get("title", "N/A")
                    e["_ticketNumber"] = ticket.get("ticketNumber", "N/A")
                    if ticket.get("companyID"):
                        e["_companyName"] = await self.resolve_company_name(ticket["companyID"])
            if e.get("contractID"):
                e["_contractName"] = await self.resolve_contract_name(e["contractID"])
            e["_billableLabel"] = "Non-Billable" if e.get("isNonBillable") else "Billable"

            # Cost per entry: hours × resource internal cost rate
            resource = self._resource_cache.get(e.get("resourceID", 0), {})
            cost_rate = float(resource.get("internalCost") or 0)
            hours_worked = float(e.get("hoursWorked") or 0)
            e["_costRate"] = cost_rate
            e["_cost"] = round(hours_worked * cost_rate, 2)

            # Bill rate per entry: from the Role assigned on this time entry
            role = self._role_cache.get(e.get("roleID", 0), {})
            bill_rate = float(role.get("hourlyRate") or 0)
            e["_roleName"] = role.get("name", "N/A")
            e["_billRate"] = bill_rate
            e["_billAmount"] = round(hours_worked * bill_rate, 2)

    # Fields we actually need per entity for name resolution (reduces payload size)
    _INCLUDE_FIELDS = {
        "Resources": ["id", "firstName", "lastName", "email", "isActive", "title", "internalCost"],
        "Companies": ["id", "companyName", "isActive", "phone", "address1", "address2", "city", "state", "postalCode", "webAddress"],
        "Projects": ["id", "projectName", "companyID", "status", "projectLeadResourceID", "estimatedTime", "actualHours", "actualBilledHours", "completedPercentage", "startDateTime", "endDateTime", "contractID"],
        "Tasks": ["id", "title", "projectID", "assignedResourceID", "status", "estimatedHours", "remainingHours", "startDateTime", "endDateTime"],
        "Tickets": ["id", "title", "ticketNumber", "companyID", "assignedResourceID", "status", "priority", "createDate"],
        "Contracts": ["id", "contractName", "companyID", "contractType", "status", "startDate", "endDate", "estimatedRevenue", "estimatedCost", "estimatedHours"],
        "BillingCodes": ["id", "name", "isActive", "unitPrice", "description", "billingCodeType"],
        "Roles": ["id", "name", "hourlyRate"],
    }

    # Autotask limits queries to 500 OR conditions per call;
    # the 'in' operator counts each value against this limit
    _MAX_IN_VALUES = 500

    async def _batch_query(self, entity: str, ids: set[int], include_fields: list = None) -> list:
        """Fetch multiple entities by ID in a single query using the 'in' operator.
        Uses IncludeFields to minimize payload. Automatically chunks into multiple
        queries if the ID count exceeds Autotask's 500-value limit.
        """
        if not ids:
            return []
        id_list = [eid for eid in ids if eid]
        if not id_list:
            return []
        fields = include_fields or self._INCLUDE_FIELDS.get(entity)

        if len(id_list) <= self._MAX_IN_VALUES:
            filters = [{"op": "in", "field": "id", "value": id_list}]
            return await self.query_all_pages(entity, filters, include_fields=fields)

        # Chunk into batches of 500 to stay within Autotask's OR limit
        all_items = []
        for i in range(0, len(id_list), self._MAX_IN_VALUES):
            chunk = id_list[i:i + self._MAX_IN_VALUES]
            filters = [{"op": "in", "field": "id", "value": chunk}]
            items = await self.query_all_pages(entity, filters, include_fields=fields)
            all_items.extend(items)
        return all_items

    async def _prefetch_ids(self, cache: dict, entity: str, ids: set[int]):
        """Prefetch a set of entity IDs into the cache using a single batch query.
        Turns N individual GET requests into 1 POST query with the 'in' operator.
        """
        to_fetch = {eid for eid in ids if eid and eid not in cache}
        if not to_fetch:
            return
        items = await self._batch_query(entity, to_fetch)
        for item in items:
            cache[item["id"]] = item
        # Mark IDs that weren't found so we don't re-query them
        for eid in to_fetch:
            if eid not in cache:
                cache[eid] = {}

    async def enrich_project(self, project: dict):
        """Add resolved names to a project record."""
        project["_companyName"] = await self.resolve_company_name(project.get("companyID", 0))
        project["_leadName"] = await self.resolve_resource_name(project.get("projectLeadResourceID", 0))
        status_map = {
            0: "Inactive", 1: "New", 2: "In Progress", 5: "Complete",
            8: "Contract Setup", 9: "Lost", 10: "SE", 12: "Pending Decision",
            13: "Project Setup", 14: "SE Review", 15: "Delayed Execution",
            16: "Cancelled", 18: "Ready to Start", 20: "Not Ready to Start",
        }
        project["_statusLabel"] = status_map.get(project.get("status"), str(project.get("status", "Unknown")))

    async def enrich_projects(self, projects: list):
        """Batch-enrich projects: prefetch unique IDs then apply labels."""
        company_ids = {p.get("companyID", 0) for p in projects}
        resource_ids = {p.get("projectLeadResourceID", 0) for p in projects}
        await asyncio.gather(
            self._prefetch_ids(self._company_cache, "Companies", company_ids),
            self._prefetch_ids(self._resource_cache, "Resources", resource_ids),
        )
        for p in projects:
            await self.enrich_project(p)

    async def enrich_task(self, task: dict):
        """Add resolved names to a task record."""
        task["_assignedResourceName"] = await self.resolve_resource_name(task.get("assignedResourceID", 0))
        task["_projectName"] = await self.resolve_project_name(task.get("projectID", 0))
        status_map = {
            1: "New", 5: "Complete", 7: "Waiting Customer", 8: "In Process",
            12: "Waiting Vendor", 19: "Customer Note Added", 20: "SLA Violation",
            27: "Planned", 28: "CTA Initiated", 36: "Action Required",
            37: "Not Started", 38: "Waiting TAM", 39: "Waiting ENG",
            40: "Response Requested",
        }
        task["_statusLabel"] = status_map.get(task.get("status"), str(task.get("status", "Unknown")))

    async def enrich_tasks(self, tasks: list):
        """Batch-enrich tasks: prefetch unique IDs then apply labels."""
        resource_ids = {t.get("assignedResourceID", 0) for t in tasks}
        project_ids = {t.get("projectID", 0) for t in tasks}
        await asyncio.gather(
            self._prefetch_ids(self._resource_cache, "Resources", resource_ids),
            self._prefetch_ids(self._project_cache, "Projects", project_ids),
        )
        for t in tasks:
            await self.enrich_task(t)

    async def enrich_ticket(self, ticket: dict):
        """Add resolved names to a ticket record."""
        ticket["_companyName"] = await self.resolve_company_name(ticket.get("companyID", 0))
        ticket["_assignedResourceName"] = await self.resolve_resource_name(ticket.get("assignedResourceID", 0))
        status_map = {
            1: "New", 5: "Complete", 7: "Waiting Customer", 8: "In Process",
            12: "Waiting Vendor", 19: "Customer Note Added", 20: "SLA Violation",
            27: "Planned", 28: "CTA Initiated", 36: "Action Required",
            37: "Not Started", 38: "Waiting TAM", 39: "Waiting ENG",
            40: "Response Requested",
        }
        ticket["_statusLabel"] = status_map.get(ticket.get("status"), str(ticket.get("status", "Unknown")))

    async def enrich_tickets(self, tickets: list):
        """Batch-enrich tickets: prefetch unique IDs then apply labels."""
        company_ids = {t.get("companyID", 0) for t in tickets}
        resource_ids = {t.get("assignedResourceID", 0) for t in tickets}
        await asyncio.gather(
            self._prefetch_ids(self._company_cache, "Companies", company_ids),
            self._prefetch_ids(self._resource_cache, "Resources", resource_ids),
        )
        for t in tickets:
            await self.enrich_ticket(t)

    async def enrich_contract(self, contract: dict):
        """Add resolved names to a contract record."""
        contract["_companyName"] = await self.resolve_company_name(contract.get("companyID", 0))
        type_map = {
            1: "Time & Materials", 3: "Fixed Price", 4: "Block Hours",
            6: "Retainer", 7: "Recurring Service", 8: "Per Ticket", 9: "Umbrella",
        }
        contract["_typeLabel"] = type_map.get(contract.get("contractType"), str(contract.get("contractType", "Unknown")))
        status_map = {0: "Inactive", 1: "Active"}
        contract["_statusLabel"] = status_map.get(contract.get("status"), str(contract.get("status", "Unknown")))

    async def enrich_contracts(self, contracts: list):
        """Batch-enrich contracts: prefetch unique IDs then apply labels."""
        company_ids = {c.get("companyID", 0) for c in contracts}
        await self._prefetch_ids(self._company_cache, "Companies", company_ids)
        for c in contracts:
            await self.enrich_contract(c)

    # ─── COMPOUND QUERIES ───────────────────────────────────────────

    async def get_contract_costs(self, contract_id: int) -> dict:
        """Get aggregate cost metrics for all time entries on a contract.
        Queries by contractID directly (single API call + pagination),
        then uses resource internalCost for cost calculation.
        Returns dict with total_hours, total_cost, and blended_cost_rate.
        """
        entries = await self.query_all_pages("TimeEntries", [
            {"op": "eq", "field": "contractID", "value": contract_id}
        ])
        if not entries:
            return {"total_hours": 0.0, "total_cost": 0.0, "blended_cost_rate": 0.0}

        # Only need resource internalCost — lightweight prefetch
        resource_ids = {e.get("resourceID", 0) for e in entries} - {0}
        await self._prefetch_ids(self._resource_cache, "Resources", resource_ids)

        total_hours = 0.0
        total_cost = 0.0
        for e in entries:
            hours = float(e.get("hoursWorked") or 0)
            resource = self._resource_cache.get(e.get("resourceID", 0), {})
            cost_rate = float(resource.get("internalCost") or 0)
            total_hours += hours
            total_cost += hours * cost_rate

        blended_rate = total_cost / total_hours if total_hours > 0 else 0.0
        return {
            "total_hours": round(total_hours, 2),
            "total_cost": round(total_cost, 2),
            "blended_cost_rate": round(blended_rate, 2),
        }

    async def _batch_time_entries(self, field: str, ids: list[int], extra_filters: list = None, paging_status: Optional[dict] = None) -> list:
        """Fetch time entries matching any of the given IDs on *field* using
        the 'in' operator.  Chunks into batches of 500 to respect Autotask's
        OR-limit, and combines any additional filters (date ranges, etc.).
        """
        if not ids:
            return []
        sanitized = [f for f in (extra_filters or []) if f.get("field") != field]
        all_entries: list[dict] = []
        for i in range(0, len(ids), self._MAX_IN_VALUES):
            chunk = ids[i:i + self._MAX_IN_VALUES]
            filters = [{"op": "in", "field": field, "value": chunk}]
            filters.extend(sanitized)
            chunk_ps: dict = {}
            entries = await self.query_all_pages("TimeEntries", filters, paging_status=chunk_ps)
            if paging_status is not None:
                merge_paging_status(paging_status, chunk_ps)
            all_entries.extend(entries)
        return all_entries

    async def get_time_entries_for_company(self, company_id: int, extra_filters: list = None, paging_status: Optional[dict] = None) -> list:
        """Get all time entries for a company by finding its projects and tickets.
        Uses batch 'in' queries instead of per-entity fan-out.
        """
        # Step 1: Fetch projects and tickets for this company
        proj_ps: dict = {}
        tick_ps: dict = {}
        projects, tickets = await asyncio.gather(
            self.query_all_pages("Projects", [
                {"op": "eq", "field": "companyID", "value": company_id}
            ], paging_status=proj_ps),
            self.query_all_pages("Tickets", [
                {"op": "eq", "field": "companyID", "value": company_id}
            ], paging_status=tick_ps),
        )
        if paging_status is not None:
            merge_paging_status(paging_status, proj_ps)
            merge_paging_status(paging_status, tick_ps)

        for proj in projects:
            self._project_cache[proj["id"]] = proj

        # Step 2: Batch-fetch tasks for all projects using 'in'
        project_ids = [p["id"] for p in projects if p.get("id")]
        all_tasks: list[dict] = []
        for i in range(0, len(project_ids), self._MAX_IN_VALUES):
            chunk = project_ids[i:i + self._MAX_IN_VALUES]
            task_ps: dict = {}
            tasks = await self.query_all_pages("Tasks", [
                {"op": "in", "field": "projectID", "value": chunk}
            ], paging_status=task_ps)
            if paging_status is not None:
                merge_paging_status(paging_status, task_ps)
            all_tasks.extend(tasks)

        for task in all_tasks:
            self._project_cache[f"task_{task['id']}"] = task

        # Step 3: Batch-fetch time entries for all tasks + all tickets
        task_ids = [t["id"] for t in all_tasks if t.get("id")]
        ticket_ids = [t["id"] for t in tickets if t.get("id")]

        te_ps_tasks: dict = {}
        te_ps_tickets: dict = {}
        te_by_tasks, te_by_tickets = await asyncio.gather(
            self._batch_time_entries("taskID", task_ids, extra_filters, paging_status=te_ps_tasks),
            self._batch_time_entries("ticketID", ticket_ids, extra_filters, paging_status=te_ps_tickets),
        )
        if paging_status is not None:
            merge_paging_status(paging_status, te_ps_tasks)
            merge_paging_status(paging_status, te_ps_tickets)

        # Deduplicate by ID
        seen: dict[int, dict] = {}
        for e in te_by_tasks + te_by_tickets:
            seen.setdefault(e["id"], e)
        return list(seen.values())

    async def get_time_entries_for_project(self, project_id: int, extra_filters: list = None, paging_status: Optional[dict] = None) -> list:
        """Get all time entries for a project by finding its tasks.
        Uses a single batch 'in' query instead of per-task fan-out.
        """
        task_ps: dict = {}
        tasks = await self.query_all_pages("Tasks", [
            {"op": "eq", "field": "projectID", "value": project_id}
        ], paging_status=task_ps)
        if paging_status is not None:
            merge_paging_status(paging_status, task_ps)
        for task in tasks:
            self._project_cache[f"task_{task['id']}"] = task

        task_ids = [t["id"] for t in tasks if t.get("id")]
        return await self._batch_time_entries("taskID", task_ids, extra_filters, paging_status=paging_status)
