# Datto PSA / Autotask MCP Server
# Provides LLM-friendly analysis of time entries, projects, tasks, tickets, and contracts
# with automatic pagination, ID-to-name resolution, and hours aggregation.

import asyncio
import os
import json
from mcp.server.fastmcp import FastMCP
from api_client import AutotaskClient
from formatters import (
    format_time_entries_summary,
    format_project_summary,
    format_task_summary,
    format_ticket_summary,
    format_contract_summary,
    format_company_summary,
    format_resource_summary,
    format_entity_list,
)

mcp = FastMCP(
    "datto_psa",
    host="0.0.0.0",
    stateless_http=True,
    instructions="""Datto PSA / Autotask — time tracking, project management, and financial analysis.

IMPORTANT — USERS THINK IN PROJECTS, NOT CONTRACTS:
Users will ask about "projects" when they want financials (margin, budget, cost, profitability).
Behind the scenes, revenue/budget data lives on the contract linked to a project — but users
don't know or care about that. Always use get_project_summary as the primary tool for project
financial questions. It automatically fetches the linked contract and computes margin.
Only use get_contract_summary when the user explicitly asks about contracts, or when you need
a company-wide financial view across all contracts.
In your answers, say "project budget" or "project revenue" — not "contract value".

TOOL ROUTING GUIDE — pick the right tool for the question:

| Question type | Tool to use |
|---|---|
| Hours worked, time logged, who worked on what, cost by resource/date/project | `query_time_entries` |
| Quick utilization or hours summary grouped by resource | `analyze_hours` |
| Project status, budget, margin, GM, "how's [project] going?", resource allocation, remaining hours per person | `get_project_summary` |
| "What's our margin on [project/client]?", profitability | `get_project_summary` (single project) or `get_contract_summary` (all projects for a client) |
| Dashboard JSON for a single project (budget, hours, resources) | `get_project_dashboard` |
| Actual hours/cost/bill by resource and month for a project date range | `get_project_actuals` |
| Find/list projects by company, status, or name | `search_projects` |
| Task-level detail with time entries | `get_task_details` |
| Find/list tickets, ticket status, time on tickets | `search_tickets` |
| Company-wide financial overview across all projects | `get_contract_summary` |
| Company overview: contacts, projects | `get_company_info` |
| Find employees/technicians by name | `search_resources` |
| Billing codes and work types reference | `get_work_types` |

COMMON MULTI-STEP PATTERNS:
- "What's our margin on [project]?" → get_project_summary(project_name="...")
- "How are we doing on [project]?" → get_project_summary(project_name="...")
- "Are we profitable on [client]?" → get_contract_summary(company_name="...") for the full picture
- "What did [person] work on last week?" → query_time_entries(resource_name="...", date_from="...", date_to="...", group_by="project", include_details=True)
- "Show me all active projects for [client]" → search_projects(company_name="...", status="active")
- "Get dashboard data for project 1234" → get_project_dashboard(project_id=1234) + get_project_actuals(project_id=1234, date_from="...", date_to="...")
- "Who logged the most hours this month?" → query_time_entries(date_from="...", date_to="...", group_by="resource")

FINANCIAL CONTEXT:
- All projects are fixed-fee. Each project has a total budget (stored on its linked contract).
- Cost = hours worked × resource internal cost rate (automatically computed per time entry).
- Gross margin = project budget − cost. Target GM defaults to 60% (adjustable via target_gm_pct).
- Cost budget = project budget × (1 − target GM%). Budget consumed is measured against this.
- get_project_summary automatically resolves the linked contract — no extra steps needed.
""",
)


def _compute_financials(contract: dict, actual_hours: float, actual_cost: float,
                        remaining_hours: float = 0, target_gm_pct: float = 60.0) -> dict:
    """Compute gross margin metrics from contract value and accumulated costs.

    Returns a dict with projected GM, current (prorated) GM, and budget consumption.
    Projected GM uses the blended cost rate to estimate cost for remaining hours.
    Current GM prorates the contract value linearly from start to end date.
    Budget consumed is measured against the cost budget derived from the target
    (sold-at) gross margin percentage.
    """
    from datetime import datetime, date

    contract_amount = float(contract.get("estimatedRevenue") or 0)
    start_str = contract.get("startDate", "")
    end_str = contract.get("endDate", "")

    # Cost budget = the portion of contract value allocated to delivery cost
    cost_budget = contract_amount * (1 - target_gm_pct / 100) if contract_amount else 0

    blended_cost_rate = actual_cost / actual_hours if actual_hours > 0 else 0
    projected_total_cost = actual_cost + (remaining_hours * blended_cost_rate)
    projected_gm = contract_amount - projected_total_cost if contract_amount else None
    projected_gm_pct = (projected_gm / contract_amount * 100) if contract_amount and projected_gm is not None else None

    # Budget consumed against cost budget, not total contract value
    budget_consumed_pct = (actual_cost / cost_budget * 100) if cost_budget > 0 else None

    # Prorate contract value to today based on elapsed contract duration
    prorated_revenue = contract_amount
    prorate_factor = 1.0
    if start_str and end_str and contract_amount:
        try:
            start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00")).date()
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00")).date()
            today = date.today()
            total_days = (end_dt - start_dt).days
            elapsed_days = max(0, min((today - start_dt).days, total_days))
            if total_days > 0:
                prorate_factor = elapsed_days / total_days
                prorated_revenue = round(contract_amount * prorate_factor, 2)
        except (ValueError, TypeError):
            pass

    # Prorated cost budget = what we should have spent by now at target GM
    prorated_cost_budget = round(cost_budget * prorate_factor, 2) if cost_budget else 0

    current_gm = prorated_revenue - actual_cost if prorated_revenue else None
    current_gm_pct = (current_gm / prorated_revenue * 100) if prorated_revenue and current_gm is not None else None

    return {
        "contract_amount": contract_amount,
        "contract_start": start_str,
        "contract_end": end_str,
        "target_gm_pct": target_gm_pct,
        "cost_budget": round(cost_budget, 2),
        "actual_hours": round(actual_hours, 2),
        "actual_cost": round(actual_cost, 2),
        "remaining_hours": round(remaining_hours, 2),
        "blended_cost_rate": round(blended_cost_rate, 2),
        "projected_total_cost": round(projected_total_cost, 2),
        "projected_gm": round(projected_gm, 2) if projected_gm is not None else None,
        "projected_gm_pct": round(projected_gm_pct, 1) if projected_gm_pct is not None else None,
        "prorated_revenue": round(prorated_revenue, 2),
        "prorated_cost_budget": prorated_cost_budget,
        "current_gm": round(current_gm, 2) if current_gm is not None else None,
        "current_gm_pct": round(current_gm_pct, 1) if current_gm_pct is not None else None,
        "budget_consumed_pct": round(budget_consumed_pct, 1) if budget_consumed_pct is not None else None,
    }


def _get_client() -> AutotaskClient:
    """Create an AutotaskClient from environment variables."""
    username = os.getenv("AUTOTASK_USERNAME", "")
    secret = os.getenv("AUTOTASK_SECRET", "")
    integration_code = os.getenv("AUTOTASK_INTEGRATION_CODE", "")
    api_url = os.getenv("AUTOTASK_API_URL", "")
    if not all([username, secret, integration_code]):
        raise ValueError(
            "Missing required environment variables: AUTOTASK_USERNAME, AUTOTASK_SECRET, AUTOTASK_INTEGRATION_CODE. "
            "Optionally set AUTOTASK_API_URL (e.g. https://webservices5.autotask.net/atservicesrest)."
        )
    return AutotaskClient(username, secret, integration_code, api_url)


# ─── TIME ENTRY TOOLS ───────────────────────────────────────────────

@mcp.tool()
async def query_time_entries(
    date_from: str = "",
    date_to: str = "",
    resource_name: str = "",
    resource_id: int = 0,
    company_name: str = "",
    company_id: int = 0,
    project_id: int = 0,
    task_id: int = 0,
    ticket_id: int = 0,
    contract_id: int = 0,
    billable_only: bool = False,
    non_billable_only: bool = False,
    group_by: str = "",
    include_details: bool = False,
) -> str:
    """
    Query and analyze time entries — the main tool for hours, cost, and utilization questions.

    USE THIS WHEN asked about: hours worked, time logged, who worked on what,
    billable vs non-billable breakdown, cost by resource/date/project, utilization.

    Examples: "What did John work on last week?", "How many billable hours for Acme Corp?",
    "Show me time entries grouped by project for March", "What's our cost on this contract?"

    Returns summary with total hours, billable/non-billable split, total cost, blended cost rate,
    and optional group-by breakdown (each group includes hours AND cost). Use group_by to slice
    data by resource, company, project, task, ticket, date, or work_type.

    Args:
        date_from: Start date (YYYY-MM-DD). Defaults to last 7 days if omitted.
        date_to: End date (YYYY-MM-DD). Defaults to today.
        resource_name: Filter by employee name (partial match, e.g. "John").
        resource_id: Filter by exact resource ID (use search_resources to find IDs).
        company_name: Filter by client name (partial match, e.g. "Smith Allergy").
        company_id: Filter by exact company ID.
        project_id: Filter by project ID.
        task_id: Filter by task ID.
        ticket_id: Filter by ticket ID.
        contract_id: Filter by contract ID.
        billable_only: Only billable time entries.
        non_billable_only: Only non-billable time entries.
        group_by: Slice results by: 'resource', 'company', 'project', 'task', 'ticket', 'date', 'work_type'. Leave empty for overall totals.
        include_details: Include individual time entry lines (verbose — use for "show me every entry" questions).
    """
    try:
        async with _get_client() as client:
            filters = []
            paging_status: dict = {}

            # Date filters
            if date_from:
                filters.append({"op": "gte", "field": "dateWorked", "value": f"{date_from}T00:00:00"})
            if date_to:
                filters.append({"op": "lte", "field": "dateWorked", "value": f"{date_to}T23:59:59"})
            if not date_from and not date_to:
                from datetime import datetime, timedelta
                end = datetime.utcnow()
                start = end - timedelta(days=7)
                filters.append({"op": "gte", "field": "dateWorked", "value": start.strftime("%Y-%m-%dT00:00:00")})
                filters.append({"op": "lte", "field": "dateWorked", "value": end.strftime("%Y-%m-%dT23:59:59")})

            # Resource filter
            if resource_name:
                rid = await client.resolve_resource_by_name(resource_name)
                if rid:
                    filters.append({"op": "eq", "field": "resourceID", "value": rid})
                else:
                    return f"Could not find a resource matching '{resource_name}'. Try a different name or use resource_id."
            elif resource_id:
                filters.append({"op": "eq", "field": "resourceID", "value": resource_id})

            # Company filter - need to find tasks/tickets for that company
            resolved_company_id = company_id
            if company_name and not company_id:
                cid = await client.resolve_company_by_name(company_name)
                if cid:
                    resolved_company_id = cid
                else:
                    return f"Could not find a company matching '{company_name}'. Try a different name or use company_id."

            # Direct entity filters
            if project_id:
                filters.append({"op": "eq", "field": "taskID", "value": project_id})  # Will be handled via task lookup
            if task_id:
                filters.append({"op": "eq", "field": "taskID", "value": task_id})
            if ticket_id:
                filters.append({"op": "eq", "field": "ticketID", "value": ticket_id})
            if contract_id:
                filters.append({"op": "eq", "field": "contractID", "value": contract_id})
            if billable_only:
                filters.append({"op": "eq", "field": "isNonBillable", "value": False})
            if non_billable_only:
                filters.append({"op": "eq", "field": "isNonBillable", "value": True})

            # If filtering by company, we need to get time entries via project tasks and tickets
            if resolved_company_id and not project_id and not task_id and not ticket_id:
                entries = await client.get_time_entries_for_company(resolved_company_id, filters, paging_status=paging_status)
            elif project_id and not task_id:
                # Get all tasks for the project, then get time entries
                entries = await client.get_time_entries_for_project(project_id, filters, paging_status=paging_status)
            else:
                if not filters:
                    filters.append({"op": "exist", "field": "id"})
                entries = await client.query_all_pages("TimeEntries", filters, paging_status=paging_status)

            paging_status["items_returned"] = len(entries)

            # Resolve names for display
            await client.enrich_time_entries(entries)

            return format_time_entries_summary(
                entries, group_by=group_by, include_details=include_details, paging_status=paging_status
            )
    except Exception as e:
        return f"Error querying time entries: {str(e)}"


# ─── PROJECT TOOLS ──────────────────────────────────────────────────

@mcp.tool()
async def get_project_summary(
    project_id: int = 0,
    project_name: str = "",
    include_tasks: bool = True,
    include_time_entries: bool = False,
    target_gm_pct: float = 60.0,
) -> str:
    """
    THE PRIMARY TOOL for project status, budget, and profitability.
    Automatically resolves the project's budget and computes gross margin — no need
    to look up contracts separately.

    USE THIS WHEN asked about: project status, project budget, margin/GM, profitability,
    "how are we doing on [project]?", "what's our margin?", task breakdown, hours vs estimate.

    Examples: "How's the Smith migration going?", "What's the margin on the network upgrade?",
    "Are we on budget for project X?", "Show me task breakdown for the Acme project"

    Returns: project details, hours vs estimate, budget and GM analysis (projected GM,
    current GM, cost budget consumed), per-resource allocation (remaining hours, actual
    hours, and cost per person), and task breakdown. Financial metrics are computed
    automatically from the project's linked budget and all time entries.

    Args:
        project_id: Autotask project ID. Use search_projects to find IDs.
        project_name: Search by name (partial match). If multiple match, returns a list to pick from.
        include_tasks: Show task-level breakdown (default True).
        include_time_entries: Show individual time entry lines per task (verbose).
        target_gm_pct: Sold-at gross margin % for cost budget calculation. Default 60%.
    """
    try:
        async with _get_client() as client:
            if not project_id and project_name:
                projects = await client.query_all_pages("Projects", [
                    {"op": "contains", "field": "projectName", "value": project_name}
                ])
                if not projects:
                    return f"No projects found matching '{project_name}'."
                if len(projects) > 1:
                    await client.enrich_projects(projects[:20])
                    lines = [f"Multiple projects found matching '{project_name}':"]
                    for p in projects[:20]:
                        lines.append(f"  - ID {p['id']}: {p.get('projectName', 'N/A')} (Status: {p.get('_statusLabel', 'N/A')})")
                    lines.append("\nPlease specify a project_id to get details.")
                    return "\n".join(lines)
                project_id = projects[0]["id"]

            # Get project
            project = await client.get_entity("Projects", project_id)
            if not project:
                return f"Project ID {project_id} not found."

            # Resolve company name
            await client.enrich_project(project)

            # Fetch the linked contract for financial analysis
            contract = None
            sibling_project_count = 0
            if project.get("contractID"):
                contract = await client.get_entity("Contracts", project["contractID"])
                if contract:
                    await client.enrich_contract(contract)
                    # Check how many projects share this contract
                    sibling_projects = await client.query_all_pages("Projects", [
                        {"op": "eq", "field": "contractID", "value": project["contractID"]}
                    ], include_fields=["id"])
                    sibling_project_count = len(sibling_projects)

            # Always fetch tasks (needed for remaining hours in GM calculation)
            tasks = await client.query_all_pages("Tasks", [
                {"op": "eq", "field": "projectID", "value": project_id}
            ])
            await client.enrich_tasks(tasks)

            # Always fetch time entries for cost analysis
            time_entries = await client.get_time_entries_for_project(project_id)
            await client.enrich_time_entries(time_entries)

            # Assign time entries to their tasks
            te_by_task: dict[int, list] = {}
            for te in time_entries:
                te_by_task.setdefault(te.get("taskID", 0), []).append(te)
            for task in tasks:
                task["_time_entries"] = te_by_task.get(task["id"], [])

            # Compute financial metrics if contract exists
            financials = None
            if contract:
                actual_hours = sum(float(te.get("hoursWorked") or 0) for te in time_entries)
                actual_cost = sum(float(te.get("_cost") or 0) for te in time_entries)
                remaining_hours = sum(float(t.get("remainingHours") or 0) for t in tasks)
                financials = _compute_financials(contract, actual_hours, actual_cost, remaining_hours, target_gm_pct)
                financials["sibling_project_count"] = sibling_project_count

            # Per-resource allocation rollup:
            # - Forward estimate (remaining_hours) from task primary assignments
            # - Actual hours/cost from time entries (captures everyone who logged time)
            resource_alloc: dict[int, dict] = {}
            for t in tasks:
                rid = t.get("assignedResourceID", 0)
                if not rid:
                    continue
                if rid not in resource_alloc:
                    resource_alloc[rid] = {
                        "resourceID": rid,
                        "resourceName": t.get("_assignedResourceName", f"Resource #{rid}"),
                        "tasks_assigned": 0,
                        "estimated_hours": 0.0,
                        "remaining_hours": 0.0,
                        "actual_hours": 0.0,
                        "actual_cost": 0.0,
                    }
                resource_alloc[rid]["tasks_assigned"] += 1
                resource_alloc[rid]["estimated_hours"] += float(t.get("estimatedHours") or 0)
                resource_alloc[rid]["remaining_hours"] += float(t.get("remainingHours") or 0)

            for te in time_entries:
                rid = te.get("resourceID", 0)
                if not rid:
                    continue
                if rid not in resource_alloc:
                    resource_alloc[rid] = {
                        "resourceID": rid,
                        "resourceName": te.get("_resourceName", f"Resource #{rid}"),
                        "tasks_assigned": 0,
                        "estimated_hours": 0.0,
                        "remaining_hours": 0.0,
                        "actual_hours": 0.0,
                        "actual_cost": 0.0,
                    }
                resource_alloc[rid]["actual_hours"] += float(te.get("hoursWorked") or 0)
                resource_alloc[rid]["actual_cost"] += float(te.get("_cost") or 0)

            # Sort by remaining hours descending (who has the most work ahead)
            resource_allocations = sorted(
                resource_alloc.values(),
                key=lambda r: r["remaining_hours"],
                reverse=True,
            )

            return format_project_summary(
                project, tasks, time_entries, include_tasks, include_time_entries,
                contract=contract, financials=financials,
                resource_allocations=resource_allocations,
            )
    except Exception as e:
        return f"Error getting project summary: {str(e)}"


@mcp.tool()
async def get_project_dashboard(
    project_id: int = 0,
    project_name: str = "",
    target_gm_pct: float = 60.0,
) -> str:
    """
    Dashboard-ready JSON snapshot for a single project.

    Returns a structured JSON object with project metadata, budget, hours,
    and a per-resource breakdown — designed to feed directly into a dashboard UI.

    USE THIS WHEN building or populating a project dashboard, or when the caller
    needs machine-readable project data rather than a narrative summary.

    The response includes:
    - project_id, project_name, status, company, period dates
    - budget, target_gm_pct, cost_budget, actual_cost, projected_gm, projected_gm_pct
    - hours_estimated, hours_actual, hours_remaining
    - resources[]: name, role, hours_estimated, hours_actual, hours_remaining, cost_actual

    Args:
        project_id: Autotask project ID. Use search_projects to find IDs.
        project_name: Search by name (partial match). If multiple match, returns a list to pick from.
        target_gm_pct: Sold-at gross margin % for cost budget calculation. Default 60%.
    """
    try:
        async with _get_client() as client:
            # Resolve project by name if needed
            if not project_id and project_name:
                projects = await client.query_all_pages("Projects", [
                    {"op": "contains", "field": "projectName", "value": project_name}
                ])
                if not projects:
                    return json.dumps({"error": f"No projects found matching '{project_name}'."})
                if len(projects) > 1:
                    matches = [{"project_id": p["id"], "project_name": p.get("projectName", "N/A")} for p in projects[:20]]
                    return json.dumps({"error": "Multiple projects matched", "matches": matches})
                project_id = projects[0]["id"]

            if not project_id:
                return json.dumps({"error": "Provide project_id or project_name."})

            project = await client.get_entity("Projects", project_id)
            if not project:
                return json.dumps({"error": f"Project ID {project_id} not found."})

            await client.enrich_project(project)

            # Step 1: Fetch tasks + contract metadata in parallel.
            # All queries here are single API calls — no fan-out.
            contract = None
            contract_rates_by_role: dict[int, float] = {}
            contract_id = project.get("contractID", 0)

            fetch_coros = [
                client.query_all_pages("Tasks", [
                    {"op": "eq", "field": "projectID", "value": project_id}
                ]),
            ]
            if contract_id:
                fetch_coros.extend([
                    client.get_entity("Contracts", contract_id),
                    client.query_all_pages("ContractRates", [
                        {"op": "eq", "field": "contractID", "value": contract_id}
                    ]),
                    client.query_all_pages("TimeEntries", [
                        {"op": "eq", "field": "contractID", "value": contract_id}
                    ]),
                ])

            results = await asyncio.gather(*fetch_coros)
            tasks = results[0]
            te_by_contract: list[dict] = []
            if contract_id:
                contract = results[1]
                contract_rates_raw = results[2]
                te_by_contract = results[3]
                if contract:
                    await client.enrich_contract(contract)
                for cr in contract_rates_raw:
                    role_id = cr.get("roleID", 0)
                    if role_id:
                        contract_rates_by_role[role_id] = float(cr.get("contractHourlyRate") or 0)

            # Step 2: Batch-fetch task-based time entries using the 'in'
            # operator (1 API call) instead of N per-task queries.  This
            # catches unposted entries where contractID is still null.
            task_ids = [t["id"] for t in tasks if t.get("id")]
            te_by_tasks: list[dict] = []
            if task_ids:
                te_by_tasks = await client.query_all_pages("TimeEntries", [
                    {"op": "in", "field": "taskID", "value": task_ids}
                ])

            # Merge both sets by entry ID — no double-counting
            merged: dict[int, dict] = {e["id"]: e for e in te_by_contract}
            for e in te_by_tasks:
                merged.setdefault(e["id"], e)
            time_entries = list(merged.values())

            await asyncio.gather(
                client.enrich_tasks(tasks),
                client.enrich_time_entries(time_entries),
            )

            budget = float(contract.get("estimatedRevenue") or 0) if contract else 0.0
            period_start = _format_date_iso(contract.get("startDate", "")) if contract else _format_date_iso(project.get("startDateTime", ""))
            period_end = _format_date_iso(contract.get("endDate", "")) if contract else _format_date_iso(project.get("endDateTime", ""))

            hours_estimated = sum(float(t.get("estimatedHours") or 0) for t in tasks)
            hours_actual = sum(float(te.get("hoursWorked") or 0) for te in time_entries)
            hours_remaining = sum(float(t.get("remainingHours") or 0) for t in tasks)
            actual_cost = sum(float(te.get("_cost") or 0) for te in time_entries)

            # Financials
            cost_budget = budget * (1 - target_gm_pct / 100) if budget else 0.0
            blended_cost_rate = actual_cost / hours_actual if hours_actual > 0 else 0.0
            projected_total_cost = actual_cost + (hours_remaining * blended_cost_rate)
            projected_gm = budget - projected_total_cost if budget else None
            projected_gm_pct = (projected_gm / budget * 100) if budget and projected_gm is not None else None
            budget_consumed_pct = (actual_cost / cost_budget * 100) if cost_budget > 0 else None

            # Per-resource rollup.
            # Bill rate priority: ContractRate for the resource's role on this
            # project > standard Role hourlyRate > 0.
            resource_map: dict[int, dict] = {}

            def _init_resource(rid: int, name: str) -> dict:
                resource = client._resource_cache.get(rid, {})
                return {
                    "name": name,
                    "role": resource.get("title", "N/A"),
                    "hours_estimated": 0.0,
                    "hours_actual": 0.0,
                    "hours_remaining": 0.0,
                    "cost_actual": 0.0,
                    "_role_id": 0,
                }

            for t in tasks:
                rid = t.get("assignedResourceID", 0)
                if not rid:
                    continue
                if rid not in resource_map:
                    resource_map[rid] = _init_resource(rid, t.get("_assignedResourceName", f"Resource #{rid}"))
                resource_map[rid]["hours_estimated"] += float(t.get("estimatedHours") or 0)
                resource_map[rid]["hours_remaining"] += float(t.get("remainingHours") or 0)

            for te in time_entries:
                rid = te.get("resourceID", 0)
                if not rid:
                    continue
                if rid not in resource_map:
                    resource_map[rid] = _init_resource(rid, te.get("_resourceName", f"Resource #{rid}"))
                resource_map[rid]["hours_actual"] += float(te.get("hoursWorked") or 0)
                resource_map[rid]["cost_actual"] += float(te.get("_cost") or 0)
                # Capture the role ID from time entries (last one wins — typically
                # consistent per resource on a project)
                if te.get("roleID"):
                    resource_map[rid]["_role_id"] = te["roleID"]

            # Resolve bill rate and role name from ContractRates → Role fallback
            resources = []
            for r in sorted(resource_map.values(), key=lambda x: x["hours_actual"], reverse=True):
                role_id = r["_role_id"]
                # Prefer the contract-specific rate; fall back to standard role rate
                if role_id and role_id in contract_rates_by_role:
                    bill_rate = contract_rates_by_role[role_id]
                elif role_id:
                    role = client._role_cache.get(role_id, {})
                    bill_rate = float(role.get("hourlyRate") or 0)
                else:
                    bill_rate = 0.0
                # Use the role name from the Roles entity when available
                role_name = r["role"]
                if role_id:
                    role_entity = client._role_cache.get(role_id, {})
                    if role_entity.get("name"):
                        role_name = role_entity["name"]
                resources.append({
                    "name": r["name"],
                    "role": role_name,
                    "bill_rate": round(bill_rate, 2),
                    "hours_estimated": round(r["hours_estimated"], 2),
                    "hours_actual": round(r["hours_actual"], 2),
                    "hours_remaining": round(r["hours_remaining"], 2),
                    "cost_actual": round(r["cost_actual"], 2),
                })

            dashboard = {
                "project_id": project_id,
                "project_name": project.get("projectName", "N/A"),
                "company": project.get("_companyName", "N/A"),
                "status": project.get("_statusLabel", "Unknown"),
                "lead": project.get("_leadName", "N/A"),
                "period_start": period_start,
                "period_end": period_end,
                "budget": round(budget, 2),
                "target_gm_pct": target_gm_pct,
                "cost_budget": round(cost_budget, 2),
                "actual_cost": round(actual_cost, 2),
                "blended_cost_rate": round(blended_cost_rate, 2),
                "projected_total_cost": round(projected_total_cost, 2),
                "projected_gm": round(projected_gm, 2) if projected_gm is not None else None,
                "projected_gm_pct": round(projected_gm_pct, 1) if projected_gm_pct is not None else None,
                "budget_consumed_pct": round(budget_consumed_pct, 1) if budget_consumed_pct is not None else None,
                "hours_estimated": round(hours_estimated, 2),
                "hours_actual": round(hours_actual, 2),
                "hours_remaining": round(hours_remaining, 2),
                "resources": resources,
            }

            return json.dumps(dashboard, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Error building project dashboard: {str(e)}"})


def _format_date_iso(dt_str: str) -> str:
    """Extract YYYY-MM-DD from an ISO datetime string."""
    if not dt_str:
        return ""
    return dt_str[:10] if "T" in str(dt_str) else str(dt_str)[:10]


@mcp.tool()
async def get_project_actuals(
    project_id: int = 0,
    date_from: str = "",
    date_to: str = "",
) -> str:
    """
    Pre-aggregated time entry actuals for a project dashboard — JSON only.

    Returns hours, cost, and billing totals broken down by resource and by month.
    Designed to pair with get_project_dashboard: use that tool for budget/GM/estimates,
    and this tool for actual time entry data within a date range.

    Bill amounts use the project's ContractRates (the Rates tab in the UI), not
    the standard Role rate — so bill_rate here will match get_project_dashboard.

    USE THIS WHEN the dashboard needs actual hours/cost/bill data for a date range,
    resource-level cost and bill rates, or monthly cost/hours trends.

    Args:
        project_id: Autotask project ID.
        date_from: Start of time entry range (YYYY-MM-DD). Required.
        date_to: End of time entry range (YYYY-MM-DD). Required.
    """
    if not project_id:
        return json.dumps({"error": "project_id is required."})
    if not date_from or not date_to:
        return json.dumps({"error": "Both date_from and date_to are required."})

    try:
        async with _get_client() as client:
            project = await client.get_entity("Projects", project_id)
            if not project:
                return json.dumps({"error": f"Project ID {project_id} not found."})

            contract_id = project.get("contractID", 0)

            # Fetch tasks, contract-based entries, and ContractRates in parallel.
            # Task IDs are needed to catch unposted entries (contractID is null).
            fetch_coros = [
                client.query_all_pages("Tasks", [
                    {"op": "eq", "field": "projectID", "value": project_id}
                ], include_fields=["id"]),
            ]
            if contract_id:
                fetch_coros.extend([
                    client.query_all_pages("ContractRates", [
                        {"op": "eq", "field": "contractID", "value": contract_id}
                    ]),
                    client.query_all_pages("TimeEntries", [
                        {"op": "eq", "field": "contractID", "value": contract_id},
                        {"op": "gte", "field": "dateWorked", "value": f"{date_from}T00:00:00"},
                        {"op": "lte", "field": "dateWorked", "value": f"{date_to}T23:59:59"},
                    ]),
                ])

            results = await asyncio.gather(*fetch_coros)
            tasks = results[0]
            contract_rates_raw: list[dict] = []
            te_by_contract: list[dict] = []
            if contract_id:
                contract_rates_raw = results[1]
                te_by_contract = results[2]

            # Build ContractRate lookup: roleID → contractHourlyRate
            contract_bill_rates: dict[int, float] = {}
            for cr in contract_rates_raw:
                role_id = cr.get("roleID", 0)
                if role_id:
                    contract_bill_rates[role_id] = float(cr.get("contractHourlyRate") or 0)

            # Batch-fetch task-based entries (catches unposted entries)
            task_ids = [t["id"] for t in tasks if t.get("id")]
            date_filters = [
                {"op": "gte", "field": "dateWorked", "value": f"{date_from}T00:00:00"},
                {"op": "lte", "field": "dateWorked", "value": f"{date_to}T23:59:59"},
            ]
            te_by_tasks = await client._batch_time_entries("taskID", task_ids, date_filters) if task_ids else []

            # Merge by entry ID
            merged: dict[int, dict] = {e["id"]: e for e in te_by_contract}
            for e in te_by_tasks:
                merged.setdefault(e["id"], e)
            time_entries = list(merged.values())

            if not time_entries:
                return json.dumps({
                    "project_id": project_id,
                    "date_from": date_from,
                    "date_to": date_to,
                    "summary": {"total_hours": 0, "total_cost": 0, "total_bill": 0, "entry_count": 0},
                    "by_resource": [],
                    "by_month": [],
                })

            # Prefetch resources (for names + internalCost) and roles (for fallback bill rate)
            resource_ids = {e.get("resourceID", 0) for e in time_entries} - {0}
            role_ids = {e.get("roleID", 0) for e in time_entries} - {0}
            await asyncio.gather(
                client._prefetch_ids(client._resource_cache, "Resources", resource_ids),
                client._prefetch_ids(client._role_cache, "Roles", role_ids),
            )

            # Aggregate
            total_hours = 0.0
            total_cost = 0.0
            total_bill = 0.0

            res_agg: dict[int, dict] = {}
            month_agg: dict[str, dict] = {}

            for e in time_entries:
                hours = float(e.get("hoursWorked") or 0)
                rid = e.get("resourceID", 0)
                role_id = e.get("roleID", 0)

                resource = client._resource_cache.get(rid, {})
                cost_rate = float(resource.get("internalCost") or 0)
                cost = round(hours * cost_rate, 2)

                # Bill rate: ContractRate override → standard Role rate → 0
                if role_id and role_id in contract_bill_rates:
                    bill_rate = contract_bill_rates[role_id]
                elif role_id:
                    role = client._role_cache.get(role_id, {})
                    bill_rate = float(role.get("hourlyRate") or 0)
                else:
                    bill_rate = 0.0
                bill = round(hours * bill_rate, 2)

                total_hours += hours
                total_cost += cost
                total_bill += bill

                # By resource
                if rid not in res_agg:
                    name = f"{resource.get('firstName', '')} {resource.get('lastName', '')}".strip() or f"Resource #{rid}"
                    res_agg[rid] = {"name": name, "hours": 0.0, "cost": 0.0, "bill": 0.0, "entry_count": 0}
                res_agg[rid]["hours"] += hours
                res_agg[rid]["cost"] += cost
                res_agg[rid]["bill"] += bill
                res_agg[rid]["entry_count"] += 1

                # By month
                date_worked = _format_date_iso(e.get("dateWorked", ""))
                month_key = date_worked[:7] if len(date_worked) >= 7 else "unknown"
                if month_key not in month_agg:
                    month_agg[month_key] = {"month": month_key, "hours": 0.0, "cost": 0.0, "bill": 0.0}
                month_agg[month_key]["hours"] += hours
                month_agg[month_key]["cost"] += cost
                month_agg[month_key]["bill"] += bill

            # Build by_resource with blended rates
            by_resource = []
            for r in sorted(res_agg.values(), key=lambda x: x["hours"], reverse=True):
                by_resource.append({
                    "name": r["name"],
                    "hours": round(r["hours"], 2),
                    "cost": round(r["cost"], 2),
                    "bill": round(r["bill"], 2),
                    "cost_rate": round(r["cost"] / r["hours"], 2) if r["hours"] > 0 else 0.0,
                    "bill_rate": round(r["bill"] / r["hours"], 2) if r["hours"] > 0 else 0.0,
                    "entry_count": r["entry_count"],
                })

            # Build by_month sorted ascending
            by_month = []
            for m in sorted(month_agg.values(), key=lambda x: x["month"]):
                by_month.append({
                    "month": m["month"],
                    "hours": round(m["hours"], 2),
                    "cost": round(m["cost"], 2),
                    "bill": round(m["bill"], 2),
                })

            payload = {
                "project_id": project_id,
                "date_from": date_from,
                "date_to": date_to,
                "summary": {
                    "total_hours": round(total_hours, 2),
                    "total_cost": round(total_cost, 2),
                    "total_bill": round(total_bill, 2),
                    "entry_count": len(time_entries),
                },
                "by_resource": by_resource,
                "by_month": by_month,
            }

            return json.dumps(payload, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Error getting project actuals: {str(e)}"})


@mcp.tool()
async def search_projects(
    company_name: str = "",
    company_id: int = 0,
    status: str = "",
    project_name: str = "",
) -> str:
    """
    Find and list projects. Use this to discover project IDs before calling get_project_summary.

    USE THIS WHEN asked about: "list projects", "active projects for [client]", "find project [name]".

    Returns a compact list with project name, company, status, estimated and actual hours.
    For deep details on a specific project, follow up with get_project_summary(project_id=...).

    Args:
        company_name: Filter by client name (partial match).
        company_id: Filter by exact company ID.
        status: Filter by name ('new', 'in progress', 'complete', 'inactive', 'contract setup',
            'lost', 'se', 'pending decision', 'project setup', 'se review', 'delayed execution',
            'cancelled', 'ready to start', 'not ready to start', 'active') or numeric ID. Empty = all.
        project_name: Search by project name (partial match).
    """
    try:
        async with _get_client() as client:
            filters = []

            if company_name and not company_id:
                cid = await client.resolve_company_by_name(company_name)
                if cid:
                    filters.append({"op": "eq", "field": "companyID", "value": cid})
                else:
                    return f"Could not find a company matching '{company_name}'."
            elif company_id:
                filters.append({"op": "eq", "field": "companyID", "value": company_id})

            status_name_map = {
                "new": 1, "in progress": 2, "complete": 5, "inactive": 0,
                "contract setup": 8, "lost": 9, "se": 10, "pending decision": 12,
                "project setup": 13, "se review": 14, "delayed execution": 15,
                "cancelled": 16, "ready to start": 18, "not ready to start": 20,
                "active": 2,
            }
            if status:
                status_val = status_name_map.get(status.lower())
                if status_val is None and status.isdigit():
                    status_val = int(status)
                if status_val is not None:
                    filters.append({"op": "eq", "field": "status", "value": status_val})

            if project_name:
                filters.append({"op": "contains", "field": "projectName", "value": project_name})

            if not filters:
                filters.append({"op": "exist", "field": "id"})

            projects = await client.query_all_pages("Projects", filters)
            await client.enrich_projects(projects)

            return format_entity_list("Projects", projects, [
                ("id", "ID"), ("projectName", "Name"), ("_companyName", "Company"),
                ("_statusLabel", "Status"), ("estimatedTime", "Est. Hours"),
                ("actualHours", "Actual Hours"), ("startDateTime", "Start"), ("endDateTime", "End")
            ])
    except Exception as e:
        return f"Error searching projects: {str(e)}"


@mcp.tool()
async def list_projects_json(
    company_name: str = "",
    company_id: int = 0,
    status: str = "",
    project_name: str = "",
) -> str:
    """
    Return projects as a JSON array for UI consumption.

    Same filters as search_projects, but returns machine-readable JSON instead of
    a formatted text table. Each element includes: id, name, company, status,
    est_hours, actual_hours, start, end.

    The response also includes a "pagination" object (pages_fetched, complete, etc.)
    so clients can tell whether the full project list was retrieved (Autotask returns
    up to 500 rows per query page).

    Args:
        company_name: Filter by client name (partial match).
        company_id: Filter by exact company ID.
        status: Filter by name ('new', 'in progress', 'complete', 'inactive', 'active', etc.) or numeric ID. Empty = all.
        project_name: Search by project name (partial match).
    """
    try:
        async with _get_client() as client:
            filters = []

            if company_name and not company_id:
                cid = await client.resolve_company_by_name(company_name)
                if cid:
                    filters.append({"op": "eq", "field": "companyID", "value": cid})
                else:
                    return json.dumps({"error": f"Could not find a company matching '{company_name}'.", "projects": []})
            elif company_id:
                filters.append({"op": "eq", "field": "companyID", "value": company_id})

            status_name_map = {
                "new": 1, "in progress": 2, "complete": 5, "inactive": 0,
                "contract setup": 8, "lost": 9, "se": 10, "pending decision": 12,
                "project setup": 13, "se review": 14, "delayed execution": 15,
                "cancelled": 16, "ready to start": 18, "not ready to start": 20,
                "active": 2,
            }
            if status:
                status_val = status_name_map.get(status.lower())
                if status_val is None and status.isdigit():
                    status_val = int(status)
                if status_val is not None:
                    filters.append({"op": "eq", "field": "status", "value": status_val})

            if project_name:
                filters.append({"op": "contains", "field": "projectName", "value": project_name})

            if not filters:
                filters.append({"op": "exist", "field": "id"})

            paging_status: dict = {}
            paging_meta_available = True
            try:
                projects = await client.query_all_pages("Projects", filters, paging_status=paging_status)
            except TypeError as ex:
                # Older api_client.py (no paging_status kwarg) — still list projects
                if "paging_status" not in str(ex):
                    raise
                projects = await client.query_all_pages("Projects", filters)
                paging_meta_available = False

            if paging_meta_available:
                paging_status["items_returned"] = len(projects)
            await client.enrich_projects(projects)

            result = []
            for p in projects:
                result.append({
                    "id":           p.get("id", 0),
                    "name":         p.get("projectName", ""),
                    "company":      p.get("_companyName", ""),
                    "status":       p.get("_statusLabel", ""),
                    "est_hours":    p.get("estimatedTime", 0) or 0,
                    "actual_hours": p.get("actualHours", 0) or 0,
                    "start":        p.get("startDateTime", None),
                    "end":          p.get("endDateTime", None),
                })

            if paging_meta_available:
                mr = paging_status.get("max_records_per_page") or 500
                pag = {
                    "complete": paging_status.get("complete", True),
                    "pages_fetched": paging_status.get("pages_fetched", 0),
                    "items_returned": len(result),
                    "max_records_per_page": mr,
                }
                if not pag["complete"]:
                    pag["warning"] = (
                        "Pagination did not finish; project list may be truncated. "
                        "Narrow filters (company, status, name) or compare Autotask UI."
                    )
                    if paging_status.get("failure"):
                        pag["failure"] = paging_status["failure"]
                    if paging_status.get("failures"):
                        pag["failures"] = paging_status["failures"]
                elif len(result) == mr and pag["pages_fetched"] == 1:
                    pag["note"] = (
                        f"Count equals one full API page ({mr} rows). If you expect more projects, "
                        "add filters or verify in Autotask — nextPageUrl may be missing in edge cases."
                    )
            else:
                pag = {
                    "complete": None,
                    "pages_fetched": None,
                    "items_returned": len(result),
                    "max_records_per_page": None,
                    "note": (
                        "Pagination metadata unavailable: deploy api_client.py that supports "
                        "query_all_pages(..., paging_status=). Listing still uses full pagination if implemented there."
                    ),
                }

            return json.dumps({"projects": result, "pagination": pag}, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Error listing projects: {str(e)}", "projects": []})


# ─── TASK TOOLS ─────────────────────────────────────────────────────

@mcp.tool()
async def get_task_details(
    task_id: int = 0,
    project_id: int = 0,
    task_name: str = "",
    include_time_entries: bool = True,
) -> str:
    """
    Task-level detail with estimated vs actual hours and time entries.

    USE THIS WHEN asked about: specific task status, time logged on a task, "who worked on [task]?".
    For project-wide task overview, prefer get_project_summary instead.

    Args:
        task_id: Specific task ID.
        project_id: Get all tasks for a project (combine with task_name to filter).
        task_name: Search tasks by name (partial match).
        include_time_entries: Include time entry lines per task (default True).
    """
    try:
        async with _get_client() as client:
            tasks = []

            if task_id:
                task = await client.get_entity("Tasks", task_id)
                if task:
                    tasks = [task]
            elif project_id:
                filters = [{"op": "eq", "field": "projectID", "value": project_id}]
                if task_name:
                    filters.append({"op": "contains", "field": "title", "value": task_name})
                tasks = await client.query_all_pages("Tasks", filters)
            elif task_name:
                tasks = await client.query_all_pages("Tasks", [
                    {"op": "contains", "field": "title", "value": task_name}
                ])

            if not tasks:
                return "No tasks found matching the criteria."

            await client.enrich_tasks(tasks)

            if include_time_entries:
                # Fetch all time entries for these tasks concurrently
                async def _fetch_te(task_id):
                    return await client.query_all_pages("TimeEntries", [
                        {"op": "eq", "field": "taskID", "value": task_id}
                    ])

                te_results = await asyncio.gather(
                    *[_fetch_te(t["id"]) for t in tasks],
                    return_exceptions=True,
                )
                # Flatten, batch-enrich, then assign to tasks
                all_te: list[dict] = []
                for result in te_results:
                    if isinstance(result, list):
                        all_te.extend(result)
                await client.enrich_time_entries(all_te)

                te_by_task: dict[int, list] = {}
                for te in all_te:
                    te_by_task.setdefault(te.get("taskID", 0), []).append(te)
                for t in tasks:
                    t["_time_entries"] = te_by_task.get(t["id"], [])

            return format_task_summary(tasks, include_time_entries)
    except Exception as e:
        return f"Error getting task details: {str(e)}"


# ─── TICKET TOOLS ───────────────────────────────────────────────────

@mcp.tool()
async def search_tickets(
    company_name: str = "",
    company_id: int = 0,
    resource_name: str = "",
    resource_id: int = 0,
    status: str = "",
    ticket_number: str = "",
    title: str = "",
    date_from: str = "",
    date_to: str = "",
    include_time_entries: bool = False,
) -> str:
    """
    Search and list service tickets with optional time entry analysis.

    USE THIS WHEN asked about: tickets, service requests, "open tickets for [client]",
    "what's John working on?" (ticket context), support ticket status.

    Returns tickets with company, status, assigned tech, and optional time entries per ticket.

    Args:
        company_name: Filter by client name (partial match).
        company_id: Filter by exact company ID.
        resource_name: Filter by assigned tech (partial match).
        resource_id: Filter by assigned resource ID.
        status: Filter: 'new', 'in_progress', 'complete', 'waiting'. Empty = all.
        ticket_number: Exact ticket number (e.g., T20250101.0001).
        title: Search by title (partial match).
        date_from: Created on or after (YYYY-MM-DD).
        date_to: Created on or before (YYYY-MM-DD).
        include_time_entries: Include time logged per ticket.
    """
    try:
        async with _get_client() as client:
            filters = []

            if company_name and not company_id:
                cid = await client.resolve_company_by_name(company_name)
                if cid:
                    filters.append({"op": "eq", "field": "companyID", "value": cid})
                else:
                    return f"Could not find a company matching '{company_name}'."
            elif company_id:
                filters.append({"op": "eq", "field": "companyID", "value": company_id})

            if resource_name:
                rid = await client.resolve_resource_by_name(resource_name)
                if rid:
                    filters.append({"op": "eq", "field": "assignedResourceID", "value": rid})
                else:
                    return f"Could not find a resource matching '{resource_name}'."
            elif resource_id:
                filters.append({"op": "eq", "field": "assignedResourceID", "value": resource_id})

            status_map = {"new": 1, "in_progress": 2, "complete": 5, "waiting": 7}
            if status and status.lower() in status_map:
                filters.append({"op": "eq", "field": "status", "value": status_map[status.lower()]})

            if ticket_number:
                filters.append({"op": "eq", "field": "ticketNumber", "value": ticket_number})
            if title:
                filters.append({"op": "contains", "field": "title", "value": title})
            if date_from:
                filters.append({"op": "gte", "field": "createDate", "value": f"{date_from}T00:00:00"})
            if date_to:
                filters.append({"op": "lte", "field": "createDate", "value": f"{date_to}T23:59:59"})

            if not filters:
                filters.append({"op": "exist", "field": "id"})

            tickets = await client.query_all_pages("Tickets", filters)
            await client.enrich_tickets(tickets)

            if include_time_entries:
                async def _fetch_te(ticket_id):
                    return await client.query_all_pages("TimeEntries", [
                        {"op": "eq", "field": "ticketID", "value": ticket_id}
                    ])

                te_results = await asyncio.gather(
                    *[_fetch_te(t["id"]) for t in tickets],
                    return_exceptions=True,
                )
                all_te: list[dict] = []
                for result in te_results:
                    if isinstance(result, list):
                        all_te.extend(result)
                await client.enrich_time_entries(all_te)

                te_by_ticket: dict[int, list] = {}
                for te in all_te:
                    te_by_ticket.setdefault(te.get("ticketID", 0), []).append(te)
                for t in tickets:
                    t["_time_entries"] = te_by_ticket.get(t["id"], [])

            return format_ticket_summary(tickets, include_time_entries)
    except Exception as e:
        return f"Error searching tickets: {str(e)}"


# ─── CONTRACT TOOLS ─────────────────────────────────────────────────

@mcp.tool()
async def get_contract_summary(
    contract_id: int = 0,
    company_name: str = "",
    company_id: int = 0,
    contract_name: str = "",
    include_projects: bool = True,
    include_financials: bool = True,
    target_gm_pct: float = 60.0,
) -> str:
    """
    Company-wide or multi-project financial overview. Shows all contracts for a client
    with aggregated cost, margin, and budget metrics across all associated projects.

    USE THIS WHEN asked about: client-wide profitability ("are we profitable on [client]?"),
    all projects for a company, or when the user explicitly mentions contracts.
    For a SINGLE project's margin, prefer get_project_summary instead.

    Returns: contract details, GM analysis (budget, cost, projected/current margin),
    and list of associated projects.

    Args:
        contract_id: Specific contract ID.
        company_name: Find all contracts for a client (partial match).
        company_id: Filter by company ID.
        contract_name: Search by contract name (partial match).
        include_projects: List projects under each contract (default True).
        include_financials: Compute cost & GM from time entries (default True). Disable for faster results.
        target_gm_pct: Sold-at gross margin % for cost budget. Default 60%.
    """
    try:
        async with _get_client() as client:
            contracts = []

            if contract_id:
                c = await client.get_entity("Contracts", contract_id)
                if c:
                    contracts = [c]
            else:
                filters = []
                if company_name and not company_id:
                    cid = await client.resolve_company_by_name(company_name)
                    if cid:
                        filters.append({"op": "eq", "field": "companyID", "value": cid})
                    else:
                        return f"Could not find a company matching '{company_name}'."
                elif company_id:
                    filters.append({"op": "eq", "field": "companyID", "value": company_id})
                if contract_name:
                    filters.append({"op": "contains", "field": "contractName", "value": contract_name})
                if not filters:
                    filters.append({"op": "exist", "field": "id"})
                contracts = await client.query_all_pages("Contracts", filters)

            if not contracts:
                return "No contracts found matching the criteria."

            await client.enrich_contracts(contracts)

            async def _fetch_projects_and_costs(contract):
                # Fetch projects for this contract
                projects = await client.query_all_pages("Projects", [
                    {"op": "eq", "field": "contractID", "value": contract["id"]}
                ])
                await client.enrich_projects(projects)
                contract["_projects"] = projects

                # Compute financial metrics from all time entries on this contract
                if include_financials and float(contract.get("estimatedRevenue") or 0) > 0:
                    costs = await client.get_contract_costs(contract["id"])
                    # Remaining hours across all projects under this contract
                    remaining_hours = 0.0
                    for p in projects:
                        est = float(p.get("estimatedTime") or 0)
                        act = float(p.get("actualHours") or 0)
                        remaining_hours += max(0, est - act)
                    contract["_financials"] = _compute_financials(
                        contract, costs["total_hours"], costs["total_cost"], remaining_hours, target_gm_pct,
                    )

            if include_projects or include_financials:
                await asyncio.gather(*[_fetch_projects_and_costs(c) for c in contracts])

            return format_contract_summary(contracts, include_projects)
    except Exception as e:
        return f"Error getting contract summary: {str(e)}"


# ─── COMPANY / CLIENT TOOLS ────────────────────────────────────────

@mcp.tool()
async def get_company_info(
    company_id: int = 0,
    company_name: str = "",
    include_contracts: bool = True,
    include_projects: bool = True,
) -> str:
    """
    Company/client overview — contact info, contracts, and projects.

    USE THIS WHEN asked about: "tell me about [client]", company details, "what contracts
    does [client] have?", client overview. For financial detail on a specific contract,
    follow up with get_contract_summary.

    Args:
        company_id: Specific company ID.
        company_name: Search by name (partial match). If multiple match, returns a list.
        include_contracts: List contracts for this company (default True).
        include_projects: List projects for this company (default True).
    """
    try:
        async with _get_client() as client:
            if not company_id and company_name:
                companies = await client.query_all_pages("Companies", [
                    {"op": "contains", "field": "companyName", "value": company_name}
                ])
                if not companies:
                    return f"No companies found matching '{company_name}'."
                if len(companies) > 1:
                    lines = [f"Multiple companies found matching '{company_name}':"]
                    for c in companies[:25]:
                        lines.append(f"  - ID {c['id']}: {c.get('companyName', 'N/A')}")
                    lines.append("\nPlease specify a company_id for details.")
                    return "\n".join(lines)
                company_id = companies[0]["id"]

            company = await client.get_entity("Companies", company_id)
            if not company:
                return f"Company ID {company_id} not found."

            # Fetch contracts and projects concurrently
            contracts = []
            projects = []
            fetch_coros = []
            if include_contracts:
                fetch_coros.append(client.query_all_pages("Contracts", [
                    {"op": "eq", "field": "companyID", "value": company_id}
                ]))
            if include_projects:
                fetch_coros.append(client.query_all_pages("Projects", [
                    {"op": "eq", "field": "companyID", "value": company_id}
                ]))

            if fetch_coros:
                results = await asyncio.gather(*fetch_coros)
                idx = 0
                if include_contracts:
                    contracts = results[idx]
                    idx += 1
                if include_projects:
                    projects = results[idx]

            # Enrich contracts and projects (batch prefetch avoids duplicate lookups)
            await asyncio.gather(
                client.enrich_contracts(contracts),
                client.enrich_projects(projects),
            )

            return format_company_summary(company, contracts, projects)
    except Exception as e:
        return f"Error getting company info: {str(e)}"


# ─── RESOURCE TOOLS ─────────────────────────────────────────────────

@mcp.tool()
async def search_resources(
    name: str = "",
    active_only: bool = True,
) -> str:
    """
    Find employees/technicians by name. Use to discover resource IDs for other tools.

    USE THIS WHEN asked about: "who is [name]?", looking up a person, finding a resource ID.

    Args:
        name: First or last name (partial match). Empty = list all.
        active_only: Only active resources (default True).
    """
    try:
        async with _get_client() as client:
            filters = []

            if active_only:
                filters.append({"op": "eq", "field": "isActive", "value": True})

            if name:
                filters = [{
                    "op": "or",
                    "items": [
                        {"op": "contains", "field": "firstName", "value": name},
                        {"op": "contains", "field": "lastName", "value": name},
                    ]
                }]
                if active_only:
                    filters.append({"op": "eq", "field": "isActive", "value": True})

            if not filters:
                filters.append({"op": "exist", "field": "id"})

            resources = await client.query_all_pages("Resources", filters)
            return format_resource_summary(resources)
    except Exception as e:
        return f"Error searching resources: {str(e)}"


# ─── BILLING CODE / WORK TYPE TOOLS ────────────────────────────────

@mcp.tool()
async def get_work_types() -> str:
    """
    Reference data: list all work types (billing codes) configured in Autotask.
    Rarely needed directly — use when asked about billing code names or work type configuration.
    """
    try:
        async with _get_client() as client:
            billing_codes = await client.query_all_pages("BillingCodes", [
                {"op": "eq", "field": "billingCodeType", "value": 1},
            ])
            return format_entity_list("Work Types (Billing Codes)", billing_codes, [
                ("id", "ID"), ("name", "Name"), ("isActive", "Active"),
                ("unitPrice", "Unit Price"), ("description", "Description")
            ])
    except Exception as e:
        return f"Error getting work types: {str(e)}"


# ─── HOURS ANALYSIS TOOL ───────────────────────────────────────────

@mcp.tool()
async def analyze_hours(
    date_from: str = "",
    date_to: str = "",
    company_name: str = "",
    company_id: int = 0,
    resource_name: str = "",
    resource_id: int = 0,
    project_id: int = 0,
    contract_id: int = 0,
) -> str:
    """
    Quick utilization snapshot — hours and cost grouped by resource.

    USE THIS WHEN asked about: "utilization this week", "who's been busy?",
    "team hours summary", "how many hours did everyone log?".

    For more flexible breakdowns (by project, date, company, etc.) use
    query_time_entries with a group_by parameter instead.

    Args:
        date_from: Start date (YYYY-MM-DD). Defaults to last 7 days.
        date_to: End date (YYYY-MM-DD). Defaults to today.
        company_name: Filter by client name (partial match).
        company_id: Filter by company ID.
        resource_name: Filter by employee name (partial match).
        resource_id: Filter by resource ID.
        project_id: Filter by project ID.
        contract_id: Filter by contract ID.
    """
    return await query_time_entries(
        date_from=date_from,
        date_to=date_to,
        company_name=company_name,
        company_id=company_id,
        resource_name=resource_name,
        resource_id=resource_id,
        project_id=project_id,
        contract_id=contract_id,
        group_by="resource",
        include_details=False,
    )


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
