# LLM-friendly formatters for Autotask entities
# Transforms raw API data into readable summaries with aggregations

from typing import Any, Optional


def _safe_float(val) -> float:
    """Safely convert a value to float."""
    try:
        return float(val) if val is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _format_date(dt_str: str) -> str:
    """Format an ISO datetime string to a readable date."""
    if not dt_str:
        return "N/A"
    return dt_str[:10] if "T" in str(dt_str) else str(dt_str)[:10]


def _format_hours(hours: float) -> str:
    """Format hours to 2 decimal places."""
    return f"{hours:.2f}"


def _format_currency(amount) -> str:
    """Format a dollar amount with commas and 2 decimal places."""
    val = _safe_float(amount)
    return f"${val:,.2f}"


def _format_pct(val) -> str:
    """Format a percentage value with 1 decimal place and sign indicator."""
    if val is None:
        return "N/A"
    v = _safe_float(val)
    return f"{v:+.1f}%" if v != 0 else "0.0%"


# ─── TIME ENTRIES ───────────────────────────────────────────────────

def _format_paging_footer_for_llm(entries: list, paging_status: Optional[dict[str, Any]]) -> list[str]:
    """Build markdown lines so the model knows whether API pagination completed."""
    if not paging_status or paging_status.get("pages_fetched") is None:
        return []
    lines: list[str] = []
    lines.append("")
    lines.append("### API data completeness")
    complete = paging_status.get("complete", True)
    pages = paging_status.get("pages_fetched", 0)
    mr = paging_status.get("max_records_per_page") or 500
    n = len(entries)
    if not complete:
        lines.append(
            "- **Warning:** Autotask pagination did not finish (a follow-up page request failed). "
            "**Totals and costs below may be incomplete.** Do not treat them as authoritative against the Autotask UI."
        )
        failures = list(paging_status.get("failures") or [])
        if paging_status.get("failure"):
            failures.insert(0, paging_status["failure"])
        for f in failures[:3]:
            st = f.get("http_status")
            ph = f.get("phase", "")
            lines.append(f"  - Detail: phase={ph}" + (f", http_status={st}" if st is not None else ""))
    else:
        lines.append(
            f"- **Pagination:** Completed successfully — **{pages}** API page(s) merged into **{n}** time entry row(s) "
            f"(up to **{mr}** rows per page per Autotask query)."
        )
        if n == mr and pages == 1:
            lines.append(
                "- **Note:** Row count equals one full API page. If totals disagree with Autotask, try a narrower "
                "date range or compare to an Autotask report — the API may omit `nextPageUrl` in edge cases."
            )
    return lines


def format_time_entries_summary(
    entries: list,
    group_by: str = "",
    include_details: bool = False,
    paging_status: Optional[dict[str, Any]] = None,
) -> str:
    """Format time entries into an LLM-friendly summary."""
    if not entries:
        msg = "No time entries found matching the criteria."
        if paging_status and paging_status.get("complete") is False:
            msg += (
                "\n\n**Warning:** API pagination did not complete; there may be additional rows not shown."
            )
        return msg

    total_hours = sum(_safe_float(e.get("hoursWorked", 0)) for e in entries)
    total_billed = sum(_safe_float(e.get("hoursToBill", 0)) for e in entries)
    billable_hours = sum(_safe_float(e.get("hoursWorked", 0)) for e in entries if not e.get("isNonBillable"))
    non_billable_hours = sum(_safe_float(e.get("hoursWorked", 0)) for e in entries if e.get("isNonBillable"))

    # Date range
    dates = [e.get("dateWorked", "") for e in entries if e.get("dateWorked")]
    date_range = ""
    if dates:
        sorted_dates = sorted(dates)
        date_range = f" ({_format_date(sorted_dates[0])} to {_format_date(sorted_dates[-1])})"

    # Aggregate cost and billing from enriched entries
    total_cost = sum(_safe_float(e.get("_cost", 0)) for e in entries)
    total_bill_amount = sum(_safe_float(e.get("_billAmount", 0)) for e in entries)
    billable_pct = (billable_hours / total_hours * 100) if total_hours else 0

    lines = []
    lines.append(f"## Time Entries Summary{date_range}")
    lines.append(f"")
    lines.append(f"**Total Entries:** {len(entries)} | **Total Hours:** {_format_hours(total_hours)} | **Billable:** {_format_hours(billable_hours)} ({billable_pct:.0f}%) | **Non-Billable:** {_format_hours(non_billable_hours)}")
    if total_cost > 0 or total_bill_amount > 0:
        cost_str = f"**Cost:** {_format_currency(total_cost)} ({_format_currency(total_cost / total_hours if total_hours else 0)}/hr blended)" if total_cost > 0 else ""
        bill_str = f"**Bill:** {_format_currency(total_bill_amount)} ({_format_currency(total_bill_amount / total_hours if total_hours else 0)}/hr blended)" if total_bill_amount > 0 else ""
        margin_str = ""
        if total_cost > 0 and total_bill_amount > 0:
            margin = total_bill_amount - total_cost
            margin_pct = (margin / total_bill_amount * 100) if total_bill_amount else 0
            margin_str = f" | **Margin:** {_format_currency(margin)} ({margin_pct:.0f}%)"
        parts = [p for p in [cost_str, bill_str] if p]
        lines.append(f"{' | '.join(parts)}{margin_str}")
    if total_billed > 0 and total_billed != total_hours:
        lines.append(f"**Hours to Bill:** {_format_hours(total_billed)}")

    lines.extend(_format_paging_footer_for_llm(entries, paging_status))

    # Group by analysis
    if group_by:
        lines.append(f"")
        lines.append(f"### Breakdown by {group_by.replace('_', ' ').title()}")
        lines.append("")

        group_key_map = {
            "resource": "_resourceName",
            "company": "_companyName",
            "project": "_projectName",
            "task": "_taskTitle",
            "ticket": "_ticketNumber",
            "date": "dateWorked",
            "work_type": "_workTypeName",
        }
        key = group_key_map.get(group_by, group_by)

        groups: dict[str, list] = {}
        for e in entries:
            val = e.get(key, "Unknown")
            if key == "dateWorked":
                val = _format_date(val)
            if not val:
                val = "Unknown"
            groups.setdefault(str(val), []).append(e)

        # Sort by total cost descending (falls back to hours if no cost data)
        sorted_groups = sorted(
            groups.items(),
            key=lambda x: sum(_safe_float(e.get("_cost", 0)) or _safe_float(e.get("hoursWorked", 0)) for e in x[1]),
            reverse=True,
        )

        for group_name, group_entries in sorted_groups:
            g_total = sum(_safe_float(e.get("hoursWorked", 0)) for e in group_entries)
            g_billable = sum(_safe_float(e.get("hoursWorked", 0)) for e in group_entries if not e.get("isNonBillable"))
            g_non_billable = g_total - g_billable
            g_cost = sum(_safe_float(e.get("_cost", 0)) for e in group_entries)
            g_bill = sum(_safe_float(e.get("_billAmount", 0)) for e in group_entries)
            cost_str = f" | Cost: {_format_currency(g_cost)}" if g_cost > 0 else ""
            bill_str = f" | Bill: {_format_currency(g_bill)}" if g_bill > 0 else ""

            # When grouping by resource, show cost/bill rates and remaining hours from their assigned tasks
            rate_str = ""
            remaining_str = ""
            if group_by == "resource":
                cost_rates = {_safe_float(e.get("_costRate", 0)) for e in group_entries}
                cost_rates.discard(0.0)
                bill_rates = {_safe_float(e.get("_billRate", 0)) for e in group_entries}
                bill_rates.discard(0.0)

                rate_parts = []
                if len(cost_rates) == 1:
                    rate_parts.append(f"Cost: {_format_currency(cost_rates.pop())}/hr")
                elif len(cost_rates) > 1:
                    rate_parts.append(f"Cost: {_format_currency(min(cost_rates))}–{_format_currency(max(cost_rates))}/hr")
                if len(bill_rates) == 1:
                    rate_parts.append(f"Bill: {_format_currency(bill_rates.pop())}/hr")
                elif len(bill_rates) > 1:
                    rate_parts.append(f"Bill: {_format_currency(min(bill_rates))}–{_format_currency(max(bill_rates))}/hr")
                if rate_parts:
                    rate_str = f" | Rates: {', '.join(rate_parts)}"

                seen_tasks: set[int] = set()
                remaining = 0.0
                for e in group_entries:
                    tid = e.get("_taskID", 0)
                    assigned_rid = e.get("_taskAssignedResourceID", 0)
                    entry_rid = e.get("resourceID", 0)
                    # Only count remaining hours on tasks where this resource is the primary assignee
                    if tid and tid not in seen_tasks and assigned_rid == entry_rid:
                        seen_tasks.add(tid)
                        remaining += _safe_float(e.get("_taskRemainingHours", 0))
                if remaining > 0:
                    remaining_str = f" | Remaining: {_format_hours(remaining)} hrs"

            lines.append(f"**{group_name}:** {_format_hours(g_total)} hrs ({len(group_entries)} entries) — Billable: {_format_hours(g_billable)}, Non-Billable: {_format_hours(g_non_billable)}{cost_str}{bill_str}{rate_str}{remaining_str}")

    # Individual details
    if include_details:
        lines.append(f"")
        lines.append(f"### Individual Time Entries")
        lines.append("")
        for e in sorted(entries, key=lambda x: x.get("dateWorked", "")):
            date = _format_date(e.get("dateWorked", ""))
            resource = e.get("_resourceName", "Unknown")
            hours = _format_hours(_safe_float(e.get("hoursWorked", 0)))
            billable = e.get("_billableLabel", "")
            work_type = e.get("_workTypeName", "")
            summary = (e.get("summaryNotes", "") or "")[:100]
            context = ""
            if e.get("_taskTitle"):
                context = f"Task: {e['_taskTitle']}"
            elif e.get("_ticketNumber"):
                context = f"Ticket: {e['_ticketNumber']} - {e.get('_ticketTitle', '')}"
            company = e.get("_companyName", "")
            entry_cost = _safe_float(e.get("_cost", 0))
            entry_cost_rate = _safe_float(e.get("_costRate", 0))
            entry_bill_rate = _safe_float(e.get("_billRate", 0))
            entry_bill_amt = _safe_float(e.get("_billAmount", 0))
            rate_detail = ""
            if entry_cost_rate > 0 or entry_bill_rate > 0:
                parts = []
                if entry_cost_rate > 0:
                    parts.append(f"Cost: {_format_currency(entry_cost_rate)}/hr → {_format_currency(entry_cost)}")
                if entry_bill_rate > 0:
                    parts.append(f"Bill: {_format_currency(entry_bill_rate)}/hr → {_format_currency(entry_bill_amt)}")
                rate_detail = f" | {'; '.join(parts)}"
            lines.append(f"- **{date}** | {resource} | {hours} hrs ({billable}) | {work_type} | {context} | {company}{rate_detail}")
            if summary:
                lines.append(f"  Notes: {summary}")

    # Interpretive summary for the LLM to use directly in answers
    lines.append(f"")
    lines.append(f"### Key Insights")
    insights = []
    if billable_pct >= 80:
        insights.append(f"High utilization: {billable_pct:.0f}% of hours are billable.")
    elif billable_pct < 50 and total_hours > 0:
        insights.append(f"Low billable ratio: only {billable_pct:.0f}% of hours are billable.")
    if total_cost > 0 and total_bill_amount > 0:
        margin = total_bill_amount - total_cost
        margin_pct = (margin / total_bill_amount * 100) if total_bill_amount else 0
        insights.append(f"Labor cost: {_format_currency(total_cost)} vs. billing: {_format_currency(total_bill_amount)} → {margin_pct:.0f}% margin ({_format_currency(margin)}).")
    elif total_cost > 0:
        insights.append(f"Total labor cost for this period: {_format_currency(total_cost)} at {_format_currency(total_cost / total_hours if total_hours else 0)}/hr blended rate.")
    if group_by and sorted_groups:
        top_name, top_entries = sorted_groups[0]
        top_hrs = sum(_safe_float(e.get("hoursWorked", 0)) for e in top_entries)
        insights.append(f"Highest volume: {top_name} with {_format_hours(top_hrs)} hrs.")
    if not insights:
        insights.append(f"{len(entries)} entries totaling {_format_hours(total_hours)} hrs in this period.")
    for i in insights:
        lines.append(f"- {i}")

    return "\n".join(lines)


# ─── PROJECT SUMMARY ────────────────────────────────────────────────

def format_project_summary(
    project: dict,
    tasks: list = None,
    time_entries: list = None,
    include_tasks: bool = True,
    include_time_entries: bool = False,
    contract: dict = None,
    financials: dict = None,
    resource_allocations: list = None,
) -> str:
    """Format a project into an LLM-friendly summary with optional financial metrics."""
    lines = []
    lines.append(f"## Project: {project.get('projectName', 'N/A')}")
    lines.append(f"")
    lines.append(f"**ID:** {project.get('id')} | **Company:** {project.get('_companyName', 'N/A')} | **Status:** {project.get('_statusLabel', 'N/A')}")
    lines.append(f"**Lead:** {project.get('_leadName', 'N/A')} | **Period:** {_format_date(project.get('startDateTime', ''))} to {_format_date(project.get('endDateTime', ''))}")
    lines.append(f"**Hours:** {_format_hours(_safe_float(project.get('estimatedTime', 0)))} estimated | {_format_hours(_safe_float(project.get('actualHours', 0)))} actual | {project.get('completedPercentage', 0)}% complete")

    est = _safe_float(project.get('estimatedTime', 0))
    act = _safe_float(project.get('actualHours', 0))
    if est > 0:
        variance = est - act
        lines.append(f"**Hours Variance:** {_format_hours(variance)} ({'under' if variance >= 0 else 'OVER'} estimate)")

    if contract:
        lines.append(f"**Engagement Type:** {contract.get('_typeLabel', 'N/A')}")
    elif project.get("contractID"):
        lines.append(f"**Contract ID:** {project.get('contractID')}")

    # ── Financial Analysis ──────────────────────────────────────────
    if financials and financials.get("contract_amount"):
        lines.append(f"")
        lines.append(f"### Gross Margin Analysis")
        lines.append("")
        lines.append(f"**Project Budget:** {_format_currency(financials['contract_amount'])}")
        lines.append(f"**Budget Period:** {_format_date(financials.get('contract_start', ''))} to {_format_date(financials.get('contract_end', ''))}")
        lines.append(f"**Target GM:** {financials.get('target_gm_pct', 60):.0f}% → **Cost Budget:** {_format_currency(financials.get('cost_budget', 0))}")

        sibling_count = financials.get("sibling_project_count", 0)
        if sibling_count > 1:
            lines.append(f"**Note:** Budget is shared across {sibling_count} projects — costs below reflect this project only")

        lines.append(f"")
        lines.append(f"**Actual Cost to Date:** {_format_currency(financials['actual_cost'])} ({_format_hours(financials['actual_hours'])} hrs × {_format_currency(financials['blended_cost_rate'])}/hr blended)")
        lines.append(f"**Remaining Estimated Hours:** {_format_hours(financials['remaining_hours'])}")
        lines.append(f"**Projected Total Cost:** {_format_currency(financials['projected_total_cost'])}")

        if financials.get("budget_consumed_pct") is not None:
            lines.append(f"**Cost Budget Consumed:** {financials['budget_consumed_pct']:.1f}% of {_format_currency(financials.get('cost_budget', 0))}")

        lines.append(f"")
        if financials.get("projected_gm") is not None:
            lines.append(f"**Projected Gross Margin:** {_format_currency(financials['projected_gm'])} ({_format_pct(financials.get('projected_gm_pct'))})")
        lines.append(f"**Prorated Budget to Date:** {_format_currency(financials['prorated_revenue'])}")
        if financials.get("prorated_cost_budget"):
            lines.append(f"**Prorated Cost Budget to Date:** {_format_currency(financials['prorated_cost_budget'])}")
        if financials.get("current_gm") is not None:
            lines.append(f"**Current Gross Margin:** {_format_currency(financials['current_gm'])} ({_format_pct(financials.get('current_gm_pct'))})")

    # ── Per-Resource Allocation ────────────────────────────────────
    if resource_allocations:
        lines.append(f"")
        lines.append(f"### Resource Allocation")
        lines.append("")
        for ra in resource_allocations:
            name = ra["resourceName"]
            est = _safe_float(ra["estimated_hours"])
            rem = _safe_float(ra["remaining_hours"])
            act = _safe_float(ra["actual_hours"])
            cost = _safe_float(ra["actual_cost"])
            task_count = ra["tasks_assigned"]
            parts = []
            if task_count > 0:
                parts.append(f"{task_count} tasks")
            if est > 0:
                parts.append(f"Est: {_format_hours(est)} hrs")
            parts.append(f"Remaining: {_format_hours(rem)} hrs")
            if act > 0:
                parts.append(f"Actual: {_format_hours(act)} hrs")
            if cost > 0:
                parts.append(f"Cost: {_format_currency(cost)}")
            # Flag resources with no remaining hours but who logged time (unassigned contributors)
            if task_count == 0 and act > 0:
                parts.append("(logged time but not primary assignee on any task)")
            lines.append(f"- **{name}:** {' | '.join(parts)}")

    if include_tasks and tasks:
        lines.append(f"")
        lines.append(f"### Tasks ({len(tasks)})")
        lines.append("")

        total_est = sum(_safe_float(t.get("estimatedHours", 0)) for t in tasks)
        total_remaining = sum(_safe_float(t.get("remainingHours", 0)) for t in tasks)
        lines.append(f"**Total Estimated Hours (Tasks):** {_format_hours(total_est)} | **Remaining:** {_format_hours(total_remaining)}")
        lines.append("")

        for t in tasks:
            est_h = _safe_float(t.get("estimatedHours", 0))
            remaining = _safe_float(t.get("remainingHours", 0))
            status = t.get("_statusLabel", "Unknown")
            assigned = t.get("_assignedResourceName", "Unassigned")
            te_info = ""
            if include_time_entries and t.get("_time_entries"):
                te_hours = sum(_safe_float(te.get("hoursWorked", 0)) for te in t["_time_entries"])
                te_cost = sum(_safe_float(te.get("_cost", 0)) for te in t["_time_entries"])
                te_info = f" | Logged: {_format_hours(te_hours)} hrs ({_format_currency(te_cost)})"
            remaining_label = f"Remaining: {_format_hours(remaining)} hrs" if remaining > 0 else "Remaining: 0"
            lines.append(f"- **{t.get('title', 'N/A')}** (ID: {t.get('id')}) — {status} | {assigned} | Est: {_format_hours(est_h)} hrs | {remaining_label}{te_info}")

    # Interpretive summary for LLM
    lines.append(f"")
    lines.append(f"### Key Insights")
    insights = []
    if est > 0 and act > 0:
        if act > est:
            insights.append(f"Project is {_format_hours(act - est)} hours OVER estimate ({((act - est) / est * 100):.0f}% over).")
        elif est - act < est * 0.1:
            insights.append(f"Project is approaching estimate — only {_format_hours(est - act)} hours remaining of {_format_hours(est)} budgeted.")
    if financials and financials.get("projected_gm") is not None:
        pgm_pct = financials.get("projected_gm_pct", 0)
        target = financials.get("target_gm_pct", 60)
        if pgm_pct and pgm_pct >= target:
            insights.append(f"Projected GM of {_format_pct(pgm_pct)} is at or above the {target:.0f}% target — healthy.")
        elif pgm_pct and pgm_pct > 0:
            insights.append(f"Projected GM of {_format_pct(pgm_pct)} is below the {target:.0f}% target — margin pressure.")
        elif pgm_pct is not None and pgm_pct <= 0:
            insights.append(f"⚠ Projected GM is negative ({_format_pct(pgm_pct)}) — project is projected to lose money.")
    if financials and financials.get("budget_consumed_pct") is not None:
        bc = financials["budget_consumed_pct"]
        if bc > 100:
            insights.append(f"⚠ Cost budget is {bc:.0f}% consumed — over budget.")
        elif bc > 80:
            insights.append(f"Cost budget is {bc:.0f}% consumed — nearing limit.")
    completion = _safe_float(project.get('completedPercentage', 0))
    if completion > 0:
        insights.append(f"Project is {completion:.0f}% complete.")
    if not insights:
        insights.append(f"Project has {_format_hours(act)} actual hours logged against {_format_hours(est)} estimated.")
    for i in insights:
        lines.append(f"- {i}")

    return "\n".join(lines)


# ─── TASK SUMMARY ───────────────────────────────────────────────────

def format_task_summary(tasks: list, include_time_entries: bool = False) -> str:
    """Format tasks into an LLM-friendly summary."""
    if not tasks:
        return "No tasks found."

    lines = []
    lines.append(f"## Tasks ({len(tasks)})")
    lines.append("")

    for t in tasks:
        lines.append(f"### {t.get('title', 'N/A')} (ID: {t.get('id')})")
        lines.append(f"**Project:** {t.get('_projectName', 'N/A')}")
        lines.append(f"**Status:** {t.get('_statusLabel', 'Unknown')}")
        lines.append(f"**Assigned To:** {t.get('_assignedResourceName', 'Unassigned')}")
        lines.append(f"**Estimated Hours:** {_format_hours(_safe_float(t.get('estimatedHours', 0)))}")
        lines.append(f"**Remaining Hours:** {_format_hours(_safe_float(t.get('remainingHours', 0)))}")
        lines.append(f"**Start:** {_format_date(t.get('startDateTime', ''))} | **End:** {_format_date(t.get('endDateTime', ''))}")

        if include_time_entries and t.get("_time_entries"):
            te_list = t["_time_entries"]
            total_te = sum(_safe_float(te.get("hoursWorked", 0)) for te in te_list)
            billable_te = sum(_safe_float(te.get("hoursWorked", 0)) for te in te_list if not te.get("isNonBillable"))
            lines.append(f"**Time Logged:** {_format_hours(total_te)} hrs (Billable: {_format_hours(billable_te)})")
            for te in sorted(te_list, key=lambda x: x.get("dateWorked", "")):
                lines.append(f"  - {_format_date(te.get('dateWorked', ''))} | {te.get('_resourceName', 'Unknown')} | {_format_hours(_safe_float(te.get('hoursWorked', 0)))} hrs | {te.get('_billableLabel', '')}")
        lines.append("")

    return "\n".join(lines)


# ─── TICKET SUMMARY ─────────────────────────────────────────────────

def format_ticket_summary(tickets: list, include_time_entries: bool = False) -> str:
    """Format tickets into an LLM-friendly summary."""
    if not tickets:
        return "No tickets found matching the criteria."

    lines = []
    lines.append(f"## Tickets ({len(tickets)})")
    lines.append("")

    total_hours = 0
    for t in tickets:
        te_list = t.get("_time_entries", [])
        t_hours = sum(_safe_float(te.get("hoursWorked", 0)) for te in te_list)
        total_hours += t_hours

        lines.append(f"### {t.get('ticketNumber', 'N/A')} — {t.get('title', 'N/A')}")
        lines.append(f"**Company:** {t.get('_companyName', 'N/A')} | **Status:** {t.get('_statusLabel', 'Unknown')} | **Assigned:** {t.get('_assignedResourceName', 'Unassigned')}")
        lines.append(f"**Created:** {_format_date(t.get('createDate', ''))} | **Priority:** {t.get('priority', 'N/A')}")

        if include_time_entries and te_list:
            billable_te = sum(_safe_float(te.get("hoursWorked", 0)) for te in te_list if not te.get("isNonBillable"))
            lines.append(f"**Time Logged:** {_format_hours(t_hours)} hrs (Billable: {_format_hours(billable_te)})")
            for te in sorted(te_list, key=lambda x: x.get("dateWorked", "")):
                lines.append(f"  - {_format_date(te.get('dateWorked', ''))} | {te.get('_resourceName', 'Unknown')} | {_format_hours(_safe_float(te.get('hoursWorked', 0)))} hrs | {te.get('_billableLabel', '')}")
        lines.append("")

    if include_time_entries:
        lines.insert(2, f"**Total Hours Across All Tickets:** {_format_hours(total_hours)}")
        lines.insert(3, "")

    return "\n".join(lines)


# ─── CONTRACT SUMMARY ──────────────────────────────────────────────

def format_contract_summary(contracts: list, include_projects: bool = False) -> str:
    """Format contracts into an LLM-friendly summary with financial metrics."""
    if not contracts:
        return "No contracts found matching the criteria."

    lines = []
    lines.append(f"## Contracts ({len(contracts)})")
    lines.append("")

    for c in contracts:
        lines.append(f"### {c.get('contractName', 'N/A')} (ID: {c.get('id')})")
        lines.append(f"**Company:** {c.get('_companyName', 'N/A')}")
        lines.append(f"**Type:** {c.get('_typeLabel', 'N/A')} | **Status:** {c.get('_statusLabel', 'N/A')}")
        lines.append(f"**Start:** {_format_date(c.get('startDate', ''))} | **End:** {_format_date(c.get('endDate', ''))}")

        if c.get("estimatedRevenue"):
            lines.append(f"**Contract Value:** {_format_currency(c.get('estimatedRevenue', 0))}")
        if c.get("estimatedHours"):
            lines.append(f"**Estimated Hours:** {_format_hours(_safe_float(c.get('estimatedHours', 0)))}")

        # ── Financial Analysis ──────────────────────────────────────
        fin = c.get("_financials")
        if fin and fin.get("contract_amount"):
            lines.append(f"")
            lines.append(f"**Gross Margin Analysis:**")
            lines.append(f"  Target GM: {fin.get('target_gm_pct', 60):.0f}% → Cost Budget: {_format_currency(fin.get('cost_budget', 0))}")
            lines.append(f"  Actual Cost to Date: {_format_currency(fin['actual_cost'])} ({_format_hours(fin['actual_hours'])} hrs × {_format_currency(fin['blended_cost_rate'])}/hr blended)")
            lines.append(f"  Remaining Estimated Hours: {_format_hours(fin['remaining_hours'])}")
            lines.append(f"  Projected Total Cost: {_format_currency(fin['projected_total_cost'])}")

            if fin.get("budget_consumed_pct") is not None:
                lines.append(f"  Cost Budget Consumed: {fin['budget_consumed_pct']:.1f}% of {_format_currency(fin.get('cost_budget', 0))}")

            if fin.get("projected_gm") is not None:
                lines.append(f"  **Projected GM:** {_format_currency(fin['projected_gm'])} ({_format_pct(fin.get('projected_gm_pct'))})")
            lines.append(f"  Prorated Revenue to Date: {_format_currency(fin['prorated_revenue'])}")
            if fin.get("prorated_cost_budget"):
                lines.append(f"  Prorated Cost Budget to Date: {_format_currency(fin['prorated_cost_budget'])}")
            if fin.get("current_gm") is not None:
                lines.append(f"  **Current GM:** {_format_currency(fin['current_gm'])} ({_format_pct(fin.get('current_gm_pct'))})")

        if include_projects and c.get("_projects"):
            projects = c["_projects"]
            lines.append(f"")
            lines.append(f"**Associated Projects ({len(projects)}):**")
            for p in projects:
                est = _format_hours(_safe_float(p.get("estimatedTime", 0)))
                act = _format_hours(_safe_float(p.get("actualHours", 0)))
                lines.append(f"  - {p.get('projectName', 'N/A')} (ID: {p.get('id')}) — {p.get('_statusLabel', 'N/A')} | Est: {est} hrs | Actual: {act} hrs")

        # Per-contract interpretive insights
        if fin and fin.get("contract_amount"):
            lines.append(f"")
            lines.append(f"**Key Insight:**")
            pgm_pct = fin.get("projected_gm_pct")
            target = fin.get("target_gm_pct", 60)
            bc = fin.get("budget_consumed_pct")
            if pgm_pct is not None and pgm_pct <= 0:
                lines.append(f"  ⚠ Projected to lose money — GM is {_format_pct(pgm_pct)}. Review cost allocation.")
            elif pgm_pct is not None and pgm_pct < target:
                lines.append(f"  Projected GM of {_format_pct(pgm_pct)} is below {target:.0f}% target. {_format_currency(fin.get('projected_gm', 0))} margin on {_format_currency(fin['contract_amount'])} contract.")
            elif pgm_pct is not None:
                lines.append(f"  Healthy — projected GM of {_format_pct(pgm_pct)} meets the {target:.0f}% target. {_format_currency(fin.get('projected_gm', 0))} margin.")
            if bc is not None and bc > 100:
                lines.append(f"  ⚠ Cost budget overrun: {bc:.0f}% consumed.")
            elif bc is not None and bc > 80:
                lines.append(f"  Cost budget {bc:.0f}% consumed — nearing limit.")

        lines.append("")

    return "\n".join(lines)


# ─── COMPANY SUMMARY ───────────────────────────────────────────────

def format_company_summary(company: dict, contracts: list = None, projects: list = None) -> str:
    """Format company info into an LLM-friendly summary."""
    lines = []
    lines.append(f"## Company: {company.get('companyName', 'N/A')}")
    lines.append(f"")
    lines.append(f"**ID:** {company.get('id')}")
    lines.append(f"**Active:** {company.get('isActive', 'N/A')}")
    lines.append(f"**Phone:** {company.get('phone', 'N/A')}")
    lines.append(f"**Address:** {company.get('address1', '')} {company.get('address2', '')}, {company.get('city', '')} {company.get('state', '')} {company.get('postalCode', '')}")
    lines.append(f"**Website:** {company.get('webAddress', 'N/A')}")

    if contracts:
        active_contracts = [c for c in contracts if c.get("status") == 1]
        lines.append(f"")
        lines.append(f"### Contracts ({len(contracts)} total, {len(active_contracts)} active)")
        lines.append("")
        for c in contracts:
            lines.append(f"- **{c.get('contractName', 'N/A')}** (ID: {c.get('id')}) — {c.get('_typeLabel', 'N/A')} | {c.get('_statusLabel', 'N/A')}")

    if projects:
        active_projects = [p for p in projects if p.get("status") in (1, 2)]
        lines.append(f"")
        lines.append(f"### Projects ({len(projects)} total, {len(active_projects)} active/new)")
        lines.append("")
        for p in projects:
            est = _format_hours(_safe_float(p.get("estimatedTime", 0)))
            act = _format_hours(_safe_float(p.get("actualHours", 0)))
            lines.append(f"- **{p.get('projectName', 'N/A')}** (ID: {p.get('id')}) — {p.get('_statusLabel', 'N/A')} | Est: {est} hrs | Actual: {act} hrs")

    return "\n".join(lines)


# ─── RESOURCE SUMMARY ──────────────────────────────────────────────

def format_resource_summary(resources: list) -> str:
    """Format resources into an LLM-friendly summary."""
    if not resources:
        return "No resources found matching the criteria."

    lines = []
    lines.append(f"## Resources ({len(resources)})")
    lines.append("")

    for r in resources:
        name = f"{r.get('firstName', '')} {r.get('lastName', '')}".strip()
        lines.append(f"- **{name}** (ID: {r.get('id')}) — Email: {r.get('email', 'N/A')} | Active: {r.get('isActive', 'N/A')} | Title: {r.get('title', 'N/A')}")

    return "\n".join(lines)


# ─── GENERIC ENTITY LIST ───────────────────────────────────────────

def format_entity_list(title: str, items: list, columns: list) -> str:
    """Format a generic list of entities into a readable table."""
    if not items:
        return f"No {title.lower()} found."

    lines = []
    lines.append(f"## {title} ({len(items)})")
    lines.append("")

    for item in items:
        parts = []
        for field, label in columns:
            val = item.get(field, "N/A")
            if val is None:
                val = "N/A"
            parts.append(f"{label}: {val}")
        lines.append(f"- {' | '.join(parts)}")

    return "\n".join(lines)
