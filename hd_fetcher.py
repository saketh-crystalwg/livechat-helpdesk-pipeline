"""
HelpDesk API fetcher.
Pulls raw data from all relevant endpoints, handling pagination automatically.
No DB logic here — just HTTP calls that return Python objects.
"""

import logging

import requests
from config import HD_PAT_B64, HD_API_BASE

log = logging.getLogger(__name__)

# ─── HTTP Helpers ─────────────────────────────────────────────────────────────

def _headers():
    return {
        "Authorization": f"Basic {HD_PAT_B64}",
        "Content-Type":  "application/json",
        "User-Agent":    "HelpDeskPipeline/1.0",
    }

def _get(path, params=None):
    r = requests.get(f"{HD_API_BASE}{path}", headers=_headers(),
                     params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json(), r.headers

def _get_all_pages(path, page_size=100, extra_params=None):
    """
    Fetch every page from a paginated HelpDesk endpoint.
    Pagination info comes from response headers (x-total-pages).
    """
    all_items = []
    page      = 1

    while True:
        params = {"pageSize": page_size, "page": page}
        if extra_params:
            params.update(extra_params)

        data, headers = _get(path, params=params)
        items = data if isinstance(data, list) else []
        all_items.extend(items)

        total_pages = int(headers.get("x-total-pages", 1))
        if page >= total_pages or not items:
            break
        page += 1

    return all_items


# ─── Fetchers ─────────────────────────────────────────────────────────────────

def fetch_agents():
    data, _ = _get("/agents")
    log.info(f"HD agents: {len(data)}")
    return data

def fetch_teams():
    data, _ = _get("/teams")
    log.info(f"HD teams: {len(data)}")
    return data

def fetch_mailboxes():
    data, _ = _get("/mailboxes")
    log.info(f"HD mailboxes: {len(data)}")
    return data

def fetch_tags():
    data, _ = _get("/tags")
    log.info(f"HD tags: {len(data)}")
    return data

def fetch_integrations():
    data, _ = _get("/integrations")
    log.info(f"HD integrations: {len(data)}")
    return data

def fetch_all_tickets():
    """
    Fetch all tickets across all three statuses with full pagination.
    Deduplication happens in the transform layer.
    """
    all_tickets = []
    for status in ["open", "pending", "closed", "solved"]:
        items = _get_all_pages("/tickets", extra_params={"status": status})
        log.info(f"HD tickets ({status}): {len(items)}")
        all_tickets.extend(items)
    return all_tickets

def fetch_canned_responses():
    data, _ = _get("/cannedResponses")
    log.info(f"HD canned responses: {len(data)}")
    return data

def fetch_rules():
    data, _ = _get("/rules")
    log.info(f"HD rules: {len(data)}")
    return data

def fetch_macros():
    data, _ = _get("/macros")
    log.info(f"HD macros: {len(data)}")
    return data

def fetch_views():
    data, _ = _get("/views")
    log.info(f"HD views: {len(data)}")
    return data

def fetch_webhooks():
    data, _ = _get("/webhooks")
    log.info(f"HD webhooks: {len(data)}")
    return data

def fetch_reports():
    """
    Returns today's metrics from three report endpoints.
    Returns: (new_tickets_dict, response_time_dict, resolution_time_dict)
    """
    new_t,   _ = _get("/reports/newTickets")
    resp_t,  _ = _get("/reports/responseTime")
    resol_t, _ = _get("/reports/resolutionTime")
    log.info("HD daily reports: fetched")
    return new_t, resp_t, resol_t
