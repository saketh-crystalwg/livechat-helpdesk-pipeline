"""
LiveChat API fetcher.
Pulls raw data from all relevant endpoints.
No DB logic here — just HTTP calls that return Python objects.

v3.6 API: agents, groups, tags, reports, active chats
v2 API:   archives (full history with messages) — v3.6 list_archives pagination is broken
"""

import base64
import logging
from datetime import datetime, timedelta

import requests
from config import LC_PAT_B64, LC_API_BASE

log = logging.getLogger(__name__)

# ─── HTTP Helpers ─────────────────────────────────────────────────────────────

def _headers():
    return {
        "Authorization": f"Basic {LC_PAT_B64}",
        "Content-Type": "application/json",
    }

# v2 API uses account_id:pat_token Basic auth
_LC_ACCOUNT_ID = base64.b64decode(LC_PAT_B64).decode().split(":")[0]
_LC_PAT_TOKEN  = ":".join(base64.b64decode(LC_PAT_B64).decode().split(":")[1:])
_LC_V2_AUTH    = base64.b64encode(f"{_LC_ACCOUNT_ID}:{_LC_PAT_TOKEN}".encode()).decode()

def _v2_headers():
    return {"Authorization": f"Basic {_LC_V2_AUTH}"}

def _agent_action(action, payload=None):
    url = f"{LC_API_BASE}/agent/action/{action}"
    r = requests.post(url, headers=_headers(), json=payload or {}, timeout=30)
    r.raise_for_status()
    return r.json()

def _conf_action(action, payload=None):
    url = f"{LC_API_BASE}/configuration/action/{action}"
    r = requests.post(url, headers=_headers(), json=payload or {}, timeout=30)
    r.raise_for_status()
    return r.json()

def _report_post(path, payload=None):
    url = f"{LC_API_BASE}/reports/{path}"
    r = requests.post(url, headers=_headers(), json=payload or {}, timeout=30)
    r.raise_for_status()
    return r.json()


# ─── Fetchers ─────────────────────────────────────────────────────────────────

def fetch_agents():
    """All agents and their current login status."""
    data = _conf_action("list_agents")
    agents = data if isinstance(data, list) else data.get("agents", [])
    log.info(f"LC agents: {len(agents)}")
    return agents


def fetch_chats(limit=100):
    """Active / ongoing chats (snapshot)."""
    data  = _agent_action("list_chats", {"sort_order": "desc", "limit": limit})
    chats = data.get("chats", [])
    log.info(f"LC active chats: {len(chats)}")
    return chats


def fetch_archives(page_size=100, max_chats=5000):
    """
    Fetch archived chats via the v2 API which supports proper pagination
    and includes full message threads in each chat object.
    Capped at max_chats per run (default 5,000) to avoid very long fetches.
    """
    all_chats = []
    page      = 1

    while len(all_chats) < max_chats:
        r = requests.get(
            "https://api.livechatinc.com/v2/chats",
            headers=_v2_headers(),
            params={"count": page_size, "page": page},
            timeout=30,
        )
        r.raise_for_status()
        data  = r.json()
        chats = data.get("chats", [])
        if not chats:
            break
        all_chats.extend(chats)
        if len(all_chats) >= data.get("total", 0):
            break
        page += 1

    log.info(f"LC archives (v2): {len(all_chats)} chats fetched")
    return all_chats


def fetch_archives_range(date_from=None, date_to=None, page_size=100, max_chats=None):
    """
    Fetch archived chats for a specific date range via v2 API.
    Used by the backfill script.
    """
    all_chats = []
    page      = 1
    params    = {"count": page_size, "page": page}
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to

    while True:
        params["page"] = page
        r = requests.get(
            "https://api.livechatinc.com/v2/chats",
            headers=_v2_headers(),
            params=params,
            timeout=30,
        )
        r.raise_for_status()
        data  = r.json()
        chats = data.get("chats", [])
        if not chats:
            break
        all_chats.extend(chats)
        total = data.get("total", 0)
        if len(all_chats) >= total or len(chats) < page_size:
            break
        if max_chats and len(all_chats) >= max_chats:
            break
        page += 1

    log.info(f"LC archives_range (v2): {len(all_chats)} chats | {date_from} → {date_to}")
    return all_chats


def fetch_customers(limit=100):
    """Current visitors on site (real-time snapshot)."""
    try:
        data      = _agent_action("list_customers", {"limit": limit})
        customers = data.get("customers", [])
        log.info(f"LC customers: {len(customers)}")
        return customers
    except requests.HTTPError as e:
        if e.response.status_code in (401, 403, 422):
            log.warning("LC customers: not available on this account/scope — skipping")
            return []
        raise


def fetch_groups():
    """All agent groups / departments."""
    data = _conf_action("list_groups")
    groups = data if isinstance(data, list) else data.get("groups", [])
    log.info(f"LC groups: {len(groups)}")
    return groups


def fetch_tags():
    """All chat tags."""
    data = _conf_action("list_tags")
    tags = data if isinstance(data, list) else data.get("tags", [])
    log.info(f"LC tags: {len(tags)}")
    return tags


def _date_range(days_back):
    date_to   = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    date_from = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"filters": {"from": date_from, "to": date_to}, "distribution": "day"}


def fetch_daily_reports(days_back=30):
    """Chat volume per day for the last N days."""
    data = _report_post("chats/total_chats", _date_range(days_back))
    log.info(f"LC daily reports: {len(data.get('records', {}))} days")
    return data


def fetch_report_ratings(days_back=30):
    """CSAT ratings per day."""
    data = _report_post("chats/ratings", _date_range(days_back))
    log.info(f"LC ratings report: {len(data.get('records', {}))} days")
    return data


def fetch_report_first_response_time(days_back=30):
    """First response time per day."""
    data = _report_post("chats/first_response_time", _date_range(days_back))
    log.info(f"LC first response time: {len(data.get('records', {}))} days")
    return data


def fetch_report_response_time(days_back=30):
    """Response time per day."""
    data = _report_post("chats/response_time", _date_range(days_back))
    log.info(f"LC response time: {len(data.get('records', {}))} days")
    return data


def fetch_report_agent_performance(days_back=7):
    """Per-agent performance metrics over the last N days."""
    data = _report_post("agents/performance", _date_range(days_back))
    records = data.get("records", {})
    log.info(f"LC agent performance: {len(records)} agents")
    return data
