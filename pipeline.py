"""
ETL Pipeline — fetch → transform → load.

Each section is wrapped in its own try/except so a single
failing endpoint never kills the whole run.

Run directly:   python pipeline.py
Or via scheduler: python scheduler.py
"""

import json
import logging
from datetime import datetime, timezone

import lc_fetcher
import hd_fetcher
from db import get_conn, create_schemas, create_tables, upsert, truncate_insert

log = logging.getLogger(__name__)


# ─── JSON helper ──────────────────────────────────────────────────────────────

def _j(obj):
    """Serialize to JSON string for JSONB columns. Returns None if obj is None."""
    if obj is None:
        return None
    return json.dumps(obj, default=str)


# ─── LiveChat Transforms ──────────────────────────────────────────────────────

def _transform_lc_groups(groups):
    return [{
        "id":            g.get("id"),
        "name":          g.get("name"),
        "language_code": g.get("language_code"),
        "raw":           _j(g),
    } for g in groups]


def _transform_lc_tags(tags):
    return [{
        "name":      t.get("name", ""),
        "group_ids": _j(t.get("group_ids")),
        "author_id": t.get("author_id"),
        "raw":       _j(t),
    } for t in tags]


def _transform_lc_chat_messages(chats):
    """Extract messages from archived chats. Handles both v2 and v3.6 formats."""
    rows = []
    seen = set()
    for c in chats:
        chat_id = c.get("id", "")

        # v2 API: messages array directly on chat
        if "started_timestamp" in c:
            for msg in c.get("messages", []):
                event_id = f"{chat_id}_{msg.get('event_id', msg.get('timestamp_us', ''))}"
                if event_id in seen:
                    continue
                seen.add(event_id)
                ts = datetime.utcfromtimestamp(msg["timestamp"]).isoformat() + "Z" if msg.get("timestamp") else None
                rows.append({
                    "id":         event_id,
                    "chat_id":    chat_id,
                    "thread_id":  chat_id,
                    "created_at": ts,
                    "type":       msg.get("type", "message"),
                    "author_id":  msg.get("agent_id") or msg.get("user_type"),
                    "visibility": "all",
                    "text":       msg.get("text"),
                    "fields":     _j(msg.get("message_json")),
                    "raw":        _j(msg),
                })
        else:
            # v3.6 API: events inside thread
            thread    = c.get("thread") or {}
            thread_id = thread.get("id", "")
            for event in thread.get("events", []):
                event_id = event.get("id", "")
                if not event_id or event_id in seen:
                    continue
                seen.add(event_id)
                rows.append({
                    "id":         event_id,
                    "chat_id":    chat_id,
                    "thread_id":  thread_id,
                    "created_at": event.get("created_at"),
                    "type":       event.get("type"),
                    "author_id":  event.get("author_id"),
                    "visibility": event.get("visibility"),
                    "text":       event.get("text"),
                    "fields":     _j(event.get("fields")),
                    "raw":        _j(event),
                })
    return rows


def _transform_lc_daily_ratings(data):
    rows = []
    for date_str, vals in data.get("records", {}).items():
        rows.append({
            "date":  date_str,
            "chats": vals.get("chats", 0),
            "good":  vals.get("good", 0),
            "bad":   vals.get("bad", 0),
            "raw":   _j(vals),
        })
    return rows


def _transform_lc_daily_first_response_time(data):
    rows = []
    for date_str, vals in data.get("records", {}).items():
        rows.append({
            "date":                     date_str,
            "count":                    vals.get("count", 0),
            "first_response_time_secs": vals.get("first_response_time"),
            "raw":                      _j(vals),
        })
    return rows


def _transform_lc_daily_response_time(data):
    rows = []
    for date_str, vals in data.get("records", {}).items():
        rows.append({
            "date":               date_str,
            "count":              vals.get("count", 0),
            "response_time_secs": vals.get("response_time"),
            "raw":                _j(vals),
        })
    return rows


def _transform_lc_agent_performance(data):
    rows = []
    for agent_email, vals in data.get("records", {}).items():
        rows.append({
            "agent_email":                agent_email,
            "accepting_chats_time":       vals.get("accepting_chats_time"),
            "chatting_time":              vals.get("chatting_time"),
            "logged_in_time":             vals.get("logged_in_time"),
            "not_accepting_chats_time":   vals.get("not_accepting_chats_time"),
            "chats_count":                vals.get("chats_count"),
            "chats_rated_good":           vals.get("chats_rated_good"),
            "chats_rated_bad":            vals.get("chats_rated_bad"),
            "first_response_time_secs":   vals.get("first_response_time"),
            "first_response_chats_count": vals.get("first_response_chats_count"),
            "raw":                        _j(vals),
        })
    return rows


def _transform_lc_agents(agents):
    rows = []
    for a in agents:
        rows.append({
            "id":           a.get("id", ""),
            "name":         a.get("name"),
            "email":        a.get("email"),
            "role":         a.get("role"),
            "login_status": a.get("login_status"),
            "avatar_url":   a.get("avatar"),
            "groups":       _j(a.get("groups")),
            "raw":          _j(a),
        })
    return rows


def _transform_lc_chats(chats):
    rows = []
    for c in chats:
        rows.append({
            "id":         c.get("id", ""),
            "users":      _j(c.get("users")),
            "properties": _j(c.get("properties")),
            "access":     _j(c.get("access")),
            "raw":        _j(c),
        })
    return rows


def _transform_lc_archives(chats):
    """Handles both v2 API format and v3.6 format."""
    rows = []
    for c in chats:
        # v2 API format: started_timestamp, ended_timestamp, visitor, agents, messages
        if "started_timestamp" in c:
            started = datetime.utcfromtimestamp(c["started_timestamp"]).isoformat() + "Z" if c.get("started_timestamp") else None
            ended   = datetime.utcfromtimestamp(c["ended_timestamp"]).isoformat() + "Z" if c.get("ended_timestamp") else None
            users   = {"agents": c.get("agents", []), "customers": [c.get("visitor", {})]}
            rows.append({
                "id":         c.get("id", ""),
                "thread_id":  c.get("id", ""),
                "created_at": started,
                "ended_at":   ended,
                "users":      _j(users),
                "properties": _j(c.get("custom_variables")),
                "tags":       _j(c.get("tags")),
                "raw":        _j(c),
            })
        else:
            # v3.6 API format
            thread = c.get("thread") or {}
            rows.append({
                "id":         c.get("id", ""),
                "thread_id":  thread.get("id"),
                "created_at": thread.get("created_at"),
                "ended_at":   thread.get("ended_at"),
                "users":      _j(c.get("users")),
                "properties": _j(c.get("properties")),
                "tags":       _j(thread.get("tags")),
                "raw":        _j(c),
            })
    return rows


def _transform_lc_customers(customers):
    rows = []
    for c in customers:
        stats = c.get("statistics") or {}
        last  = c.get("last_visit")  or {}
        rows.append({
            "id":                   c.get("id", ""),
            "name":                 c.get("name"),
            "email":                c.get("email"),
            "last_visit_url":       last.get("ip"),
            "visited_pages_count":  stats.get("visited_pages_count"),
            "chats_count":          stats.get("chats_count"),
            "raw":                  _j(c),
        })
    return rows


def _transform_lc_daily_reports(data):
    # records is a dict: {"2026-02-16": {"total": 78}, ...}
    rows = []
    for date_str, vals in data.get("records", {}).items():
        if not date_str:
            continue
        rows.append({
            "date":        date_str,
            "total_chats": vals.get("total", 0),
            "raw":         _j(vals),
        })
    return rows


# ─── HelpDesk Transforms ──────────────────────────────────────────────────────

def _hd_id(obj):
    """HelpDesk uses capitalised 'ID' key."""
    return str(obj.get("ID") or obj.get("id") or "")

def _hd_name(name_field):
    """Name can be a plain string or {first, last} dict."""
    if isinstance(name_field, dict):
        return f"{name_field.get('first', '')} {name_field.get('last', '')}".strip()
    return name_field


def _transform_hd_agents(agents):
    return [{
        "id":         _hd_id(a),
        "name":       _hd_name(a.get("name", "")),
        "email":      a.get("email"),
        "role":       a.get("role"),
        "deleted_at": a.get("deletedAt"),
        "raw":        _j(a),
    } for a in agents]


def _transform_hd_teams(teams):
    return [{
        "id":   _hd_id(t),
        "name": t.get("name"),
        "raw":  _j(t),
    } for t in teams]


def _transform_hd_mailboxes(mailboxes):
    return [{
        "id":      _hd_id(m),
        "name":    m.get("name"),
        "email":   m.get("email"),
        "enabled": m.get("enabled"),
        "raw":     _j(m),
    } for m in mailboxes]


def _transform_hd_tags(tags):
    return [{
        "id":    _hd_id(t),
        "name":  t.get("name"),
        "color": t.get("color"),
        "raw":   _j(t),
    } for t in tags]


def _transform_hd_integrations(items):
    return [{
        "id":         _hd_id(i),
        "type":       i.get("type"),
        "created_at": i.get("createdAt"),
        "raw":        _j(i),
    } for i in items]


def _transform_hd_tickets(tickets):
    rows = []
    seen = set()
    for t in tickets:
        tid = _hd_id(t)
        if not tid or tid in seen:
            continue
        seen.add(tid)

        req        = t.get("requester")  or {}
        assignment = t.get("assignment") or {}
        assignee   = assignment.get("agent") or {}

        rows.append({
            "id":               tid,
            "short_id":         t.get("shortID"),
            "status":           t.get("status"),
            "priority":         t.get("priority"),
            "subject":          t.get("subject"),
            "created_at":       t.get("createdAt"),
            "updated_at":       t.get("updatedAt"),
            "last_message_at":  t.get("lastMessageAt"),
            "created_by":       t.get("createdBy"),
            "created_by_type":  t.get("createdByType"),
            "requester_email":  req.get("email"),
            "requester_name":   req.get("name"),
            "assignee_id":      assignee.get("id"),
            "assignee_email":   assignee.get("email"),
            "team_ids":         _j(t.get("teamIDs")),
            "tag_ids":          _j(t.get("tagIDs")),
            "source":           (t.get("source") or {}).get("type"),
            "spam":             (t.get("spam") or {}).get("status") if isinstance(t.get("spam"), dict) else t.get("spam"),
            "rating":           _j(t.get("rating")),
            "raw":              _j(t),
        })
    return rows


def _transform_hd_canned_responses(items):
    return [{
        "id":       _hd_id(r),
        "name":     r.get("name"),
        "shortcut": r.get("shortcut"),
        "text":     r.get("text"),
        "raw":      _j(r),
    } for r in items]


def _transform_hd_rules(items):
    return [{
        "id":      _hd_id(r),
        "name":    r.get("name"),
        "enabled": r.get("enabled"),
        "raw":     _j(r),
    } for r in items]


def _transform_hd_macros(items):
    return [{
        "id":   _hd_id(m),
        "name": m.get("name"),
        "raw":  _j(m),
    } for m in items]


def _transform_hd_views(items):
    return [{
        "id":   _hd_id(v),
        "name": v.get("name"),
        "raw":  _j(v),
    } for v in items]


def _transform_hd_webhooks(items):
    return [{
        "id":  _hd_id(w),
        "url": w.get("url"),
        "raw": _j(w),
    } for w in items]


def _transform_hd_ticket_messages(tickets):
    """Extract all events from tickets into individual message rows."""
    rows = []
    seen = set()
    for t in tickets:
        ticket_id = _hd_id(t)
        for event in t.get("events", []):
            # HD event IDs are integers (index), make composite key
            event_id  = f"{ticket_id}_{event.get('ID', 0)}"
            if event_id in seen:
                continue
            seen.add(event_id)
            author  = event.get("author") or {}
            message = event.get("message") or {}
            source  = event.get("source")  or {}
            rows.append({
                "id":           event_id,
                "ticket_id":    ticket_id,
                "type":         event.get("type"),
                "date":         event.get("date"),
                "author_type":  author.get("type"),
                "author_email": author.get("email"),
                "message_text": message.get("text") if isinstance(message, dict) else None,
                "is_private":   message.get("isPrivate") if isinstance(message, dict) else None,
                "source_type":  source.get("type"),
                "raw":          _j(event),
            })
    return rows


def _transform_hd_daily_reports(new_t, resp_t, resol_t):
    all_dates = set(new_t.keys()) | set(resp_t.keys()) | set(resol_t.keys())
    rows = []
    for date in all_dates:
        nt = new_t.get(date,   {})
        rt = resp_t.get(date,  {})
        rs = resol_t.get(date, {})
        rows.append({
            "date":                             date,
            "new_tickets":                      nt.get("newTickets", 0),
            "sum_seconds_to_response":          rt.get("sumSecondsToResponse", 0),
            "sum_seconds_to_assignment":        rt.get("sumSecondsToAssignment", 0),
            "response_count":                   rt.get("count", 0),
            "sum_seconds_from_creation":        rs.get("sumSecondsFromCreation", 0),
            "sum_seconds_from_assignment":      rs.get("sumSecondsFromAssignment", 0),
            "resolution_count_from_creation":   rs.get("countFromCreation", 0),
            "resolution_count_from_assignment": rs.get("countFromAssignment", 0),
            "resolution_count":                 rs.get("count", 0),
        })
    return rows


# ─── Pipeline Runner ──────────────────────────────────────────────────────────

def _safe(label, fn):
    """Run fn(), log success or error. Never raises."""
    try:
        fn()
    except Exception as e:
        log.error(f"[{label}] FAILED: {e}", exc_info=True)


def run():
    started = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info(f"Pipeline run — {started.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log.info("=" * 60)

    conn = get_conn()
    try:
        create_schemas(conn)
        create_tables(conn)

        # ── LiveChat ──────────────────────────────────────────────────────────
        log.info("--- LiveChat ---")

        def lc_agents():
            upsert(conn, "livechat.agents",
                   _transform_lc_agents(lc_fetcher.fetch_agents()))

        def lc_chats():
            upsert(conn, "livechat.chats",
                   _transform_lc_chats(lc_fetcher.fetch_chats()))

        def lc_archives():
            upsert(conn, "livechat.archives",
                   _transform_lc_archives(lc_fetcher.fetch_archives()))

        def lc_customers():
            rows = _transform_lc_customers(lc_fetcher.fetch_customers())
            if rows:
                truncate_insert(conn, "livechat.customers", rows)

        def lc_reports():
            upsert(conn, "livechat.daily_reports",
                   _transform_lc_daily_reports(lc_fetcher.fetch_daily_reports(days_back=3)),
                   conflict_col="date")

        def lc_groups():
            upsert(conn, "livechat.groups",
                   _transform_lc_groups(lc_fetcher.fetch_groups()))

        def lc_tags():
            upsert(conn, "livechat.tags",
                   _transform_lc_tags(lc_fetcher.fetch_tags()),
                   conflict_col="name")

        # Fetch archives once, reuse for both archives and chat_messages
        _archives_cache = []

        def lc_archives_and_messages():
            nonlocal _archives_cache
            _archives_cache = lc_fetcher.fetch_archives()
            upsert(conn, "livechat.archives",
                   _transform_lc_archives(_archives_cache))
            msgs = _transform_lc_chat_messages(_archives_cache)
            if msgs:
                upsert(conn, "livechat.chat_messages", msgs)

        def lc_ratings():
            upsert(conn, "livechat.daily_ratings",
                   _transform_lc_daily_ratings(lc_fetcher.fetch_report_ratings(days_back=3)),
                   conflict_col="date")

        def lc_first_response():
            upsert(conn, "livechat.daily_first_response_time",
                   _transform_lc_daily_first_response_time(
                       lc_fetcher.fetch_report_first_response_time(days_back=3)),
                   conflict_col="date")

        def lc_response_time():
            upsert(conn, "livechat.daily_response_time",
                   _transform_lc_daily_response_time(
                       lc_fetcher.fetch_report_response_time(days_back=3)),
                   conflict_col="date")

        def lc_agent_perf():
            upsert(conn, "livechat.agent_performance",
                   _transform_lc_agent_performance(
                       lc_fetcher.fetch_report_agent_performance(days_back=3)),
                   conflict_col="agent_email")

        _safe("LC groups",              lc_groups)
        _safe("LC tags",                lc_tags)
        _safe("LC agents",              lc_agents)
        _safe("LC chats",               lc_chats)
        _safe("LC archives+messages",   lc_archives_and_messages)
        _safe("LC customers",           lc_customers)
        _safe("LC daily reports",       lc_reports)
        _safe("LC ratings",             lc_ratings)
        _safe("LC first response time", lc_first_response)
        _safe("LC response time",       lc_response_time)
        _safe("LC agent performance",   lc_agent_perf)

        # ── HelpDesk ──────────────────────────────────────────────────────────
        log.info("--- HelpDesk ---")

        def hd_agents():
            upsert(conn, "helpdesk.agents",
                   _transform_hd_agents(hd_fetcher.fetch_agents()))

        def hd_teams():
            upsert(conn, "helpdesk.teams",
                   _transform_hd_teams(hd_fetcher.fetch_teams()))

        def hd_mailboxes():
            upsert(conn, "helpdesk.mailboxes",
                   _transform_hd_mailboxes(hd_fetcher.fetch_mailboxes()))

        def hd_tags():
            upsert(conn, "helpdesk.tags",
                   _transform_hd_tags(hd_fetcher.fetch_tags()))

        def hd_integrations():
            upsert(conn, "helpdesk.integrations",
                   _transform_hd_integrations(hd_fetcher.fetch_integrations()))

        # Fetch tickets once, reuse for tickets + ticket_messages
        _tickets_cache = []

        def hd_tickets():
            nonlocal _tickets_cache
            _tickets_cache = hd_fetcher.fetch_all_tickets()
            upsert(conn, "helpdesk.tickets",
                   _transform_hd_tickets(_tickets_cache))

        def hd_ticket_messages():
            if _tickets_cache:
                msgs = _transform_hd_ticket_messages(_tickets_cache)
                if msgs:
                    upsert(conn, "helpdesk.ticket_messages", msgs)

        def hd_canned():
            upsert(conn, "helpdesk.canned_responses",
                   _transform_hd_canned_responses(hd_fetcher.fetch_canned_responses()))

        def hd_rules():
            upsert(conn, "helpdesk.rules",
                   _transform_hd_rules(hd_fetcher.fetch_rules()))

        def hd_macros():
            upsert(conn, "helpdesk.macros",
                   _transform_hd_macros(hd_fetcher.fetch_macros()))

        def hd_views():
            upsert(conn, "helpdesk.views",
                   _transform_hd_views(hd_fetcher.fetch_views()))

        def hd_webhooks():
            upsert(conn, "helpdesk.webhooks",
                   _transform_hd_webhooks(hd_fetcher.fetch_webhooks()))

        def hd_reports():
            new_t, resp_t, resol_t = hd_fetcher.fetch_reports()
            upsert(conn, "helpdesk.daily_reports",
                   _transform_hd_daily_reports(new_t, resp_t, resol_t),
                   conflict_col="date")

        _safe("HD agents",       hd_agents)
        _safe("HD teams",        hd_teams)
        _safe("HD mailboxes",    hd_mailboxes)
        _safe("HD tags",         hd_tags)
        _safe("HD integrations", hd_integrations)
        _safe("HD tickets",          hd_tickets)
        _safe("HD ticket messages",  hd_ticket_messages)
        _safe("HD canned resp",  hd_canned)
        _safe("HD rules",        hd_rules)
        _safe("HD macros",       hd_macros)
        _safe("HD views",        hd_views)
        _safe("HD webhooks",     hd_webhooks)
        _safe("HD daily reports", hd_reports)

        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        log.info(f"Pipeline complete in {elapsed:.1f}s")

    finally:
        conn.close()


# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    run()
