"""
Historical backfill script.

Fetches as much historical data as the APIs support and loads it into PostgreSQL.
Safe to run multiple times — all writes are upserts.

Usage:
    python3 backfill.py              # default: 365 days back
    python3 backfill.py --days 730   # go 2 years back

What gets backfilled:
  LiveChat:
    - daily_reports          (total chats per day)
    - daily_ratings          (CSAT per day)
    - daily_first_response_time
    - daily_response_time
    - agent_performance      (aggregate over the full period)

  HelpDesk:
    - daily_reports          (API only returns today — no historical support)
    - All other tables are already fully synced (not date-limited)

Note: LiveChat archives only exposes the 10 most recent chats via the API
(agent-scoped). There is no way to backfill further via the current PAT scope.
"""

import argparse
import base64
import logging
import sys
from datetime import datetime, timedelta, timezone

import requests

from config import LC_PAT_B64, LC_API_BASE
from db import get_conn, create_schemas, create_tables, upsert
from pipeline import (
    _transform_lc_archives,
    _transform_lc_chat_messages,
    _transform_lc_daily_reports,
    _transform_lc_daily_ratings,
    _transform_lc_daily_first_response_time,
    _transform_lc_daily_response_time,
    _transform_lc_agent_performance,
)

# v2 API auth
_LC_ACCOUNT_ID = base64.b64decode(LC_PAT_B64).decode().split(":")[0]
_LC_PAT_TOKEN  = ":".join(base64.b64decode(LC_PAT_B64).decode().split(":")[1:])
_LC_V2_AUTH    = base64.b64encode(f"{_LC_ACCOUNT_ID}:{_LC_PAT_TOKEN}".encode()).decode()
_V2_HEADERS    = {"Authorization": f"Basic {_LC_V2_AUTH}"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("backfill")


# ─── LC HTTP Helper ───────────────────────────────────────────────────────────

def _lc_report(path, payload):
    url = f"{LC_API_BASE}/reports/{path}"
    r = requests.post(
        url,
        headers={"Authorization": f"Basic {LC_PAT_B64}", "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def _date_payload(date_from, date_to):
    return {
        "filters":      {"from": date_from, "to": date_to},
        "distribution": "day",
    }


# ─── Chunked Fetcher ─────────────────────────────────────────────────────────

def fetch_in_chunks(report_path, total_days, chunk_days=90):
    """
    Fetch a date-based report in chunks to avoid API limits.
    Returns merged records dict {date: values}.
    """
    all_records = {}
    now = datetime.utcnow()

    chunks = range(0, total_days, chunk_days)
    total_chunks = len(list(chunks))

    for i, offset in enumerate(range(0, total_days, chunk_days)):
        chunk_end   = now - timedelta(days=offset)
        chunk_start = now - timedelta(days=min(offset + chunk_days, total_days))

        date_to   = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_from = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")

        log.info(f"  [{i+1}/{total_chunks}] {date_from[:10]} → {date_to[:10]}")

        try:
            data = _lc_report(report_path, _date_payload(date_from, date_to))
            records = data.get("records", {})
            all_records.update(records)
            log.info(f"    → {len(records)} days fetched (total so far: {len(all_records)})")
        except requests.HTTPError as e:
            log.warning(f"    → HTTP {e.response.status_code}: {e.response.text[:200]} — skipping chunk")

    return all_records


# ─── Archive Backfill (v2 API) ───────────────────────────────────────────────

def _fetch_chunk(date_from, date_to, existing_ids, page_size=25):
    """Fetch one date-range chunk, return (new_chats, skip_count)."""
    page       = 1
    new_chats  = []
    skip_count = 0
    while True:
        r = requests.get(
            "https://api.livechatinc.com/v2/chats",
            headers=_V2_HEADERS,
            params={"count": page_size, "page": page,
                    "date_from": date_from, "date_to": date_to},
            timeout=60,
        )
        if r.status_code == 400:
            log.warning(f"  400 at page {page} ({date_from} → {date_to}) — stopping chunk")
            break
        r.raise_for_status()
        data  = r.json()
        chats = data.get("chats", [])
        if not chats:
            break
        for c in chats:
            if c.get("id") not in existing_ids:
                new_chats.append(c)
            else:
                skip_count += 1
        total = data.get("total", 0)
        if len(new_chats) + skip_count >= total or len(chats) < page_size:
            break
        page += 1
    return new_chats, skip_count


def backfill_archives(conn, total_days=730):
    """
    Fetch all archived chats via v2 API using monthly date chunks.
    Bypasses the 1000-page API limit. Resumable — skips chats already in DB.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM livechat.archives")
        existing_ids = {row[0] for row in cur.fetchall()}
    log.info(f"Archive backfill: {len(existing_ids):,} chats already in DB")

    total_new  = 0
    total_skip = 0
    now        = datetime.utcnow()

    # Walk backwards month by month
    chunk_end = now
    while (now - chunk_end).days < total_days:
        chunk_start = chunk_end - timedelta(days=30)
        date_to   = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_from = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")

        log.info(f"Chunk: {date_from[:10]} → {date_to[:10]}")
        new_chats, skip_count = _fetch_chunk(date_from, date_to, existing_ids)

        if new_chats:
            upsert(conn, "livechat.archives",      _transform_lc_archives(new_chats))
            upsert(conn, "livechat.chat_messages", _transform_lc_chat_messages(new_chats))
            for c in new_chats:
                existing_ids.add(c.get("id"))
            total_new += len(new_chats)

        total_skip += skip_count
        log.info(f"  +{len(new_chats)} new | {skip_count} skipped | {len(existing_ids):,} total in DB")

        chunk_end = chunk_start

    log.info(f"Archive backfill done: {total_new:,} new | {total_skip:,} skipped")


# ─── Backfill Runner ─────────────────────────────────────────────────────────

def run_backfill(days_back: int):
    started = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info(f"Backfill starting — {days_back} days — {started.strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 60)

    conn = get_conn()
    try:
        create_schemas(conn)
        create_tables(conn)

        # ── LiveChat daily_reports ─────────────────────────────────────────────
        log.info(f"\n[LC] Fetching daily_reports ({days_back} days)...")
        records = fetch_in_chunks("chats/total_chats", days_back)
        rows = _transform_lc_daily_reports({"records": records})
        upsert(conn, "livechat.daily_reports", rows, conflict_col="date")

        # ── LiveChat daily_ratings (CSAT) ──────────────────────────────────────
        log.info(f"\n[LC] Fetching daily_ratings ({days_back} days)...")
        records = fetch_in_chunks("chats/ratings", days_back)
        rows = _transform_lc_daily_ratings({"records": records})
        upsert(conn, "livechat.daily_ratings", rows, conflict_col="date")

        # ── LiveChat daily_first_response_time ────────────────────────────────
        log.info(f"\n[LC] Fetching daily_first_response_time ({days_back} days)...")
        records = fetch_in_chunks("chats/first_response_time", days_back)
        rows = _transform_lc_daily_first_response_time({"records": records})
        upsert(conn, "livechat.daily_first_response_time", rows, conflict_col="date")

        # ── LiveChat daily_response_time ──────────────────────────────────────
        log.info(f"\n[LC] Fetching daily_response_time ({days_back} days)...")
        records = fetch_in_chunks("chats/response_time", days_back)
        rows = _transform_lc_daily_response_time({"records": records})
        upsert(conn, "livechat.daily_response_time", rows, conflict_col="date")

        # ── LiveChat agent_performance (aggregate over full period) ────────────
        log.info(f"\n[LC] Fetching agent_performance (aggregate over {days_back} days)...")
        now = datetime.utcnow()
        date_to   = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_from = (now - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            data = _lc_report("agents/performance", _date_payload(date_from, date_to))
            rows = _transform_lc_agent_performance(data)
            upsert(conn, "livechat.agent_performance", rows, conflict_col="agent_email")
        except requests.HTTPError as e:
            log.warning(f"  agent_performance failed: {e.response.status_code}")

        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        log.info(f"\nBackfill complete in {elapsed:.1f}s")

    finally:
        conn.close()


# ─── Entry Point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill historical data into PostgreSQL")
    parser.add_argument(
        "--days", type=int, default=365,
        help="How many days back to fetch reports (default: 365)"
    )
    parser.add_argument(
        "--no-archives", action="store_true",
        help="Skip the LiveChat archive backfill (only run report backfill)"
    )
    parser.add_argument(
        "--archives-only", action="store_true",
        help="Only run the LiveChat archive backfill, skip report backfill"
    )
    args = parser.parse_args()

    if not args.archives_only:
        if args.days < 1 or args.days > 1825:
            print("--days must be between 1 and 1825 (5 years)")
            sys.exit(1)
        run_backfill(args.days)

    if not args.no_archives:
        conn = get_conn()
        try:
            create_schemas(conn)
            create_tables(conn)
            backfill_archives(conn)
        finally:
            conn.close()
