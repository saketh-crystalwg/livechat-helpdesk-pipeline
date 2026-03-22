"""
Database layer — connection, schema/table creation, upsert helpers.

Schemas:
  livechat.*   — LiveChat data
  helpdesk.*   — HelpDesk data
"""

import logging
import psycopg2
import psycopg2.extras
from config import DATABASE_URL

log = logging.getLogger(__name__)


# ─── Connection ───────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DATABASE_URL)


# ─── Schema & Table Creation ──────────────────────────────────────────────────

def create_schemas(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS livechat;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS helpdesk;")
    conn.commit()
    log.info("Schemas ready.")


def create_tables(conn):
    ddl = """
    -- ── LiveChat ────────────────────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS livechat.groups (
        id             INTEGER PRIMARY KEY,
        name           TEXT,
        language_code  TEXT,
        raw            JSONB,
        synced_at      TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS livechat.tags (
        name       TEXT PRIMARY KEY,
        group_ids  JSONB,
        author_id  TEXT,
        raw        JSONB,
        synced_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS livechat.chat_messages (
        id          TEXT PRIMARY KEY,
        chat_id     TEXT,
        thread_id   TEXT,
        created_at  TIMESTAMPTZ,
        type        TEXT,
        author_id   TEXT,
        visibility  TEXT,
        text        TEXT,
        fields      JSONB,
        raw         JSONB,
        synced_at   TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS livechat.daily_ratings (
        date       DATE PRIMARY KEY,
        chats      INTEGER,
        good       INTEGER,
        bad        INTEGER,
        raw        JSONB,
        synced_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS livechat.daily_first_response_time (
        date                       DATE PRIMARY KEY,
        count                      INTEGER,
        first_response_time_secs   NUMERIC,
        raw                        JSONB,
        synced_at                  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS livechat.daily_response_time (
        date                  DATE PRIMARY KEY,
        count                 INTEGER,
        response_time_secs    NUMERIC,
        raw                   JSONB,
        synced_at             TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS livechat.agent_performance (
        agent_email                  TEXT PRIMARY KEY,
        accepting_chats_time         INTEGER,
        chatting_time                INTEGER,
        logged_in_time               INTEGER,
        not_accepting_chats_time     INTEGER,
        chats_count                  INTEGER,
        chats_rated_good             INTEGER,
        chats_rated_bad              INTEGER,
        first_response_time_secs     NUMERIC,
        first_response_chats_count   INTEGER,
        raw                          JSONB,
        synced_at                    TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS livechat.agents (
        id            TEXT PRIMARY KEY,
        name          TEXT,
        email         TEXT,
        role          TEXT,
        login_status  TEXT,
        avatar_url    TEXT,
        groups        JSONB,
        raw           JSONB,
        synced_at     TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS livechat.chats (
        id          TEXT PRIMARY KEY,
        users       JSONB,
        properties  JSONB,
        access      JSONB,
        raw         JSONB,
        synced_at   TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS livechat.archives (
        id          TEXT PRIMARY KEY,
        thread_id   TEXT,
        created_at  TIMESTAMPTZ,
        ended_at    TIMESTAMPTZ,
        users       JSONB,
        properties  JSONB,
        tags        JSONB,
        raw         JSONB,
        synced_at   TIMESTAMPTZ DEFAULT NOW()
    );

    -- Snapshot table: fully replaced every run (current visitors)
    CREATE TABLE IF NOT EXISTS livechat.customers (
        id                   TEXT PRIMARY KEY,
        name                 TEXT,
        email                TEXT,
        last_visit_url       TEXT,
        visited_pages_count  INTEGER,
        chats_count          INTEGER,
        raw                  JSONB,
        synced_at            TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS livechat.daily_reports (
        date         DATE PRIMARY KEY,
        total_chats  INTEGER,
        raw          JSONB,
        synced_at    TIMESTAMPTZ DEFAULT NOW()
    );

    -- ── HelpDesk ────────────────────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS helpdesk.agents (
        id          TEXT PRIMARY KEY,
        name        TEXT,
        email       TEXT,
        role        TEXT,
        deleted_at  TIMESTAMPTZ,
        raw         JSONB,
        synced_at   TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS helpdesk.teams (
        id         TEXT PRIMARY KEY,
        name       TEXT,
        raw        JSONB,
        synced_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS helpdesk.mailboxes (
        id         TEXT PRIMARY KEY,
        name       TEXT,
        email      TEXT,
        enabled    BOOLEAN,
        raw        JSONB,
        synced_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS helpdesk.tags (
        id         TEXT PRIMARY KEY,
        name       TEXT,
        color      TEXT,
        raw        JSONB,
        synced_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS helpdesk.integrations (
        id          TEXT PRIMARY KEY,
        type        TEXT,
        created_at  TIMESTAMPTZ,
        raw         JSONB,
        synced_at   TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS helpdesk.tickets (
        id                TEXT PRIMARY KEY,
        short_id          TEXT,
        status            TEXT,
        priority          TEXT,
        subject           TEXT,
        created_at        TIMESTAMPTZ,
        updated_at        TIMESTAMPTZ,
        last_message_at   TIMESTAMPTZ,
        created_by        TEXT,
        created_by_type   TEXT,
        requester_email   TEXT,
        requester_name    TEXT,
        assignee_id       TEXT,
        assignee_email    TEXT,
        team_ids          JSONB,
        tag_ids           JSONB,
        source            TEXT,
        spam              BOOLEAN,
        rating            JSONB,
        raw               JSONB,
        synced_at         TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS helpdesk.canned_responses (
        id         TEXT PRIMARY KEY,
        name       TEXT,
        shortcut   TEXT,
        text       TEXT,
        raw        JSONB,
        synced_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS helpdesk.rules (
        id         TEXT PRIMARY KEY,
        name       TEXT,
        enabled    BOOLEAN,
        raw        JSONB,
        synced_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS helpdesk.macros (
        id         TEXT PRIMARY KEY,
        name       TEXT,
        raw        JSONB,
        synced_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS helpdesk.views (
        id         TEXT PRIMARY KEY,
        name       TEXT,
        raw        JSONB,
        synced_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS helpdesk.webhooks (
        id         TEXT PRIMARY KEY,
        url        TEXT,
        raw        JSONB,
        synced_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS helpdesk.ticket_messages (
        id            TEXT PRIMARY KEY,
        ticket_id     TEXT,
        type          TEXT,
        date          TIMESTAMPTZ,
        author_type   TEXT,
        author_email  TEXT,
        message_text  TEXT,
        is_private    BOOLEAN,
        source_type   TEXT,
        raw           JSONB,
        synced_at     TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS helpdesk.daily_reports (
        date                              DATE PRIMARY KEY,
        new_tickets                       INTEGER,
        sum_seconds_to_response           BIGINT,
        sum_seconds_to_assignment         BIGINT,
        response_count                    INTEGER,
        sum_seconds_from_creation         BIGINT,
        sum_seconds_from_assignment       BIGINT,
        resolution_count_from_creation    INTEGER,
        resolution_count_from_assignment  INTEGER,
        resolution_count                  INTEGER,
        synced_at                         TIMESTAMPTZ DEFAULT NOW()
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info("All tables created / verified.")


# ─── Write Helpers ────────────────────────────────────────────────────────────

def upsert(conn, table, rows, conflict_col="id"):
    """
    INSERT ... ON CONFLICT DO UPDATE for a list of row dicts.
    All non-PK columns are updated on conflict.
    synced_at is always refreshed to NOW().
    """
    if not rows:
        log.warning(f"upsert({table}): no rows to write")
        return

    cols = list(rows[0].keys())
    col_names    = ", ".join(cols)
    placeholders = ", ".join([f"%({c})s" for c in cols])
    updates      = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != conflict_col])

    sql = f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_col}) DO UPDATE
        SET {updates}, synced_at = NOW();
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()
    log.info(f"Upserted {len(rows):,} rows → {table}")


def truncate_insert(conn, table, rows):
    """
    Full-replace strategy for snapshot tables (e.g. online customers).
    Truncates the table, then bulk-inserts all rows.
    """
    if not rows:
        log.warning(f"truncate_insert({table}): no rows")
        return

    cols = list(rows[0].keys())
    col_names    = ", ".join(cols)
    placeholders = ", ".join([f"%({c})s" for c in cols])

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table};")
        sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders});"
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()
    log.info(f"Reloaded {len(rows):,} rows → {table}")
