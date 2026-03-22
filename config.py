"""
Centralised configuration — reads from .env file.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─── Database ─────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "")

# ─── LiveChat ─────────────────────────────────────────────────────────────────
LC_PAT_B64    = os.getenv("LC_PAT_B64", "")
LC_ACCOUNT_ID = os.getenv("LC_ACCOUNT_ID", "")
LC_ENTITY_ID  = os.getenv("LC_ENTITY_ID", "")
LC_API_BASE   = "https://api.livechatinc.com/v3.6"

# ─── HelpDesk ─────────────────────────────────────────────────────────────────
HD_PAT_B64  = os.getenv("HD_PAT_B64", "")
HD_API_BASE = "https://api.helpdesk.com/v1"

# ─── Scheduler ────────────────────────────────────────────────────────────────
SCHEDULE_INTERVAL_MINUTES = int(os.getenv("SCHEDULE_INTERVAL_MINUTES", "15"))
