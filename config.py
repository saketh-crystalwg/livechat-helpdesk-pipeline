"""
Centralised configuration.
"""

# ─── Database ─────────────────────────────────────────────────────────────────
DATABASE_URL = "postgresql://udaoaoq238apcp:pd6d61a1b1d016aced1d154961cc6fe2592f373093a0131369a59481e4c1b240a@ec2-54-165-198-7.compute-1.amazonaws.com:5432/d46vii1fsld35p"

# ─── LiveChat ─────────────────────────────────────────────────────────────────
LC_PAT_B64  = "YmQwZjAxZDEtNDcxZC00NmJjLTljOWYtMTZkYWIyNmRkYjA2OnVzLXNvdXRoMTphcUloMEhoOG4yWG5iRUV6SHVpQ200MmN0Q1k="
LC_API_BASE = "https://api.livechatinc.com/v3.6"

# ─── HelpDesk ─────────────────────────────────────────────────────────────────
HD_PAT_B64  = "YmQwZjAxZDEtNDcxZC00NmJjLTljOWYtMTZkYWIyNmRkYjA2OnVzLXNvdXRoMTphcUloMEhoOG4yWG5iRUV6SHVpQ200MmN0Q1k="
HD_API_BASE = "https://api.helpdesk.com/v1"

# ─── Scheduler ────────────────────────────────────────────────────────────────
SCHEDULE_INTERVAL_MINUTES = 10
