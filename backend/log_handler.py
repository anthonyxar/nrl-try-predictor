"""
Best-effort logging handler that mirrors app log records into a Supabase
(Postgres) table (`app_logs`), so operational visibility doesn't depend on
Render's log retention window — logs can be browsed directly in Supabase's
Table Editor or SQL Editor.
"""

import logging

from database import insert_log_entry


class SupabaseLogHandler(logging.Handler):
    """logging.Handler that writes each record to the app_logs table.

    Runs the insert synchronously on whatever thread/coroutine emits the log
    line. That's a deliberate simplification: at INFO level this app logs
    only a handful of lines per run (startup, cache warmup, backfill/scrape
    summaries, warnings/errors) — not per-request — so the extra DB
    round-trip per line isn't worth the complexity of a background queue.
    Never raises: a DB hiccup while logging must not crash the app or
    recurse into more logging.
    """

    def __init__(self, source: str, level=logging.INFO):
        super().__init__(level=level)
        self.source = source

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            insert_log_entry(self.source, record.levelname, record.name, message)
        except Exception:
            pass


def attach(source: str, level=logging.INFO) -> None:
    """Attach a SupabaseLogHandler to the root logger so every module's
    logger.* calls are mirrored, without instrumenting each call site.
    Silences uvicorn's per-request access log (too high-volume to be
    useful in this table) while still capturing uvicorn's error log."""
    handler = SupabaseLogHandler(source=source, level=level)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().addHandler(handler)
    logging.getLogger("uvicorn.access").propagate = False
