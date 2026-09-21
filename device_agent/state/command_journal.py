'''
Local SQLite command journal (Part 7): "Maintain local SQLite command journal on a persistent
volume. Deduplicate commands."

This is a LOCAL cache of what THIS Device Agent process has already seen/executed - it is not
durable truth (Postgres/device_commands, owned by Cloud, is). Its only jobs are:
  1. Deduplicate an ExecuteCommand the Agent already accepted/ran, so redelivery after a
     reconnect never re-executes a command against Container Maker a second time.
  2. Remember a result the Agent computed but hasn't yet successfully reported back to Cloud
     (crash between "Container Maker succeeded" and "Cloud acknowledged" - Part 7's required
     test), so it can resend that exact result on the next connection instead of losing it or
     re-running the underlying operation.
'''
import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional


@dataclass
class JournalEntry:
    command_id: str
    operation: str
    status: str  # "accepted" | "running" | "succeeded" | "failed"
    result_json: Optional[str]
    error_code: Optional[str]
    error_message: Optional[str]
    reported_to_cloud: bool
    created_at: str
    updated_at: str


class CommandJournal:
    '''One instance per process. Thread-safe via a single lock - the journal's write volume is
    low (one row per command, not per message), so a simple lock is sufficient; no need for a
    connection pool.'''

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, isolation_level=None)  # autocommit; we manage transactions explicitly
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS command_journal (
                    command_id TEXT PRIMARY KEY,
                    operation TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result_json TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    reported_to_cloud INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_journal_reported ON command_journal (reported_to_cloud)")

    def has_seen(self, command_id: str) -> bool:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT 1 FROM command_journal WHERE command_id = ?", (command_id,)).fetchone()
            return row is not None

    def get(self, command_id: str) -> Optional[JournalEntry]:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM command_journal WHERE command_id = ?", (command_id,)).fetchone()
            return self._row_to_entry(row) if row else None

    def record_accepted(self, command_id: str, operation: str) -> None:
        '''First time this command_id is seen - insert if absent, otherwise a no-op (a duplicate
        ExecuteCommand for an already-known command must never reset its progress).'''
        now = datetime.now(timezone.utc).isoformat()
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO command_journal "
                "(command_id, operation, status, reported_to_cloud, created_at, updated_at) "
                "VALUES (?, ?, 'accepted', 0, ?, ?)",
                (command_id, operation, now, now),
            )

    def record_running(self, command_id: str) -> None:
        self._update_status(command_id, "running")

    def record_result(self, command_id: str, status: str, result: Optional[dict] = None,
                       error_code: Optional[str] = None, error_message: Optional[str] = None) -> None:
        '''status must be "succeeded" or "failed". reported_to_cloud starts at 0 - the caller
        marks it reported only after Cloud actually acknowledges (mark_reported), so a crash
        right after this call still has the result available to resend.'''
        now = datetime.now(timezone.utc).isoformat()
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE command_journal SET status = ?, result_json = ?, error_code = ?, "
                "error_message = ?, reported_to_cloud = 0, updated_at = ? WHERE command_id = ?",
                (status, json.dumps(result) if result is not None else None, error_code, error_message, now, command_id),
            )

    def mark_reported(self, command_id: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE command_journal SET reported_to_cloud = 1, updated_at = ? WHERE command_id = ?",
                (now, command_id),
            )

    def unreported_terminal_entries(self) -> list:
        '''"Resend unacknowledged results from the local journal" (Part 7) - what the Agent
        replays to Cloud right after (re)connecting, covering the crash-after-success-before-ack
        window explicitly called out in Part 7's required tests.'''
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM command_journal WHERE reported_to_cloud = 0 "
                "AND status IN ('succeeded', 'failed') ORDER BY created_at ASC"
            ).fetchall()
            return [self._row_to_entry(row) for row in rows]

    def _update_status(self, command_id: str, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE command_journal SET status = ?, updated_at = ? WHERE command_id = ?",
                (status, now, command_id),
            )

    @staticmethod
    def _row_to_entry(row: sqlite3.Row) -> JournalEntry:
        return JournalEntry(
            command_id=row["command_id"], operation=row["operation"], status=row["status"],
            result_json=row["result_json"], error_code=row["error_code"], error_message=row["error_message"],
            reported_to_cloud=bool(row["reported_to_cloud"]), created_at=row["created_at"], updated_at=row["updated_at"],
        )
