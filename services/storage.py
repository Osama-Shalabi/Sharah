from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class JobRecord:
    id: str
    type: str
    page_url: Optional[str]
    status: str
    created_at: float
    updated_at: float
    options: Dict[str, Any]
    stats: Dict[str, Any]
    error: Optional[str]


class SQLiteStorage:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
        self.init_db()

    def init_db(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                  id TEXT PRIMARY KEY,
                  type TEXT NOT NULL,
                  page_url TEXT,
                  status TEXT NOT NULL,
                  created_at REAL NOT NULL,
                  updated_at REAL NOT NULL,
                  options_json TEXT NOT NULL,
                  stats_json TEXT NOT NULL,
                  error TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS job_items (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  job_id TEXT NOT NULL,
                  url TEXT NOT NULL,
                  status TEXT NOT NULL,
                  file_path TEXT,
                  meta_json TEXT,
                  error TEXT,
                  created_at REAL NOT NULL,
                  updated_at REAL NOT NULL,
                  UNIQUE(job_id, url)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS downloads (
                  url TEXT PRIMARY KEY,
                  video_id TEXT,
                  file_path TEXT,
                  meta_json TEXT,
                  created_at REAL NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS job_logs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  job_id TEXT NOT NULL,
                  ts REAL NOT NULL,
                  level TEXT NOT NULL,
                  message TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS facebook_reels (
                  reel_id TEXT PRIMARY KEY,
                  reel_url TEXT NOT NULL,
                  title TEXT,
                  upload_date TEXT,
                  thumbnail_url TEXT,
                  source_page_url TEXT NOT NULL,
                  created_at REAL NOT NULL,
                  updated_at REAL NOT NULL
                )
                """
            )
            # Backward-compatible migration for existing DBs.
            cols = {r["name"] for r in cur.execute("PRAGMA table_info(facebook_reels)").fetchall()}
            if "thumbnail_url" not in cols:
                cur.execute("ALTER TABLE facebook_reels ADD COLUMN thumbnail_url TEXT")
            self._conn.commit()

    def upsert_facebook_reel(
        self,
        *,
        reel_id: str,
        reel_url: str,
        source_page_url: str,
        title: Optional[str] = None,
        upload_date: Optional[str] = None,
        thumbnail_url: Optional[str] = None,
    ) -> None:
        now = time.time()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO facebook_reels (reel_id, reel_url, title, upload_date, thumbnail_url, source_page_url, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(reel_id) DO UPDATE SET
                  reel_url = excluded.reel_url,
                  title = COALESCE(excluded.title, facebook_reels.title),
                  upload_date = COALESCE(excluded.upload_date, facebook_reels.upload_date),
                  thumbnail_url = COALESCE(excluded.thumbnail_url, facebook_reels.thumbnail_url),
                  source_page_url = excluded.source_page_url,
                  updated_at = excluded.updated_at
                """,
                (reel_id, reel_url, title, upload_date, thumbnail_url, source_page_url, now, now),
            )
            self._conn.commit()

    def count_facebook_reels(self, *, source_page_url: Optional[str] = None) -> int:
        with self._lock:
            cur = self._conn.cursor()
            if source_page_url:
                row = cur.execute(
                    "SELECT COUNT(*) AS c FROM facebook_reels WHERE source_page_url = ?",
                    (source_page_url,),
                ).fetchone()
            else:
                row = cur.execute("SELECT COUNT(*) AS c FROM facebook_reels").fetchone()
            return int(row["c"] if row else 0)

    def delete_facebook_reels(self, *, source_page_url: str) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM facebook_reels WHERE source_page_url = ?", (source_page_url,))
            self._conn.commit()

    def list_facebook_reels(
        self,
        *,
        limit: Optional[int] = 10,
        offset: int = 0,
        source_page_url: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        offset = max(0, int(offset or 0))
        with self._lock:
            cur = self._conn.cursor()
            if source_page_url:
                if limit is None:
                    rows = cur.execute(
                        """
                        SELECT reel_id, reel_url, title, upload_date, thumbnail_url, source_page_url, created_at, updated_at
                        FROM facebook_reels
                        WHERE source_page_url = ?
                        ORDER BY (upload_date IS NULL) ASC, upload_date DESC, updated_at DESC
                        """,
                        (source_page_url,),
                    ).fetchall()
                else:
                    limit_n = max(1, int(limit))
                    rows = cur.execute(
                        """
                        SELECT reel_id, reel_url, title, upload_date, thumbnail_url, source_page_url, created_at, updated_at
                        FROM facebook_reels
                        WHERE source_page_url = ?
                        ORDER BY (upload_date IS NULL) ASC, upload_date DESC, updated_at DESC
                        LIMIT ? OFFSET ?
                        """,
                        (source_page_url, limit_n, offset),
                    ).fetchall()
            else:
                if limit is None:
                    rows = cur.execute(
                        """
                        SELECT reel_id, reel_url, title, upload_date, thumbnail_url, source_page_url, created_at, updated_at
                        FROM facebook_reels
                        ORDER BY (upload_date IS NULL) ASC, upload_date DESC, updated_at DESC
                        """,
                    ).fetchall()
                else:
                    limit_n = max(1, int(limit))
                    rows = cur.execute(
                        """
                        SELECT reel_id, reel_url, title, upload_date, thumbnail_url, source_page_url, created_at, updated_at
                        FROM facebook_reels
                        ORDER BY (upload_date IS NULL) ASC, upload_date DESC, updated_at DESC
                        LIMIT ? OFFSET ?
                        """,
                        (limit_n, offset),
                    ).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def create_job(self, job_type: str, *, page_url: Optional[str], options: Dict[str, Any]) -> str:
        job_id = str(uuid.uuid4())
        now = time.time()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO jobs (id, type, page_url, status, created_at, updated_at, options_json, stats_json, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (job_id, job_type, page_url, "queued", now, now, json.dumps(options), json.dumps({}), None),
            )
            self._conn.commit()
        return job_id

    def set_job_status(
        self,
        job_id: str,
        status: str,
        *,
        error: Optional[str] = None,
        stats: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = time.time()
        with self._lock:
            cur = self._conn.cursor()
            if stats is None:
                cur.execute(
                    "UPDATE jobs SET status = ?, updated_at = ?, error = ? WHERE id = ?",
                    (status, now, error, job_id),
                )
            else:
                cur.execute(
                    "UPDATE jobs SET status = ?, updated_at = ?, error = ?, stats_json = ? WHERE id = ?",
                    (status, now, error, json.dumps(stats), job_id),
                )
            self._conn.commit()

    def append_log(self, job_id: str, level: str, message: str) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "INSERT INTO job_logs (job_id, ts, level, message) VALUES (?, ?, ?, ?)",
                (job_id, time.time(), level, message),
            )
            self._conn.commit()

    def add_job_item(self, job_id: str, url: str) -> None:
        now = time.time()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT OR IGNORE INTO job_items (job_id, url, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, url, "discovered", now, now),
            )
            self._conn.commit()

    def update_job_item(
        self,
        job_id: str,
        url: str,
        *,
        status: str,
        file_path: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                UPDATE job_items
                SET status = ?, file_path = ?, meta_json = ?, error = ?, updated_at = ?
                WHERE job_id = ? AND url = ?
                """,
                (status, file_path, json.dumps(meta) if meta is not None else None, error, time.time(), job_id, url),
            )
            self._conn.commit()

    def is_downloaded(self, url: str) -> bool:
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute("SELECT 1 FROM downloads WHERE url = ? LIMIT 1", (url,)).fetchone()
            return row is not None

    def mark_downloaded(self, *, url: str, video_id: Optional[str], file_path: str, meta: Dict[str, Any]) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT OR REPLACE INTO downloads (url, video_id, file_path, meta_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (url, video_id, file_path, json.dumps(meta), time.time()),
            )
            self._conn.commit()

    def get_download(self, url: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute(
                "SELECT url, video_id, file_path, meta_json, created_at FROM downloads WHERE url = ? LIMIT 1",
                (url,),
            ).fetchone()
            if row is None:
                return None
            try:
                meta = json.loads(row["meta_json"] or "") if row["meta_json"] else {}
            except Exception:
                meta = {}
            return {
                "url": row["url"],
                "video_id": row["video_id"],
                "file_path": row["file_path"],
                "meta": meta,
                "created_at": row["created_at"],
            }

    def get_job(self, job_id: str) -> Dict[str, Any]:
        with self._lock:
            cur = self._conn.cursor()
            job_row = cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if job_row is None:
                raise KeyError(job_id)

            items = cur.execute(
                "SELECT url, status, file_path, meta_json, error, created_at, updated_at FROM job_items WHERE job_id = ? ORDER BY id ASC",
                (job_id,),
            ).fetchall()
            logs = cur.execute(
                "SELECT ts, level, message FROM job_logs WHERE job_id = ? ORDER BY id ASC",
                (job_id,),
            ).fetchall()

        def _json_or_empty(s: str) -> Dict[str, Any]:
            try:
                return json.loads(s) if s else {}
            except Exception:
                return {}

        return {
            "job": {
                "id": job_row["id"],
                "type": job_row["type"],
                "page_url": job_row["page_url"],
                "status": job_row["status"],
                "created_at": job_row["created_at"],
                "updated_at": job_row["updated_at"],
                "options": _json_or_empty(job_row["options_json"]),
                "stats": _json_or_empty(job_row["stats_json"]),
                "error": job_row["error"],
            },
            "items": [
                {
                    "url": r["url"],
                    "status": r["status"],
                    "file_path": r["file_path"],
                    "meta": _json_or_empty(r["meta_json"] or ""),
                    "error": r["error"],
                    "created_at": r["created_at"],
                    "updated_at": r["updated_at"],
                }
                for r in items
            ],
            "logs": [{"ts": r["ts"], "level": r["level"], "message": r["message"]} for r in logs],
        }

    def list_job_item_urls(self, job_id: str) -> List[str]:
        with self._lock:
            cur = self._conn.cursor()
            rows = cur.execute("SELECT url FROM job_items WHERE job_id = ? ORDER BY id ASC", (job_id,)).fetchall()
            return [r["url"] for r in rows]
