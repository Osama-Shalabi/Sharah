from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS reel_title_embeddings (
                  reel_id TEXT PRIMARY KEY,
                  title TEXT NOT NULL,
                  embedding TEXT NOT NULL,
                  model TEXT NOT NULL,
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

    def upsert_reel_title_embedding(
        self,
        *,
        reel_id: str,
        title: str,
        embedding: str,
        model: str,
    ) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO reel_title_embeddings (reel_id, title, embedding, model, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(reel_id) DO UPDATE SET
                  title = excluded.title,
                  embedding = excluded.embedding,
                  model = excluded.model,
                  updated_at = excluded.updated_at
                """,
                (reel_id, title, embedding, model, now),
            )
            self._conn.commit()

    def list_reel_title_embeddings(self, *, model: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.cursor()
            if model:
                rows = cur.execute(
                    """
                    SELECT reel_id, title, embedding, model, updated_at
                    FROM reel_title_embeddings
                    WHERE model = ?
                    """,
                    (model,),
                ).fetchall()
            else:
                rows = cur.execute(
                    """
                    SELECT reel_id, title, embedding, model, updated_at
                    FROM reel_title_embeddings
                    """
                ).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
