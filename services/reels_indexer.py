from __future__ import annotations

import json
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import asyncio

from services.facebook_scraper import FacebookReelsScraper
from services.storage import SQLiteStorage


REEL_ID_RE = re.compile(r"(?:https?:\/\/(?:www\.)?facebook\.com)?\/reel\/([0-9]+)", re.IGNORECASE)


@dataclass(frozen=True)
class ReelRecord:
    reel_id: str
    reel_url: str
    title: Optional[str]
    upload_date: Optional[str]  # YYYY-MM-DD
    thumbnail_url: Optional[str]


def _to_date_yyyy_mm_dd(ts: int) -> str:
    d = datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
    return d.isoformat()


def _yt_dlp_metadata(url: str, *, timeout_s: int = 45) -> dict:
    """
    Best-effort metadata fetch via yt-dlp WITHOUT downloading media.
    """
    proc = subprocess.run(
        ["yt-dlp", "--skip-download", "--dump-single-json", "--no-warnings", "--", url],
        capture_output=True,
        text=True,
        timeout=timeout_s,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "yt-dlp failed").strip())
    return json.loads(proc.stdout)


def fetch_reel_metadata(url: str) -> ReelRecord:
    m = REEL_ID_RE.search(url)
    if not m:
        raise ValueError("Not a reel URL.")
    rid = m.group(1)
    reel_url = f"https://www.facebook.com/reel/{rid}"

    title: Optional[str] = None
    upload_date: Optional[str] = None
    thumbnail_url: Optional[str] = None
    try:
        meta = _yt_dlp_metadata(reel_url)
        raw_title = meta.get("title")
        if isinstance(raw_title, str) and raw_title.strip():
            title = raw_title.strip()

        now = int(time.time())
        ts = meta.get("timestamp")
        if isinstance(ts, (int, float)) and 0 < int(ts) <= now + 3600:
            upload_date = _to_date_yyyy_mm_dd(int(ts))
        else:
            ud = meta.get("upload_date")
            if isinstance(ud, str) and re.fullmatch(r"\d{8}", ud):
                y = int(ud[0:4])
                m = int(ud[4:6])
                d = int(ud[6:8])
                if 2004 <= y <= datetime.now(timezone.utc).year and 1 <= m <= 12 and 1 <= d <= 31:
                    upload_date = f"{ud[0:4]}-{ud[4:6]}-{ud[6:8]}"

        t = meta.get("thumbnail")
        if isinstance(t, str) and t.strip().startswith("http"):
            thumbnail_url = t.strip()
    except Exception:
        pass

    return ReelRecord(reel_id=rid, reel_url=reel_url, title=title, upload_date=upload_date, thumbnail_url=thumbnail_url)


async def sync_page_reels_to_db(
    *,
    storage: SQLiteStorage,
    page_url: str,
    max_reels: int = 500,
    headless: bool = True,
    enrich_metadata: bool = True,
    reset_existing: bool = False,
) -> dict:
    """
    Discovers reel URLs from a public page/reels URL and stores them in SQLite.
    Metadata (title/upload_date) is best-effort and may be missing if Facebook blocks requests.
    """
    max_reels = max(1, min(int(max_reels or 500), 2000))
    scraper = FacebookReelsScraper()

    if reset_existing:
        storage.delete_facebook_reels(source_page_url=page_url)

    result = await scraper.discover_urls(
        page_url,
        max_videos=max_reels,
        headless=headless,
        scroll_pause_s=1.2,
        max_no_new_scrolls=80,
        log=None,
    )

    discovered = 0
    enriched = 0
    started = time.time()

    for u in result.video_urls:
        m = REEL_ID_RE.search(u)
        if not m:
            continue
        rid = m.group(1)
        reel_url = f"https://www.facebook.com/reel/{rid}"
        storage.upsert_facebook_reel(reel_id=rid, reel_url=reel_url, source_page_url=page_url)
        discovered += 1
        if discovered >= max_reels:
            break

    if enrich_metadata:
        rows = storage.list_facebook_reels(limit=max_reels, offset=0, source_page_url=page_url)
        sem = asyncio.Semaphore(4)

        async def worker(row: dict) -> None:
            nonlocal enriched
            if row.get("title") and row.get("upload_date") and row.get("thumbnail_url"):
                return
            async with sem:
                try:
                    rec = await asyncio.to_thread(fetch_reel_metadata, row["reel_url"])
                    storage.upsert_facebook_reel(
                        reel_id=rec.reel_id,
                        reel_url=rec.reel_url,
                        source_page_url=page_url,
                        title=rec.title,
                        upload_date=rec.upload_date,
                        thumbnail_url=rec.thumbnail_url,
                    )
                    enriched += 1
                except Exception:
                    return

        await asyncio.gather(*(worker(r) for r in rows))

    return {
        "page_url": page_url,
        "stored": storage.count_facebook_reels(source_page_url=page_url),
        "discovered": discovered,
        "enriched_attempted": enriched,
        "elapsed_s": round(time.time() - started, 2),
    }
