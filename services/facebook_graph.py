from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class GraphReelItem:
    item_id: str
    permalink_url: str
    created_time: Optional[str]
    title: str
    thumbnail_url: str


def _date_from_created_time(created_time: Optional[str]) -> Optional[str]:
    if not created_time:
        return None
    # Most Graph API timestamps look like: 2024-05-24T12:34:56+0000
    s = created_time.strip()
    try:
        dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z")
        return dt.date().isoformat()
    except Exception:
        return None


def _http_get_json(url: str, *, timeout_s: int = 20) -> Dict[str, Any]:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=timeout_s) as resp:  # nosec - intended: Graph API request
        raw = resp.read()
    return json.loads(raw.decode("utf-8", errors="ignore"))


def _normalize_permalink(url: str) -> str:
    u = (url or "").strip()
    if u.startswith("/"):
        return "https://www.facebook.com" + u
    return u


def iter_page_reels(
    *,
    page_id: str,
    page_access_token: str,
    graph_api_version: str = "v20.0",
    max_items: Optional[int] = None,
    page_size: int = 50,
) -> Iterable[GraphReelItem]:
    """
    Iterates public Page video items using Graph API pagination.
    Note: Graph API may return videos that include reels; use permalink_url for navigation.
    """
    if not page_id or not page_access_token:
        raise ValueError("Missing page_id or page_access_token.")

    page_size = max(1, min(int(page_size or 50), 100))
    max_items_n = None if max_items is None else max(1, int(max_items))

    # Try dedicated reels edge first; fall back to videos.
    edges = ["video_reels", "videos"]

    for edge in edges:
        after = None
        yielded = 0
        while True:
            params = {
                "access_token": page_access_token,
                "limit": str(page_size),
                "fields": ",".join(["id", "permalink_url", "created_time", "title", "description", "picture"]),
            }
            if after:
                params["after"] = after
            url = f"https://graph.facebook.com/{graph_api_version}/{page_id}/{edge}?{urlencode(params)}"

            try:
                payload = _http_get_json(url)
            except (HTTPError, URLError, TimeoutError, ValueError) as e:
                # If the edge doesn't exist (common), try next edge.
                if isinstance(e, HTTPError) and e.code in {400, 404}:
                    break
                raise

            data = payload.get("data") or []
            if not isinstance(data, list):
                data = []

            for it in data:
                if not isinstance(it, dict):
                    continue
                item_id = str(it.get("id") or "").strip()
                permalink = _normalize_permalink(str(it.get("permalink_url") or "").strip())
                if not item_id or not permalink:
                    continue

                title = str(it.get("title") or it.get("description") or "").strip()
                pic = it.get("picture") or ""
                if isinstance(pic, dict):
                    pic = pic.get("data", {}).get("url", "") if isinstance(pic.get("data"), dict) else ""
                thumbnail = str(pic or "").strip()

                yield GraphReelItem(
                    item_id=item_id,
                    permalink_url=permalink,
                    created_time=str(it.get("created_time") or "").strip() or None,
                    title=title,
                    thumbnail_url=thumbnail,
                )
                yielded += 1
                if max_items_n is not None and yielded >= max_items_n:
                    return

            paging = payload.get("paging") or {}
            cursors = paging.get("cursors") if isinstance(paging, dict) else {}
            after = cursors.get("after") if isinstance(cursors, dict) else None
            if not after:
                return


def sync_page_reels_via_graph(
    *,
    storage,
    source_page_url: str,
    page_id: str,
    page_access_token: str,
    graph_api_version: str = "v20.0",
    max_items: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Stores reels/videos from Graph API in the local SQLite DB.
    """
    stored = 0
    for item in iter_page_reels(
        page_id=page_id,
        page_access_token=page_access_token,
        graph_api_version=graph_api_version,
        max_items=max_items,
        page_size=100,
    ):
        upload_date = _date_from_created_time(item.created_time)
        storage.upsert_facebook_reel(
            reel_id=item.item_id,
            reel_url=item.permalink_url,
            source_page_url=source_page_url,
            title=item.title or None,
            upload_date=upload_date,
            thumbnail_url=item.thumbnail_url or None,
        )
        stored += 1

    return {"stored": stored}

