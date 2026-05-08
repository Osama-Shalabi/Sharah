from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

from config import get_settings
from services.storage import SQLiteStorage
from services.facebook_graph import sync_page_reels_via_graph
from services.excel_reels import list_reels_from_excel
from services.hybrid_search import (
    EMBEDDING_MODEL,
    create_embedding,
    embedding_from_json,
    embedding_to_json,
    hybrid_rank_reels,
)


log = logging.getLogger("sharah")
_SHARAH_SOURCE_PAGE_URL = "https://www.facebook.com/shadi.shirri/reels/"


def _excel_row_to_reel(row: dict) -> dict:
    return {
        "id": row["id"],
        "title": row.get("reel_title") or "",
        "topic": "عام",
        "thumbnail": row.get("thumbnail_url") or "",
        "facebookReelUrl": row["reel_url"],
        "uploadDate": row.get("upload_date") or None,
    }


def _db_row_to_reel(row: dict) -> dict:
    return {
        "id": row["reel_id"],
        "title": row.get("title") or "",
        "topic": "عام",
        "thumbnail": row.get("thumbnail_url") or "",
        "facebookReelUrl": row["reel_url"],
        "uploadDate": row.get("upload_date"),
    }


def create_app() -> FastAPI:
    load_dotenv()
    settings = get_settings()
    app = FastAPI(title="Sharrah", version="0.1.0")

    base_dir = Path(__file__).resolve().parent
    templates = Jinja2Templates(directory=str(base_dir / "templates"))
    app.mount("/static", StaticFiles(directory=str(base_dir / "static")), name="static")
    app.mount("/images", StaticFiles(directory=str(base_dir.parent / "images")), name="images")

    storage = SQLiteStorage(settings.app_state_db)

    def list_display_reels(*, limit: int | None = 100, offset: int = 0) -> list[dict]:
        source = (settings.sharah_reels_source or "auto").strip().lower()
        use_excel = source in {"excel", "auto"} and settings.sharah_reels_xlsx.exists()
        if use_excel:
            rows = list_reels_from_excel(xlsx_path=settings.sharah_reels_xlsx, limit=limit, offset=offset)
            return [_excel_row_to_reel(r) for r in rows]

        rows = storage.list_facebook_reels(limit=limit, offset=offset, source_page_url=_SHARAH_SOURCE_PAGE_URL)
        return [_db_row_to_reel(r) for r in rows]

    def ensure_title_embeddings(reels: list[dict]) -> dict:
        existing_rows = storage.list_reel_title_embeddings(model=EMBEDDING_MODEL)
        existing_by_id = {str(r["reel_id"]): r for r in existing_rows}

        for reel in reels:
            reel_id = str(reel.get("id") or "")
            title = str(reel.get("title") or "").strip()
            if not reel_id or not title:
                continue

            current = existing_by_id.get(reel_id)
            if current and str(current.get("title") or "") == title:
                continue

            storage.upsert_reel_title_embedding(
                reel_id=reel_id,
                title=title,
                embedding=embedding_to_json(create_embedding(title)),
                model=EMBEDDING_MODEL,
            )

        refreshed_rows = storage.list_reel_title_embeddings(model=EMBEDDING_MODEL)
        return {
            str(r["reel_id"]): embedding_from_json(str(r.get("embedding") or "[]"))
            for r in refreshed_rows
        }

    @app.get("/health")
    async def health() -> dict:
        return {"ok": True}

    @app.get("/")
    async def root():
        return RedirectResponse(url="/sharah", status_code=302)

    @app.get("/sharah", response_class=HTMLResponse)
    async def sharah(request: Request):
        return templates.TemplateResponse("sharah.html", {"request": request})

    @app.get("/api/sharah/reels")
    async def sharah_reels(limit: int | None = 100, offset: int = 0) -> list[dict]:
        limit_n = None if limit is None else max(1, int(limit))
        return list_display_reels(limit=limit_n, offset=offset)

    @app.post("/api/sharah/reels/index-embeddings")
    async def sharah_index_reel_embeddings() -> dict:
        reels = list_display_reels(limit=None, offset=0)
        ensure_title_embeddings(reels)
        indexed = sum(1 for r in reels if str(r.get("title") or "").strip())
        return {"indexed": indexed, "model": EMBEDDING_MODEL}

    @app.get("/api/sharah/reels/search")
    async def sharah_search_reels(q: str, limit: int = 100) -> list[dict]:
        query = (q or "").strip()
        if not query:
            return list_display_reels(limit=max(1, int(limit or 100)), offset=0)

        reels = list_display_reels(limit=None, offset=0)
        embeddings = ensure_title_embeddings(reels)
        return hybrid_rank_reels(
            query=query,
            reels=reels,
            stored_embeddings=embeddings,
            limit=max(1, int(limit or 100)),
        )

    @app.post("/api/sharah/reels/sync-graph")
    async def sharah_sync_reels_graph(max_items: int | None = None, reset: bool = False) -> dict:
        """
        Sync reels/videos via Facebook Graph API (recommended).
        Requires FB_PAGE_ID and FB_PAGE_ACCESS_TOKEN in .env.
        """
        if not settings.fb_page_id or not settings.fb_page_access_token:
            raise HTTPException(status_code=400, detail="Missing FB_PAGE_ID / FB_PAGE_ACCESS_TOKEN in environment.")

        if reset:
            storage.delete_facebook_reels(source_page_url=_SHARAH_SOURCE_PAGE_URL)

        try:
            res = sync_page_reels_via_graph(
                storage=storage,
                source_page_url=_SHARAH_SOURCE_PAGE_URL,
                page_id=settings.fb_page_id,
                page_access_token=settings.fb_page_access_token,
                graph_api_version=settings.fb_graph_api_version,
                max_items=max_items,
            )
        except Exception as e:
            log.warning("Graph sync failed: %s", e)
            return {
                "stored": 0,
                "db_total": storage.count_facebook_reels(source_page_url=_SHARAH_SOURCE_PAGE_URL),
                "error": "Could not sync from Facebook Graph API",
            }

        res["db_total"] = storage.count_facebook_reels(source_page_url=_SHARAH_SOURCE_PAGE_URL)
        return res

    @app.get("/api/sharah/reels/from-db")
    async def sharah_reels_from_db(limit: int | None = 100, offset: int = 0) -> list[dict]:
        limit_n = None if limit is None else max(1, int(limit))
        rows = storage.list_facebook_reels(limit=limit_n, offset=offset, source_page_url=_SHARAH_SOURCE_PAGE_URL)
        return [
            {
                "id": r["reel_id"],
                "facebookReelUrl": r["reel_url"],
                "title": r.get("title"),
                "uploadDate": r.get("upload_date"),
                "thumbnail": r.get("thumbnail_url"),
                "sourcePageUrl": r.get("source_page_url"),
            }
            for r in rows
        ]

    return app


app = create_app()
