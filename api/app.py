from __future__ import annotations

import logging
import re
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request as UrlRequest, urlopen
from html import unescape

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

from api.models import DownloadRequest, FetchAndDownloadRequest, FetchLinksRequest, JobResponse, WatchRequest
from config import get_settings
from services.downloader import VideoDownloader
from services.drive_uploader import DriveUploader
from services.facebook_scraper import FacebookReelsScraper
from services.jobs import JobManager
from services.storage import SQLiteStorage
from services.reels_indexer import sync_page_reels_to_db


log = logging.getLogger("fb_public_downloader")

_SHARAH_FB_REELS_URL = "https://www.facebook.com/shadi.shirri/reels/"
_SHARAH_REEL_ID_RE = re.compile(r"(?:https?:\/\/(?:www\.)?facebook\.com)?\/reel\/([0-9]+)", re.IGNORECASE)
_OG_IMAGE_RE = re.compile(r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', re.IGNORECASE)
_TW_IMAGE_RE = re.compile(r'<meta[^>]+name="twitter:image"[^>]+content="([^"]+)"', re.IGNORECASE)

_SHARAH_SOURCE_PAGE_URL = "https://www.facebook.com/shadi.shirri/reels/"


def create_app() -> FastAPI:
    load_dotenv()
    settings = get_settings()
    app = FastAPI(title="Public Facebook Reels/Page Downloader", version="0.1.0")

    base_dir = Path(__file__).resolve().parent
    templates = Jinja2Templates(directory=str(base_dir / "templates"))
    app.mount("/static", StaticFiles(directory=str(base_dir / "static")), name="static")

    storage = SQLiteStorage(settings.app_state_db)
    scraper = FacebookReelsScraper()
    downloader = VideoDownloader()
    drive_uploader = DriveUploader(
        client_secret_path=settings.gdrive_client_secret,
        token_path=settings.gdrive_token_path,
        oauth_console=settings.gdrive_oauth_console,
    )
    jobs = JobManager(
        storage=storage,
        scraper=scraper,
        downloader=downloader,
        default_output_root=settings.downloads_root,
        drive_uploader=drive_uploader,
    )

    @app.get("/health")
    async def health() -> dict:
        return {"ok": True}

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        return templates.TemplateResponse("index.html", {"request": request})

    @app.get("/sharah", response_class=HTMLResponse)
    async def sharah(request: Request):
        return templates.TemplateResponse("sharah.html", {"request": request})

    @app.get("/api/sharah/reel-example")
    async def sharah_reel_example() -> dict:
        """
        Best-effort: fetch the public page and extract a real reel ID/URL.
        Facebook may block server-side requests; on failure return {}.
        """
        def _fetch_text(url: str) -> str:
            req = UrlRequest(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept-Language": "ar,en;q=0.8",
                },
            )
            with urlopen(req, timeout=12) as resp:  # nosec - intended: fetch public HTML
                raw = resp.read()
            return raw.decode("utf-8", errors="ignore")

        html = ""
        try:
            html = _fetch_text(_SHARAH_FB_REELS_URL)
        except (HTTPError, URLError, TimeoutError, ValueError) as e:
            log.warning("Could not fetch Facebook reels (html fetch): %s", e)

        reel_id = None
        seen: set[str] = set()
        if html:
            for m in _SHARAH_REEL_ID_RE.finditer(html):
                rid = m.group(1)
                if rid in seen:
                    continue
                seen.add(rid)
                reel_id = rid
                break

        if not reel_id:
            try:
                res = await scraper.discover_urls(_SHARAH_FB_REELS_URL, max_videos=5, headless=True, log=None)
                for u in res.video_urls:
                    m = _SHARAH_REEL_ID_RE.search(u)
                    if m:
                        reel_id = m.group(1)
                        break
            except Exception as e:
                log.warning("Could not fetch Facebook reels (playwright): %s", e)
                return {}

        if not reel_id:
            log.warning("Could not fetch Facebook reels: no reel IDs found")
            return {}

        reel_url = f"https://www.facebook.com/reel/{reel_id}"
        thumbnail_url = ""

        try:
            reel_html = _fetch_text(reel_url)
            m = re.search(r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', reel_html, re.IGNORECASE)
            if not m:
                m = re.search(r'<meta[^>]+name="twitter:image"[^>]+content="([^"]+)"', reel_html, re.IGNORECASE)
            if m:
                thumbnail_url = unescape(m.group(1)).strip()
        except Exception as e:
            log.warning("Could not fetch Facebook reel thumbnail: %s", e)

        return {
            "id": reel_id,
            "facebookReelUrl": reel_url,
            "thumbnailUrl": thumbnail_url,
        }

    @app.get("/api/sharah/reels")
    async def sharah_reels(limit: int | None = None) -> list[dict]:
        """
        Returns reels from the local SQLite DB (fast). If the DB is empty, it performs a small best-effort sync.
        """
        limit_n = None if limit is None else max(1, int(limit))
        if storage.count_facebook_reels(source_page_url=_SHARAH_SOURCE_PAGE_URL) == 0:
            try:
                await sync_page_reels_to_db(
                    storage=storage,
                    page_url=_SHARAH_SOURCE_PAGE_URL,
                    max_reels=max(10, limit_n or 10),
                    enrich_metadata=True,
                )
            except Exception as e:
                log.warning("Could not sync Facebook reels to DB: %s", e)

        rows = storage.list_facebook_reels(limit=limit_n, offset=0, source_page_url=_SHARAH_SOURCE_PAGE_URL)
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "id": r["reel_id"],
                    "title": "",  # keep stored in DB; hide from public UI for now
                    "topic": "عام",
                    "thumbnail": r.get("thumbnail_url") or "",
                    "facebookReelUrl": r["reel_url"],
                    "uploadDate": r.get("upload_date"),
                }
            )
        return out

    @app.post("/api/sharah/reels/sync-db")
    async def sharah_sync_reels_db(
        max_reels: int = 500,
        enrich_metadata: bool = True,
        headless: bool = True,
        reset: bool = False,
    ) -> dict:
        """
        One-time (or occasional) sync: discovers up to `max_reels` reels and stores them in SQLite.
        This can take several minutes for large counts.
        """
        return await sync_page_reels_to_db(
            storage=storage,
            page_url=_SHARAH_SOURCE_PAGE_URL,
            max_reels=max_reels,
            headless=headless,
            enrich_metadata=enrich_metadata,
            reset_existing=reset,
        )

    @app.get("/api/sharah/reels/from-db")
    async def sharah_reels_from_db(limit: int | None = 10, offset: int = 0) -> list[dict]:
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

    @app.post("/fetch-links")
    async def fetch_links(req: FetchLinksRequest) -> dict:
        try:
            job_id = jobs.start_fetch_links(page_url=req.page_url, options=req.options.model_dump())
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        return {"job_id": job_id}

    @app.post("/download")
    async def download(req: DownloadRequest) -> dict:
        try:
            job_id = jobs.start_download(urls=req.urls, options=req.options.model_dump())
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        return {"job_id": job_id}

    @app.post("/fetch-and-download")
    async def fetch_and_download(req: FetchAndDownloadRequest) -> dict:
        options = {**req.fetch.model_dump(), **req.download.model_dump()}
        try:
            job_id = jobs.start_fetch_and_download(page_url=req.page_url, options=options)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        return {"job_id": job_id}

    @app.post("/watch")
    async def watch(req: WatchRequest) -> dict:
        options = {**req.fetch.model_dump(), **req.download.model_dump(), **req.watch.model_dump()}
        try:
            job_id = jobs.start_watch(page_url=req.page_url, options=options)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        return {"job_id": job_id}

    @app.get("/jobs/{job_id}", response_model=JobResponse)
    async def get_job(job_id: str):
        try:
            return storage.get_job(job_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="Job not found")

    return app


app = create_app()
