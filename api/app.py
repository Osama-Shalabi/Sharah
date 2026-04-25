from __future__ import annotations

import logging
from pathlib import Path

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


log = logging.getLogger("fb_public_downloader")


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
