from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl


class FetchOptions(BaseModel):
    output: Optional[str] = Field(default="downloads", description="Output root folder")
    max_videos: int = Field(default=50, ge=1, le=500)
    headless: bool = True


class DownloadOptions(BaseModel):
    output: Optional[str] = Field(default="downloads", description="Output root folder")
    quality: str = Field(default="best", description="yt-dlp format string")
    dry_run: bool = False
    since: Optional[str] = Field(default=None, description="ISO date (YYYY-MM-DD), best-effort")
    concurrency: int = Field(default=2, ge=1, le=10)
    upload_to_drive: bool = Field(default=False, description="Upload downloaded files to Google Drive")
    gdrive_folder_id: Optional[str] = Field(default=None, description="Drive folder ID (optional)")


class FetchLinksRequest(BaseModel):
    page_url: str
    options: FetchOptions = FetchOptions()


class DownloadRequest(BaseModel):
    urls: List[str] = Field(min_length=1)
    options: DownloadOptions = DownloadOptions()


class FetchAndDownloadRequest(BaseModel):
    page_url: str
    fetch: FetchOptions = FetchOptions()
    download: DownloadOptions = DownloadOptions()


class WatchOptions(BaseModel):
    interval_s: int = Field(default=600, ge=30, le=86400, description="Polling interval in seconds")
    max_cycles: Optional[int] = Field(default=None, ge=1, le=100000, description="Stop after N cycles (optional)")
    max_consecutive_errors: int = Field(default=3, ge=1, le=20)


class WatchRequest(BaseModel):
    page_url: str
    fetch: FetchOptions = FetchOptions()
    download: DownloadOptions = DownloadOptions()
    watch: WatchOptions = WatchOptions()


class JobResponse(BaseModel):
    job: Dict[str, Any]
    items: List[Dict[str, Any]]
    logs: List[Dict[str, Any]]
