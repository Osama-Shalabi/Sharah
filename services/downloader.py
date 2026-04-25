from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import yt_dlp
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from services.utils import ensure_dir, normalize_facebook_url, safe_fs_name


LogFn = Callable[[str, str], None]  # (level, message)


class YtDlpDownloadError(RuntimeError):
    pass


@dataclass(frozen=True)
class DownloadResult:
    url: str
    status: str  # downloaded|skipped|failed|filtered|dry_run
    file_path: Optional[str]
    video_id: Optional[str]
    meta: Dict[str, Any]
    error: Optional[str] = None


def _parse_since(since: Optional[str]) -> Optional[date]:
    if not since:
        return None
    return date.fromisoformat(since)


def _upload_date_to_date(upload_date: Optional[str]) -> Optional[date]:
    if not upload_date or len(upload_date) != 8:
        return None
    try:
        y, m, d = int(upload_date[0:4]), int(upload_date[4:6]), int(upload_date[6:8])
        return date(y, m, d)
    except Exception:
        return None


class _YtDlpLogger:
    def __init__(self, log: Optional[LogFn]):
        self._log = log

    def debug(self, msg: str) -> None:
        if self._log:
            self._log("debug", msg)

    def warning(self, msg: str) -> None:
        if self._log:
            self._log("warning", msg)

    def error(self, msg: str) -> None:
        if self._log:
            self._log("error", msg)


class VideoDownloader:
    def __init__(self, *, ffmpeg_location: Optional[str] = None):
        self.ffmpeg_location = ffmpeg_location

    @retry(
        retry=retry_if_exception_type(YtDlpDownloadError),
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=12),
        reraise=True,
    )
    def download_one(
        self,
        url: str,
        *,
        output_dir: Path,
        quality: str = "best",
        dry_run: bool = False,
        since: Optional[str] = None,
        log: Optional[LogFn] = None,
    ) -> DownloadResult:
        url = normalize_facebook_url(url)
        ensure_dir(output_dir)
        since_date = _parse_since(since)

        ydl_logger = _YtDlpLogger(log)
        outtmpl = str(output_dir / "%(upload_date>%Y-%m-%d)s_%(id)s_%(title).80B.%(ext)s")

        opts: Dict[str, Any] = {
            "format": quality or "best",
            "outtmpl": outtmpl,
            "noplaylist": True,
            "retries": 5,
            "fragment_retries": 5,
            "concurrent_fragment_downloads": 4,
            "windowsfilenames": True,
            "restrictfilenames": True,
            "quiet": True,
            "no_warnings": True,
            "logger": ydl_logger,
            "postprocessors": [
                {"key": "FFmpegVideoRemuxer", "preferedformat": "mp4"},
            ],
            "merge_output_format": "mp4",
        }
        if self.ffmpeg_location:
            opts["ffmpeg_location"] = self.ffmpeg_location

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=not dry_run)
        except Exception as e:
            raise YtDlpDownloadError(str(e)) from e

        video_id = str(info.get("id") or "") or None
        upload_date = info.get("upload_date")
        if since_date:
            uploaded = _upload_date_to_date(upload_date)
            if uploaded and uploaded < since_date:
                return DownloadResult(
                    url=url,
                    status="filtered",
                    file_path=None,
                    video_id=video_id,
                    meta=info,
                )

        if dry_run:
            return DownloadResult(url=url, status="dry_run", file_path=None, video_id=video_id, meta=info)

        file_path = info.get("_filename")
        if not file_path:
            file_path = info.get("requested_downloads", [{}])[0].get("filepath")
        file_path = str(file_path) if file_path else None

        return DownloadResult(url=url, status="downloaded", file_path=file_path, video_id=video_id, meta=info)


def write_metadata_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

