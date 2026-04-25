from __future__ import annotations

import asyncio
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from services.downloader import VideoDownloader, write_metadata_json
from services.drive_uploader import DriveUploader, GoogleDriveAuthError, GoogleDriveUploadError
from services.facebook_scraper import FacebookReelsScraper
from services.storage import SQLiteStorage
from services.utils import DownloadStats, ensure_dir, normalize_facebook_url, safe_fs_name


class JobManager:
    def __init__(
        self,
        *,
        storage: SQLiteStorage,
        scraper: FacebookReelsScraper,
        downloader: VideoDownloader,
        default_output_root: Path,
        drive_uploader: DriveUploader | None = None,
    ):
        self.storage = storage
        self.scraper = scraper
        self.downloader = downloader
        self.default_output_root = Path(default_output_root)
        self.drive_uploader = drive_uploader
        self._tasks: Dict[str, asyncio.Task] = {}

    def _log(self, job_id: str, level: str, message: str) -> None:
        self.storage.append_log(job_id, level, message)

    def start_fetch_links(self, *, page_url: str, options: Dict[str, Any]) -> str:
        job_id = self.storage.create_job("fetch_links", page_url=page_url, options=options)
        self._tasks[job_id] = asyncio.create_task(self._run_fetch_links(job_id, page_url, options))
        return job_id

    def start_download(self, *, urls: List[str], options: Dict[str, Any]) -> str:
        options = {**options, "urls_count": len(urls), "page_slug": options.get("page_slug") or "manual"}
        job_id = self.storage.create_job(
            "download",
            page_url=None,
            options=options,
        )
        for u in urls:
            self.storage.add_job_item(job_id, normalize_facebook_url(u))
        self._tasks[job_id] = asyncio.create_task(self._run_download(job_id, options))
        return job_id

    def start_fetch_and_download(self, *, page_url: str, options: Dict[str, Any]) -> str:
        job_id = self.storage.create_job("fetch_and_download", page_url=page_url, options=options)
        self._tasks[job_id] = asyncio.create_task(self._run_fetch_and_download(job_id, page_url, options))
        return job_id

    def start_watch(self, *, page_url: str, options: Dict[str, Any]) -> str:
        job_id = self.storage.create_job("watch", page_url=page_url, options=options)
        self._tasks[job_id] = asyncio.create_task(self._run_watch(job_id, page_url, options))
        return job_id

    async def _run_fetch_links(self, job_id: str, page_url: str, options: Dict[str, Any]) -> None:
        self.storage.set_job_status(job_id, "running")
        stats = DownloadStats()
        try:
            result = await self.scraper.discover_urls(
                page_url,
                max_videos=int(options.get("max_videos", 50)),
                headless=bool(options.get("headless", True)),
                log=lambda lvl, msg: self._log(job_id, lvl, msg),
            )
            for u in result.video_urls:
                self.storage.add_job_item(job_id, u)
            stats = DownloadStats(discovered=len(result.video_urls))

            out_root = Path(options.get("output") or self.default_output_root)
            page_dir = ensure_dir(out_root / result.page_slug)
            write_metadata_json(page_dir / "discovered_urls.json", result.video_urls)

            self.storage.set_job_status(job_id, "completed", stats=asdict(stats))
        except Exception as e:
            self._log(job_id, "error", str(e))
            self.storage.set_job_status(job_id, "failed", error=str(e), stats=asdict(stats))

    async def _run_download(self, job_id: str, options: Dict[str, Any]) -> None:
        self.storage.set_job_status(job_id, "running")
        stats = DownloadStats()
        try:
            urls = self.storage.list_job_item_urls(job_id)
            stats = await self._download_urls(job_id, urls, options, page_slug=options.get("page_slug"))
            self.storage.set_job_status(job_id, "completed", stats=asdict(stats))
        except Exception as e:
            self._log(job_id, "error", str(e))
            self.storage.set_job_status(job_id, "failed", error=str(e), stats=asdict(stats))

    async def _run_fetch_and_download(self, job_id: str, page_url: str, options: Dict[str, Any]) -> None:
        self.storage.set_job_status(job_id, "running")
        stats = DownloadStats()
        try:
            result = await self.scraper.discover_urls(
                page_url,
                max_videos=int(options.get("max_videos", 50)),
                headless=bool(options.get("headless", True)),
                log=lambda lvl, msg: self._log(job_id, lvl, msg),
            )
            for u in result.video_urls:
                self.storage.add_job_item(job_id, u)

            out_root = Path(options.get("output") or "downloads")
            page_dir = ensure_dir(out_root / result.page_slug)
            write_metadata_json(page_dir / "discovered_urls.json", result.video_urls)

            stats = await self._download_urls(job_id, result.video_urls, options, page_slug=result.page_slug)
            self.storage.set_job_status(job_id, "completed", stats=asdict(stats))
        except Exception as e:
            self._log(job_id, "error", str(e))
            self.storage.set_job_status(job_id, "failed", error=str(e), stats=asdict(stats))

    async def _run_watch(self, job_id: str, page_url: str, options: Dict[str, Any]) -> None:
        self.storage.set_job_status(job_id, "running")
        interval_s = int(options.get("interval_s", 600))
        max_cycles = options.get("max_cycles")
        max_cycles = int(max_cycles) if max_cycles is not None else None
        max_consecutive_errors = int(options.get("max_consecutive_errors", 3))

        cycle = 0
        consecutive_errors = 0
        total = DownloadStats()

        try:
            while True:
                cycle += 1
                self._log(job_id, "info", f"Watch cycle {cycle} starting.")
                try:
                    result = await self.scraper.discover_urls(
                        page_url,
                        max_videos=int(options.get("max_videos", 50)),
                        headless=bool(options.get("headless", True)),
                        log=lambda lvl, msg: self._log(job_id, lvl, msg),
                    )
                    for u in result.video_urls:
                        self.storage.add_job_item(job_id, u)

                    out_root = Path(options.get("output") or self.default_output_root)
                    page_dir = ensure_dir(out_root / result.page_slug)
                    write_metadata_json(page_dir / "discovered_urls.json", result.video_urls)

                    stats = await self._download_urls(job_id, result.video_urls, options, page_slug=result.page_slug)
                    total = DownloadStats(
                        discovered=total.discovered + stats.discovered,
                        downloaded=total.downloaded + stats.downloaded,
                        uploaded=total.uploaded + stats.uploaded,
                        upload_failed=total.upload_failed + stats.upload_failed,
                        skipped=total.skipped + stats.skipped,
                        failed=total.failed + stats.failed,
                        filtered=total.filtered + stats.filtered,
                    )
                    self.storage.set_job_status(job_id, "running", stats=asdict(total))
                    consecutive_errors = 0
                except Exception as e:
                    consecutive_errors += 1
                    self._log(job_id, "error", f"Watch cycle {cycle} error: {e}")
                    if consecutive_errors >= max_consecutive_errors:
                        raise

                if max_cycles is not None and cycle >= max_cycles:
                    break
                await asyncio.sleep(interval_s)

            self._log(job_id, "info", "Watch complete.")
            self.storage.set_job_status(job_id, "completed", stats=asdict(total))
        except Exception as e:
            self.storage.set_job_status(job_id, "failed", error=str(e), stats=asdict(total))

    async def _download_urls(
        self,
        job_id: str,
        urls: List[str],
        options: Dict[str, Any],
        *,
        page_slug: Optional[str],
    ) -> DownloadStats:
        out_root = Path(options.get("output") or self.default_output_root)
        slug = safe_fs_name(page_slug) if page_slug else "manual"
        page_dir = ensure_dir(out_root / slug)

        discovered = len(urls)
        downloaded = uploaded = upload_failed = skipped = failed = filtered = 0
        quality = str(options.get("quality") or "best")
        dry_run = bool(options.get("dry_run", False))
        concurrency = max(1, int(options.get("concurrency", 2)))
        since = options.get("since")
        upload_to_drive = bool(options.get("upload_to_drive", False))
        gdrive_folder_id = (options.get("gdrive_folder_id") or "").strip() or None

        semaphore = asyncio.Semaphore(concurrency)
        all_meta: List[Dict[str, Any]] = []

        async def worker(url: str) -> None:
            nonlocal downloaded, uploaded, upload_failed, skipped, failed, filtered
            url = normalize_facebook_url(url)
            if self.storage.is_downloaded(url):
                rec = self.storage.get_download(url)
                if (
                    upload_to_drive
                    and not dry_run
                    and rec
                    and rec.get("file_path")
                    and self.drive_uploader is not None
                ):
                    try:
                        file_path = Path(str(rec["file_path"]))
                        self.storage.update_job_item(job_id, url, status="uploading", file_path=str(file_path), meta=rec.get("meta") or {})
                        up = await asyncio.to_thread(
                            self.drive_uploader.upload_file,
                            file_path,
                            folder_id=gdrive_folder_id,
                        )
                        uploaded += 1
                        meta = dict(rec.get("meta") or {})
                        meta["gdrive"] = {"file_id": up.file_id, "name": up.name, "web_view_link": up.web_view_link}
                        self.storage.update_job_item(job_id, url, status="uploaded", file_path=str(file_path), meta=meta)
                        self._log(job_id, "info", f"Uploaded existing file to Drive: {up.name} ({up.file_id})")
                        return
                    except (GoogleDriveAuthError, GoogleDriveUploadError, FileNotFoundError) as e:
                        upload_failed += 1
                        self.storage.update_job_item(job_id, url, status="upload_failed", meta=rec.get("meta") or {}, error=str(e))
                        self._log(job_id, "error", f"Drive upload failed (existing file): {e}")
                        return

                skipped += 1
                self.storage.update_job_item(job_id, url, status="skipped", file_path=(rec or {}).get("file_path"), meta=(rec or {}).get("meta") or None)
                return

            async with semaphore:
                self.storage.update_job_item(job_id, url, status="downloading")
                self._log(job_id, "info", f"Downloading: {url}")
                try:
                    result = await asyncio.to_thread(
                        self.downloader.download_one,
                        url,
                        output_dir=page_dir,
                        quality=quality,
                        dry_run=dry_run,
                        since=since,
                        log=lambda lvl, msg: self._log(job_id, lvl, msg),
                    )
                    all_meta.append(result.meta)
                    if result.status == "filtered":
                        filtered += 1
                        self.storage.update_job_item(job_id, url, status="filtered", meta=result.meta)
                        return
                    if result.status == "dry_run":
                        skipped += 1
                        self.storage.update_job_item(job_id, url, status="dry_run", meta=result.meta)
                        return

                    downloaded += 1
                    self.storage.update_job_item(
                        job_id,
                        url,
                        status="downloaded",
                        file_path=result.file_path,
                        meta=result.meta,
                    )
                    if result.file_path:
                        self.storage.mark_downloaded(url=url, video_id=result.video_id, file_path=result.file_path, meta=result.meta)

                    if (
                        upload_to_drive
                        and not dry_run
                        and result.file_path
                        and self.drive_uploader is not None
                    ):
                        try:
                            self.storage.update_job_item(job_id, url, status="uploading", file_path=result.file_path, meta=result.meta)
                            up = await asyncio.to_thread(
                                self.drive_uploader.upload_file,
                                Path(result.file_path),
                                folder_id=gdrive_folder_id,
                            )
                            uploaded += 1
                            meta = dict(result.meta)
                            meta["gdrive"] = {"file_id": up.file_id, "name": up.name, "web_view_link": up.web_view_link}
                            self.storage.update_job_item(job_id, url, status="uploaded", file_path=result.file_path, meta=meta)
                            self._log(job_id, "info", f"Uploaded to Drive: {up.name} ({up.file_id})")
                        except (GoogleDriveAuthError, GoogleDriveUploadError) as e:
                            upload_failed += 1
                            self.storage.update_job_item(job_id, url, status="upload_failed", file_path=result.file_path, meta=result.meta, error=str(e))
                            self._log(job_id, "error", f"Drive upload failed: {e}")
                except Exception as e:
                    failed += 1
                    self.storage.update_job_item(job_id, url, status="failed", error=str(e))
                    self._log(job_id, "error", f"Failed: {url} ({e})")

        await asyncio.gather(*(worker(u) for u in urls))

        write_metadata_json(page_dir / "metadata.json", all_meta)
        return DownloadStats(
            discovered=discovered,
            downloaded=downloaded,
            uploaded=uploaded,
            upload_failed=upload_failed,
            skipped=skipped,
            failed=failed,
            filtered=filtered,
        )
