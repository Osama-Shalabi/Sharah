from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from dotenv import load_dotenv

from config import get_settings
from services.downloader import VideoDownloader, write_metadata_json
from services.drive_uploader import DriveUploader, GoogleDriveAuthError, GoogleDriveUploadError
from services.facebook_scraper import FacebookReelsScraper
from services.storage import SQLiteStorage
from services.utils import ensure_dir, normalize_facebook_url, page_slug_from_url
from services.reels_indexer import sync_page_reels_to_db


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("cli")


async def cmd_fetch(args: argparse.Namespace) -> int:
    scraper = FacebookReelsScraper()
    result = await scraper.discover_urls(
        args.page_url,
        max_videos=args.max_videos,
        headless=args.headless,
        log=lambda lvl, msg: log.info(msg) if lvl in {"info", "debug"} else log.warning(msg),
    )
    out_root = Path(args.output)
    page_dir = ensure_dir(out_root / result.page_slug)
    write_metadata_json(page_dir / "discovered_urls.json", result.video_urls)
    log.info("Discovered %d URLs -> %s", len(result.video_urls), str(page_dir / "discovered_urls.json"))
    for u in result.video_urls:
        print(u)
    return 0


async def cmd_fetch_and_download(args: argparse.Namespace) -> int:
    storage = SQLiteStorage(Path(args.state_db))
    scraper = FacebookReelsScraper()
    downloader = VideoDownloader()
    uploader: DriveUploader | None = None
    if args.upload_to_drive:
        s = get_settings()
        uploader = DriveUploader(
            client_secret_path=Path(args.gdrive_client_secret or s.gdrive_client_secret),
            token_path=Path(args.gdrive_token_path or s.gdrive_token_path),
            oauth_console=bool(args.gdrive_oauth_console),
        )

    result = await scraper.discover_urls(
        args.page_url,
        max_videos=args.max_videos,
        headless=args.headless,
        log=lambda lvl, msg: log.info(msg),
    )
    out_root = Path(args.output)
    page_dir = ensure_dir(out_root / result.page_slug)
    write_metadata_json(page_dir / "discovered_urls.json", result.video_urls)

    sem = asyncio.Semaphore(args.concurrency)
    all_meta = []
    downloaded = skipped = failed = filtered = 0

    async def worker(url: str) -> None:
        nonlocal downloaded, skipped, failed, filtered
        url = normalize_facebook_url(url)
        if storage.is_downloaded(url):
            if uploader is not None and not args.dry_run:
                rec = storage.get_download(url)
                if rec and rec.get("file_path"):
                    try:
                        up = await asyncio.to_thread(
                            uploader.upload_file,
                            Path(str(rec["file_path"])),
                            folder_id=(args.gdrive_folder_id or "").strip() or None,
                        )
                        log.info("Uploaded existing file to Drive: %s (%s)", up.name, up.file_id)
                    except (GoogleDriveAuthError, GoogleDriveUploadError, FileNotFoundError) as e:
                        log.error("Drive upload failed for existing file %s: %s", rec.get("file_path"), e)
                    return
            skipped += 1
            return
        async with sem:
            try:
                res = await asyncio.to_thread(
                    downloader.download_one,
                    url,
                    output_dir=page_dir,
                    quality=args.quality,
                    dry_run=args.dry_run,
                    since=args.since,
                )
                all_meta.append(res.meta)
                if res.status == "filtered":
                    filtered += 1
                    return
                if res.status == "dry_run":
                    skipped += 1
                    return
                downloaded += 1
                if res.file_path:
                    storage.mark_downloaded(url=url, video_id=res.video_id, file_path=res.file_path, meta=res.meta)
                    if uploader is not None and not args.dry_run:
                        try:
                            up = await asyncio.to_thread(
                                uploader.upload_file,
                                Path(res.file_path),
                                folder_id=(args.gdrive_folder_id or "").strip() or None,
                            )
                            log.info("Uploaded to Drive: %s (%s)", up.name, up.file_id)
                        except (GoogleDriveAuthError, GoogleDriveUploadError) as e:
                            log.error("Drive upload failed for %s: %s", res.file_path, e)
            except Exception as e:
                failed += 1
                log.error("Failed %s: %s", url, e)

    await asyncio.gather(*(worker(u) for u in result.video_urls))
    write_metadata_json(page_dir / "metadata.json", all_meta)
    log.info("Done: downloaded=%d skipped=%d failed=%d filtered=%d", downloaded, skipped, failed, filtered)
    return 0 if failed == 0 else 2


async def cmd_watch(args: argparse.Namespace) -> int:
    storage = SQLiteStorage(Path(args.state_db))
    scraper = FacebookReelsScraper()
    downloader = VideoDownloader()
    uploader: DriveUploader | None = None
    if args.upload_to_drive:
        s = get_settings()
        uploader = DriveUploader(
            client_secret_path=Path(args.gdrive_client_secret or s.gdrive_client_secret),
            token_path=Path(args.gdrive_token_path or s.gdrive_token_path),
            oauth_console=bool(args.gdrive_oauth_console),
        )

    sem = asyncio.Semaphore(args.concurrency)
    consecutive_errors = 0
    cycle = 0

    while True:
        cycle += 1
        log.info("Watch cycle %d starting.", cycle)
        try:
            result = await scraper.discover_urls(
                args.page_url,
                max_videos=args.max_videos,
                headless=args.headless,
                log=lambda lvl, msg: log.info(msg),
            )
            out_root = Path(args.output)
            page_dir = ensure_dir(out_root / result.page_slug)
            write_metadata_json(page_dir / "discovered_urls.json", result.video_urls)

            async def worker(url: str) -> None:
                url_n = normalize_facebook_url(url)
                if storage.is_downloaded(url_n):
                    if uploader is not None and not args.dry_run:
                        rec = storage.get_download(url_n)
                        if rec and rec.get("file_path"):
                            try:
                                up = await asyncio.to_thread(
                                    uploader.upload_file,
                                    Path(str(rec["file_path"])),
                                    folder_id=(args.gdrive_folder_id or "").strip() or None,
                                )
                                log.info("Uploaded existing file to Drive: %s (%s)", up.name, up.file_id)
                            except (GoogleDriveAuthError, GoogleDriveUploadError, FileNotFoundError) as e:
                                log.error("Drive upload failed for existing file %s: %s", rec.get("file_path"), e)
                    return
                async with sem:
                    res = await asyncio.to_thread(
                        downloader.download_one,
                        url_n,
                        output_dir=page_dir,
                        quality=args.quality,
                        dry_run=args.dry_run,
                        since=args.since,
                    )
                    if res.status in {"filtered", "dry_run"}:
                        return
                    if res.file_path:
                        storage.mark_downloaded(url=url_n, video_id=res.video_id, file_path=res.file_path, meta=res.meta)
                        if uploader is not None and not args.dry_run:
                            try:
                                up = await asyncio.to_thread(
                                    uploader.upload_file,
                                    Path(res.file_path),
                                    folder_id=(args.gdrive_folder_id or "").strip() or None,
                                )
                                log.info("Uploaded to Drive: %s (%s)", up.name, up.file_id)
                            except (GoogleDriveAuthError, GoogleDriveUploadError) as e:
                                log.error("Drive upload failed for %s: %s", res.file_path, e)

            await asyncio.gather(*(worker(u) for u in result.video_urls))
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            log.error("Watch cycle %d error: %s", cycle, e)
            if consecutive_errors >= args.max_consecutive_errors:
                return 2

        if args.max_cycles is not None and cycle >= args.max_cycles:
            return 0
        await asyncio.sleep(args.interval)


async def cmd_drive_auth(args: argparse.Namespace) -> int:
    s = get_settings()
    uploader = DriveUploader(
        client_secret_path=Path(args.gdrive_client_secret or s.gdrive_client_secret),
        token_path=Path(args.gdrive_token_path or s.gdrive_token_path),
        oauth_console=bool(args.gdrive_oauth_console),
    )
    await asyncio.to_thread(uploader.ensure_authenticated)
    log.info("Drive token saved to %s", str(uploader.token_path))
    return 0


async def cmd_sync_sharah_reels(args: argparse.Namespace) -> int:
    storage = SQLiteStorage(Path(args.state_db))
    try:
        res = await sync_page_reels_to_db(
            storage=storage,
            page_url="https://www.facebook.com/shadi.shirri/reels/",
            max_reels=args.max_reels,
            headless=args.headless,
            enrich_metadata=args.enrich_metadata,
            reset_existing=args.reset,
        )
        log.info("Sync result: %s", res)
        return 0
    finally:
        storage.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Public Facebook page/reels downloader (public content only).")
    sub = p.add_subparsers(dest="command", required=True)

    pf = sub.add_parser("fetch", help="Discover reel/video URLs from a public page/reels URL.")
    pf.add_argument("page_url")
    pf.add_argument("--max-videos", type=int, default=50)
    pf.add_argument("--output", default="downloads")
    pf.add_argument("--headless", action=argparse.BooleanOptionalAction, default=True)
    pf.set_defaults(func=cmd_fetch)

    ps = sub.add_parser("sync-sharah-reels", help="Discover and store reels from https://www.facebook.com/shadi.shirri/reels/ into SQLite.")
    ps.add_argument("--state-db", default="data/app_state.db")
    ps.add_argument("--max-reels", type=int, default=500)
    ps.add_argument("--headless", action=argparse.BooleanOptionalAction, default=True)
    ps.add_argument("--enrich-metadata", action=argparse.BooleanOptionalAction, default=True)
    ps.add_argument("--reset", action="store_true", help="Delete existing stored reels for this page before syncing.")
    ps.set_defaults(func=cmd_sync_sharah_reels)

    pfd = sub.add_parser("fetch-and-download", help="Discover URLs, then download them with yt-dlp.")
    pfd.add_argument("page_url")
    pfd.add_argument("--max-videos", type=int, default=50)
    pfd.add_argument("--output", default="downloads")
    pfd.add_argument("--quality", default="best")
    pfd.add_argument("--dry-run", action="store_true")
    pfd.add_argument("--headless", action=argparse.BooleanOptionalAction, default=True)
    pfd.add_argument("--since", default=None, help="ISO date (YYYY-MM-DD), best-effort")
    pfd.add_argument("--concurrency", type=int, default=2)
    pfd.add_argument("--state-db", default="data/app_state.db")
    pfd.add_argument("--upload-to-drive", action="store_true", help="Upload downloaded files to Google Drive (requires token)")
    pfd.add_argument("--gdrive-folder-id", default=None, help="Google Drive folder ID (optional)")
    pfd.add_argument("--gdrive-client-secret", default=None, help="Path to Google OAuth client secret JSON (optional)")
    pfd.add_argument("--gdrive-token-path", default=None, help="Path to store OAuth token JSON (optional)")
    pfd.add_argument("--gdrive-oauth-console", action=argparse.BooleanOptionalAction, default=False, help="Use console OAuth flow (no browser)")
    pfd.set_defaults(func=cmd_fetch_and_download)

    pw = sub.add_parser("watch", help="Periodically check the page for new reels and download only new ones.")
    pw.add_argument("page_url")
    pw.add_argument("--interval", type=int, default=600, help="Polling interval in seconds")
    pw.add_argument("--max-cycles", type=int, default=None, help="Stop after N cycles (optional)")
    pw.add_argument("--max-consecutive-errors", type=int, default=3)
    pw.add_argument("--max-videos", type=int, default=50)
    pw.add_argument("--output", default="downloads")
    pw.add_argument("--quality", default="best")
    pw.add_argument("--dry-run", action="store_true")
    pw.add_argument("--headless", action=argparse.BooleanOptionalAction, default=True)
    pw.add_argument("--since", default=None, help="ISO date (YYYY-MM-DD), best-effort")
    pw.add_argument("--concurrency", type=int, default=2)
    pw.add_argument("--state-db", default="data/app_state.db")
    pw.add_argument("--upload-to-drive", action="store_true", help="Upload downloaded files to Google Drive (requires token)")
    pw.add_argument("--gdrive-folder-id", default=None, help="Google Drive folder ID (optional)")
    pw.add_argument("--gdrive-client-secret", default=None, help="Path to Google OAuth client secret JSON (optional)")
    pw.add_argument("--gdrive-token-path", default=None, help="Path to store OAuth token JSON (optional)")
    pw.add_argument("--gdrive-oauth-console", action=argparse.BooleanOptionalAction, default=False, help="Use console OAuth flow (no browser)")
    pw.set_defaults(func=cmd_watch)

    pa = sub.add_parser("drive-auth", help="Authenticate Google Drive OAuth and write a token JSON file.")
    pa.add_argument("--gdrive-client-secret", default=None, help="Path to Google OAuth client secret JSON (optional)")
    pa.add_argument("--gdrive-token-path", default=None, help="Path to store OAuth token JSON (optional)")
    pa.add_argument("--gdrive-oauth-console", action=argparse.BooleanOptionalAction, default=False, help="Use console OAuth flow (no browser)")
    pa.set_defaults(func=cmd_drive_auth)

    return p


def main() -> int:
    load_dotenv()
    args = build_parser().parse_args()
    return asyncio.run(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
