from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Set
from urllib.parse import urlparse

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from services.utils import (
    PublicContentUnavailableError,
    normalize_facebook_url,
    page_slug_from_url,
)


LogFn = Callable[[str, str], None]  # (level, message)


@dataclass(frozen=True)
class DiscoverResult:
    page_slug: str
    page_url: str
    video_urls: List[str]


def _looks_like_login_wall(current_url: str, html: str) -> bool:
    u = (current_url or "").lower()
    if "facebook.com/login" in u:
        return True
    html_l = (html or "").lower()
    signals = [
        "log in",
        "create new account",
        "you must log in",
        "sign up",
        "see posts",
    ]
    return any(s in html_l for s in signals)


def _extract_candidate_hrefs(hrefs: Iterable[str]) -> Set[str]:
    out: Set[str] = set()
    for href in hrefs:
        if not href:
            continue
        if href.startswith("/"):
            href = "https://www.facebook.com" + href
        if "facebook.com" not in href:
            continue
        try:
            n = normalize_facebook_url(href)
        except Exception:
            continue

        path = urlparse(n).path.lower()
        if "/reel/" in path or path.startswith("/reel/") or "/videos/" in path or path == "/watch":
            out.add(n)
    return out


class FacebookReelsScraper:
    def __init__(self, *, default_timeout_ms: int = 30_000):
        self.default_timeout_ms = default_timeout_ms

    @retry(
        retry=retry_if_exception_type(PlaywrightTimeoutError),
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=8),
        reraise=True,
    )
    async def discover_urls(
        self,
        page_url: str,
        *,
        max_videos: int = 50,
        headless: bool = True,
        scroll_pause_s: float = 1.0,
        max_no_new_scrolls: int = 4,
        log: Optional[LogFn] = None,
    ) -> DiscoverResult:
        page_url = normalize_facebook_url(page_url)
        page_slug = page_slug_from_url(page_url)

        def _log(level: str, message: str) -> None:
            if log:
                log(level, message)

        _log("info", f"Opening page: {page_url}")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context()
            page = await context.new_page()
            page.set_default_timeout(self.default_timeout_ms)

            await page.goto(page_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(500)

            html = await page.content()
            if _looks_like_login_wall(page.url, html):
                await browser.close()
                raise PublicContentUnavailableError(
                    "Facebook requires login or blocked access for this page. Only public content is supported."
                )

            discovered: Set[str] = set()
            no_new = 0

            async def collect_once() -> int:
                hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
                candidates = _extract_candidate_hrefs(hrefs)
                before = len(discovered)
                discovered.update(candidates)
                return len(discovered) - before

            gained = await collect_once()
            _log("info", f"Found {len(discovered)} candidate video URLs (delta {gained}).")

            while len(discovered) < max_videos and no_new < max_no_new_scrolls:
                await page.mouse.wheel(0, 2000)
                await page.wait_for_timeout(int(scroll_pause_s * 1000))
                gained = await collect_once()
                if gained == 0:
                    no_new += 1
                    _log("debug", f"No new URLs on this scroll (streak {no_new}/{max_no_new_scrolls}).")
                else:
                    no_new = 0
                    _log("info", f"Found {len(discovered)} candidate video URLs (delta {gained}).")

            await browser.close()

        urls = list(sorted(discovered))[:max_videos]
        _log("info", f"Discovery complete: {len(urls)} URLs.")
        return DiscoverResult(page_slug=page_slug, page_url=page_url, video_urls=urls)

