from __future__ import annotations

import asyncio
import re
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

_REEL_ID_RE = re.compile(r"(?:https?:\/\/(?:www\.)?facebook\.com)?\/reel\/([0-9]+)", re.IGNORECASE)


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

            # Capture reel IDs from JSON responses (DOM can be virtualized).
            discovered: Set[str] = set()

            async def process_response(resp) -> None:
                try:
                    ct = (resp.headers or {}).get("content-type", "")
                    if "graphql" not in resp.url and "application/json" not in ct:
                        return
                    txt = await resp.text()
                except Exception:
                    return
                gained = 0
                for m in _REEL_ID_RE.finditer(txt or ""):
                    rid = m.group(1)
                    u = f"https://www.facebook.com/reel/{rid}"
                    if u in discovered:
                        continue
                    discovered.add(u)
                    gained += 1
                if gained:
                    _log("debug", f"Captured {gained} reel URLs from network responses.")

            page.on("response", lambda r: asyncio.create_task(process_response(r)))

            await page.goto(page_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(500)

            html = await page.content()
            if _looks_like_login_wall(page.url, html):
                await browser.close()
                raise PublicContentUnavailableError(
                    "Facebook requires login or blocked access for this page. Only public content is supported."
                )

            no_new = 0

            async def collect_once() -> int:
                hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
                candidates = _extract_candidate_hrefs(hrefs)
                before = len(discovered)
                discovered.update(candidates)
                return len(discovered) - before

            gained = await collect_once()
            _log("info", f"Found {len(discovered)} candidate video URLs (delta {gained}).")

            async def try_click_more() -> bool:
                selectors = [
                    'div[role="button"]:has-text("عرض المزيد")',
                    'div[role="button"]:has-text("عرض المزيد من النتائج")',
                    'div[role="button"]:has-text("See more")',
                    'div[role="button"]:has-text("Show more")',
                    'a[role="button"]:has-text("عرض المزيد")',
                    'a[role="button"]:has-text("See more")',
                ]
                for sel in selectors:
                    try:
                        loc = page.locator(sel).first
                        if await loc.count() == 0:
                            continue
                        await loc.click(timeout=1500)
                        return True
                    except Exception:
                        continue
                return False

            while len(discovered) < max_videos and no_new < max_no_new_scrolls:
                # Facebook pages can be sensitive to scroll mechanics; use multiple strategies.
                await page.mouse.wheel(0, 2600)
                await page.keyboard.press("End")
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(int(scroll_pause_s * 1000))
                try:
                    await page.wait_for_load_state("networkidle", timeout=2_500)
                except Exception:
                    pass

                gained = await collect_once()
                if gained == 0:
                    no_new += 1
                    # Sometimes a "more" button is required.
                    if no_new % 5 == 0:
                        clicked = await try_click_more()
                        if clicked:
                            await page.wait_for_timeout(int(scroll_pause_s * 1000))
                            gained2 = await collect_once()
                            if gained2 > 0:
                                no_new = 0
                    _log("debug", f"No new URLs on this scroll (streak {no_new}/{max_no_new_scrolls}).")
                else:
                    no_new = 0
                    _log("info", f"Found {len(discovered)} candidate video URLs (delta {gained}).")

            await browser.close()

        urls = list(sorted(discovered))[:max_videos]
        _log("info", f"Discovery complete: {len(urls)} URLs.")
        return DiscoverResult(page_slug=page_slug, page_url=page_url, video_urls=urls)
