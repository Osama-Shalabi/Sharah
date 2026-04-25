from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlparse, urlunparse, urlencode


FB_HOSTS = {"facebook.com", "www.facebook.com", "m.facebook.com", "mbasic.facebook.com"}


class PublicContentUnavailableError(RuntimeError):
    pass


class UnsupportedUrlError(ValueError):
    pass


def _strip_trailing_slash(path: str) -> str:
    return path.rstrip("/") if path != "/" else path


def normalize_facebook_url(raw_url: str) -> str:
    raw_url = (raw_url or "").strip()
    if not raw_url:
        raise UnsupportedUrlError("URL is empty.")

    parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https"}:
        raise UnsupportedUrlError("URL must start with http:// or https://")

    host = (parsed.netloc or "").lower()
    if host not in FB_HOSTS:
        raise UnsupportedUrlError("Only facebook.com URLs are supported.")

    path = _strip_trailing_slash(parsed.path or "/")

    # Keep only essential query params (watch?v=...).
    query = {}
    if path.endswith("/watch") or path == "/watch":
        qs = parse_qs(parsed.query or "")
        if "v" in qs and qs["v"]:
            query["v"] = qs["v"][0]

    normalized = urlunparse(("https", "www.facebook.com", path, "", urlencode(query), ""))
    return normalized


def is_supported_page_url(url: str) -> bool:
    u = normalize_facebook_url(url)
    path = urlparse(u).path.lower().rstrip("/") + "/"
    return path.endswith("/reels/") or path.endswith("/videos/") or path.endswith("/watch/")


def is_supported_video_url(url: str) -> bool:
    u = normalize_facebook_url(url)
    path = urlparse(u).path.lower()
    if "/reel/" in path or path.startswith("/reel/"):
        return True
    if "/videos/" in path:
        return True
    if path == "/watch":
        return "v=" in urlparse(u).query
    return False


_PAGE_SLUG_RE = re.compile(r"^/([^/]+)/", re.IGNORECASE)


def page_slug_from_url(page_url: str) -> str:
    u = normalize_facebook_url(page_url)
    path = urlparse(u).path
    m = _PAGE_SLUG_RE.match(path + "/")
    if m:
        slug = m.group(1)
    else:
        slug = "facebook_page"
    return safe_fs_name(slug)


def safe_fs_name(name: str, *, max_len: int = 80) -> str:
    name = (name or "").strip()
    if not name:
        return "untitled"
    name = re.sub(r"[^\w\-]+", "_", name, flags=re.UNICODE)
    name = re.sub(r"_+", "_", name).strip("._-")
    return (name[:max_len] or "untitled")


def ensure_dir(path: Path) -> Path:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except PermissionError as e:
        raise PermissionError(
            f"Permission denied creating directory: {path}. "
            "Choose a writable output folder or fix permissions (e.g. chown/chmod)."
        ) from e
    return path


@dataclass(frozen=True)
class DownloadStats:
    discovered: int = 0
    downloaded: int = 0
    uploaded: int = 0
    upload_failed: int = 0
    skipped: int = 0
    failed: int = 0
    filtered: int = 0
