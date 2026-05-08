#!/usr/bin/env python3

import sys
import re
import html
import requests
from bs4 import BeautifulSoup


def clean_title(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"\s+", " ", text).strip()

    # Facebook titles often end with these suffixes
    text = re.sub(r"\s*\|\s*Facebook$", "", text, flags=re.I)
    text = re.sub(r"\s+on Facebook$", "", text, flags=re.I)

    return text.strip()


def get_meta_content(soup: BeautifulSoup, *names: str) -> str | None:
    for name in names:
        tag = soup.find("meta", attrs={"property": name})
        if tag and tag.get("content"):
            return tag["content"]

        tag = soup.find("meta", attrs={"name": name})
        if tag and tag.get("content"):
            return tag["content"]

    return None


def get_facebook_reel_title(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    response = requests.get(url, headers=headers, timeout=20, allow_redirects=True)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    candidates = [
        get_meta_content(soup, "og:title"),
        get_meta_content(soup, "twitter:title"),
        get_meta_content(soup, "description", "og:description", "twitter:description"),
        soup.title.string if soup.title else None,
    ]

    for candidate in candidates:
        title = clean_title(candidate)
        if title and title.lower() not in {
            "facebook",
            "facebook reels",
            "log into facebook",
            "watch",
        }:
            return title

    raise RuntimeError(
        "Could not extract a public title. The reel may require login, "
        "be private, region-blocked, or Facebook may not expose the title in metadata."
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python fb_reel_title.py <facebook_reel_url>")
        sys.exit(1)

    reel_url = sys.argv[1]

    try:
        title = get_facebook_reel_title(reel_url)
        print(title)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
