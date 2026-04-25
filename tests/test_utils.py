from services.utils import normalize_facebook_url, page_slug_from_url


def test_normalize_watch_keeps_v_param():
    u = normalize_facebook_url("https://www.facebook.com/watch/?v=123&__cft__=abc")
    assert u == "https://www.facebook.com/watch?v=123"


def test_page_slug_from_url():
    assert page_slug_from_url("https://www.facebook.com/shadi.shirri/reels/") == "shadi_shirri"

