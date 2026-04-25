from api.models import WatchOptions


def test_watch_options_defaults():
    o = WatchOptions()
    assert o.interval_s == 600
    assert o.max_cycles is None
    assert o.max_consecutive_errors == 3

