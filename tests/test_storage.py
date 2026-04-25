from pathlib import Path

from services.storage import SQLiteStorage


def test_storage_dedupe_downloaded(tmp_path: Path):
    db = SQLiteStorage(tmp_path / "state.db")
    url = "https://www.facebook.com/reel/123"
    assert db.is_downloaded(url) is False
    db.mark_downloaded(url=url, video_id="123", file_path="x.mp4", meta={"id": "123"})
    assert db.is_downloaded(url) is True
    rec = db.get_download(url)
    assert rec is not None
    assert rec["file_path"] == "x.mp4"
