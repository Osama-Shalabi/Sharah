from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_state_db: Path = Path("data/app_state.db")
    sharah_reels_xlsx: Path = Path("shadi_shirri_reels (Copy).xlsx")
    sharah_reels_source: str = "auto"  # auto | excel | db

    # Optional: Facebook Graph API (Page reels/videos indexing)
    fb_graph_api_version: str = "v20.0"
    fb_page_id: str = ""
    fb_page_access_token: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
