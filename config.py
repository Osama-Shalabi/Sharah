from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_state_db: Path = Path("data/app_state.db")
    downloads_root: Path = Path("downloads")

    # Optional: Google Drive uploads (OAuth "installed app" flow)
    gdrive_client_secret: Path = Path("client_secret.json")
    gdrive_token_path: Path = Path("data/gdrive_token.json")
    gdrive_folder_id: str = ""
    gdrive_oauth_console: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()
