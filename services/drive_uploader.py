from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence


class GoogleDriveAuthError(RuntimeError):
    pass


class GoogleDriveUploadError(RuntimeError):
    pass


DEFAULT_SCOPES: Sequence[str] = ("https://www.googleapis.com/auth/drive.file",)


@dataclass(frozen=True)
class DriveUploadResult:
    file_id: str
    name: str
    web_view_link: Optional[str]


class DriveUploader:
    def __init__(
        self,
        *,
        client_secret_path: Path,
        token_path: Path,
        scopes: Sequence[str] = DEFAULT_SCOPES,
        oauth_console: bool = False,
    ):
        self.client_secret_path = Path(client_secret_path)
        self.token_path = Path(token_path)
        self.scopes = tuple(scopes)
        self.oauth_console = oauth_console
        self._service = None

    def ensure_authenticated(self) -> None:
        self._get_service(interactive=True)

    def upload_file(self, file_path: Path, *, folder_id: Optional[str] = None) -> DriveUploadResult:
        file_path = Path(file_path)
        if not file_path.exists():
            raise GoogleDriveUploadError(f"File not found: {file_path}")

        service = self._get_service(interactive=False)

        try:
            # Lazy imports so non-Drive usage doesn't require these deps installed.
            from googleapiclient.http import MediaFileUpload  # type: ignore
        except Exception as e:  # pragma: no cover
            raise GoogleDriveUploadError(
                "Missing Google Drive dependencies. Install: "
                "`python3 -m pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib` "
                "(on Debian/Ubuntu you may need `--break-system-packages` or a virtualenv with `python3-venv`)."
            ) from e

        metadata = {"name": file_path.name}
        if folder_id:
            metadata["parents"] = [folder_id]

        media = MediaFileUpload(str(file_path), resumable=True)
        try:
            created = (
                service.files()
                .create(body=metadata, media_body=media, fields="id,name,webViewLink")
                .execute()
            )
        except Exception as e:
            raise GoogleDriveUploadError(str(e)) from e

        return DriveUploadResult(
            file_id=str(created.get("id")),
            name=str(created.get("name") or file_path.name),
            web_view_link=created.get("webViewLink"),
        )

    def _get_service(self, *, interactive: bool):
        if self._service is not None:
            return self._service

        if not self.client_secret_path.exists():
            raise GoogleDriveAuthError(
                f"Google client secret not found: {self.client_secret_path}. Set GDRIVE_CLIENT_SECRET."
            )

        try:
            # Lazy imports so non-Drive usage doesn't require these deps installed.
            from google.auth.transport.requests import Request  # type: ignore
            from google.oauth2.credentials import Credentials  # type: ignore
            from google_auth_oauthlib.flow import InstalledAppFlow  # type: ignore
            from googleapiclient.discovery import build  # type: ignore
        except Exception as e:  # pragma: no cover
            raise GoogleDriveAuthError(
                "Missing Google Drive dependencies. Install: "
                "`python3 -m pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib` "
                "(on Debian/Ubuntu you may need `--break-system-packages` or a virtualenv with `python3-venv`)."
            ) from e

        creds = None
        if self.token_path.exists():
            try:
                creds = Credentials.from_authorized_user_file(str(self.token_path), list(self.scopes))
            except Exception:
                creds = None

        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())

        if not creds or not creds.valid:
            if not interactive:
                raise GoogleDriveAuthError(
                    f"No valid Drive token at {self.token_path}. Run `python3 main.py drive-auth` once to create it."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(self.client_secret_path), list(self.scopes))

            last_error: Exception | None = None
            for attempt in range(1, 6):
                try:
                    if self.oauth_console:
                        creds = flow.run_console()
                    else:
                        creds = flow.run_local_server(port=0)
                    break
                except Exception as e:
                    last_error = e
                    msg = str(e).lower()
                    # `socket.gaierror: Temporary failure in name resolution` tends to be transient in some VMs.
                    if "temporary failure in name resolution" in msg or "name or service not known" in msg:
                        import time

                        time.sleep(min(2**attempt, 10))
                        continue
                    raise

            if not creds or not getattr(creds, "valid", False):
                raise GoogleDriveAuthError(f"Drive OAuth did not complete. Last error: {last_error}") from last_error

            self.token_path.parent.mkdir(parents=True, exist_ok=True)
            self.token_path.write_text(creds.to_json(), encoding="utf-8")

        self._service = build("drive", "v3", credentials=creds, cache_discovery=False)
        return self._service
