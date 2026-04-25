## Public Facebook Reels/Page Downloader (Playwright + yt-dlp)

Production-oriented Python app that:

1. Accepts a **public** Facebook page/reels URL (example: `https://www.facebook.com/shadi.shirri/reels/`)
2. Uses **Playwright** to load/scroll the page and collect reel/video post URLs
3. Uses **yt-dlp** (+ **FFmpeg** when needed) to download each discovered URL
4. Prevents duplicates using a local **SQLite** database
5. Provides both a **CLI** and a small **web UI** (FastAPI)

### Compliance / Safety

- Public content only.
- No login flows, no cookie/session theft, no captcha bypass, no private content access, no paywall/access-control bypass.
- If Facebook requires login or blocks access, the app fails gracefully with a clear error.
- You are responsible for complying with Facebook’s terms and applicable laws.

## Project structure

```
.
├─ api/
│  ├─ app.py
│  ├─ models.py
│  ├─ templates/index.html
│  └─ static/{app.js,styles.css}
├─ services/
│  ├─ facebook_scraper.py
│  ├─ downloader.py
│  ├─ storage.py
│  ├─ jobs.py
│  └─ utils.py
├─ tests/
├─ main.py
├─ requirements.txt
├─ Dockerfile
├─ docker-compose.yml
└─ .env.example
```

## Setup (Windows / local)

Requirements:

- Python 3.11+
- FFmpeg installed and on `PATH` (Windows: install via `winget install Gyan.FFmpeg`)

Install:

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
python -m playwright install chromium
```

Run the web app:

```bash
uvicorn api.app:app --reload
```

Open `http://127.0.0.1:8000/`.

### Environment variables

Copy `.env.example` to `.env` to customize:

- `APP_STATE_DB` (default: `data/app_state.db`)
- `DOWNLOADS_ROOT` (default: `downloads`)

## Setup (Docker)

```bash
docker compose up --build
```

Open `http://127.0.0.1:8000/`.

## CLI usage

Discover URLs (does not download):

```bash
python main.py fetch "https://www.facebook.com/shadi.shirri/reels/" --max-videos 50 --output ./downloads
```

Fetch + download:

```bash
python main.py fetch-and-download "https://www.facebook.com/shadi.shirri/reels/" --max-videos 50 --output ./downloads --quality best --concurrency 2
```

Watch mode (polls for new reels and downloads only new ones):

```bash
python main.py watch "https://www.facebook.com/shadi.shirri/reels/" --interval 600 --max-videos 50 --output ./downloads
```

## Upload to Google Drive (optional)

This project can upload the downloaded video files to Google Drive using the Drive API (OAuth “installed app” flow).

1) Create Google OAuth credentials (Desktop app) and download the client secret JSON.

2) Set env vars (or pass CLI flags):

- `GDRIVE_CLIENT_SECRET=path/to/client_secret.json`
- `GDRIVE_TOKEN_PATH=data/gdrive_token.json` (default)
- Optional default destination folder: `GDRIVE_FOLDER_ID=...`

3) Authenticate once (creates the token JSON):

```bash
python3 main.py drive-auth
```

4) Download + upload:

```bash
python3 main.py fetch-and-download "https://www.facebook.com/<page>/reels/" --upload-to-drive --gdrive-folder-id "<FOLDER_ID>"
```

Notes:
- For Docker/server runs, generate the token on your machine first, then mount the `data/` folder into the container (the default `docker-compose.yml` already mounts `./data:/app/data`).
- If you can’t open a browser on the machine doing auth, use console auth: `python3 main.py drive-auth --gdrive-oauth-console`.

## API

- `POST /fetch-links`
- `POST /download`
- `POST /fetch-and-download`
- `POST /watch`
- `GET /jobs/{job_id}`
- `GET /health`

## Outputs

For a page like `.../shadi.shirri/reels/`:

- `downloads/shadi_shirri/discovered_urls.json`
- `downloads/shadi_shirri/metadata.json`
- downloaded video files in the same folder
- persistent state DB: `data/app_state.db`

## How it works

1. Playwright loads the reels/page URL, scrolls, and extracts candidate reel/video links from `<a href="...">`.
2. Discovered URLs are de-duplicated and persisted per job.
3. yt-dlp downloads each URL (best quality by default) and writes metadata.
4. The SQLite DB prevents re-downloading the same canonical URL.
5. Watch mode repeats discovery on an interval and only downloads URLs not yet present in `data/app_state.db`.

## Known limitations

- Facebook markup changes frequently; discovery can break.
- Some pages or regions require login even for public content; in that case, the tool will fail with a clear error.
- Date filtering is best-effort and depends on `upload_date` being available via yt-dlp extraction.
