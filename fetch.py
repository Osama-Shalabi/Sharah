import requests
from openpyxl import Workbook
from dotenv import load_dotenv

from config import get_settings

# --- CONFIGURATION ---
OUTPUT_FILE = "facebook_reels_urls.xlsx"

def get_page_reels(page_id, access_token):
    """Fetch all Reels permalink URLs from a Facebook page using the Graph API."""
    url = f"https://graph.facebook.com/v19.0/{page_id}/video_reels"
    reels_urls = []
    params = {
        'access_token': access_token,
        'fields': 'id,permalink_url',
        'limit': 100
    }

    while True:
        try:
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            for reel in data.get('data', []):
                if 'permalink_url' in reel:
                    reels_urls.append(reel['permalink_url'])

            # Pagination
            if 'paging' in data and 'next' in data['paging']:
                url = data['paging']['next']
                params = {}   # next URL already contains all parameters
            else:
                break
        except requests.exceptions.RequestException as e:
            print(f"API request error: {e}")
            break

    return reels_urls

def save_to_excel(urls, filename):
    """Save a list of URLs to an Excel file with hyperlinks."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Facebook Reels"
    ws.append(["#", "Reel URL"])

    for i, url in enumerate(urls, 1):
        ws.append([i, url])
        ws.cell(row=i+1, column=2).hyperlink = url

    wb.save(filename)
    print(f"Saved {len(urls)} URLs to {filename}")

if __name__ == "__main__":
    load_dotenv()
    settings = get_settings()
    if not settings.fb_page_id or not settings.fb_page_access_token:
        raise SystemExit("Missing FB_PAGE_ID / FB_PAGE_ACCESS_TOKEN in .env")

    print("Fetching Reels URLs via Graph API...")
    reels = get_page_reels(settings.fb_page_id, settings.fb_page_access_token)
    if reels:
        save_to_excel(reels, OUTPUT_FILE)
    else:
        print("No Reels found. Check Page ID and Access Token.")
