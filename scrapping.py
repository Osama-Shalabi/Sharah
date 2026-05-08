import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from openpyxl import Workbook

# --- CONFIG ---
PROFILE_URL = "https://www.facebook.com/shadi.shirri/reels"   # change to your username
OUTPUT_FILE = "facebook_reels_urls.xlsx"
SCROLL_PAUSE = 6
MAX_SCROLLS = 15

def get_my_reels(url):
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    # Reduce automation flags
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])

    driver = webdriver.Chrome(service=service, options=options)
    urls = set()

    try:
        driver.get(url)
        print("Log in manually in the browser window, then press Enter here...")
        input()  # Wait for you to log in and load the Reels tab

        for i in range(MAX_SCROLLS):
            print(f"Scroll {i+1}/{MAX_SCROLLS}")
            # Find all links that contain "/reel/" in href
            links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/reel/"], a[href*="/reels/"]')
            for link in links:
                href = link.get_attribute('href')
                if href:
                    # Keep only the base URL, remove tracking
                    clean = href.split('?')[0]
                    urls.add(clean)

            # Scroll down
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE)

        print(f"Found {len(urls)} unique Reels URLs.")
        return list(urls)

    except Exception as e:
        print(f"Error: {e}")
        return []
    finally:
        driver.quit()

def save_to_excel(urls, filename):
    wb = Workbook()
    ws = wb.active
    ws.title = "My Reels"
    ws.append(["#", "URL"])
    for i, url in enumerate(urls, 1):
        ws.append([i, url])
        ws.cell(row=i+1, column=2).hyperlink = url
    wb.save(filename)
    print(f"Saved {len(urls)} URLs to {filename}")

if __name__ == "__main__":
    reel_urls = get_my_reels(PROFILE_URL)
    if reel_urls:
        save_to_excel(reel_urls, OUTPUT_FILE)
