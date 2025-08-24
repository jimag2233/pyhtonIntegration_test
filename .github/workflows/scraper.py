import os
import time
import logging
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from requests.adapters import HTTPAdapter, Retry

# ---- Logging ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---- Supabase setup ----
SUPABASE_URL = "https://sdhhajicibvvbvysfmtj.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNkaGhhamljaWJ2dmJ2eXNmbXRqIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDQyMjU2OCwiZXhwIjoyMDY5OTk4NTY4fQ.msfaReycugJznDqbH7Z7LvPNqmKwdKq-viLLHCLASQc"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---- Requests session with retries ----
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))

BASE_URL = "https://books.toscrape.com/catalogue/page-{}.html"

def scrape_page(page: int):
    """Scrape en sida med böcker"""
    url = BASE_URL.format(page)
    logging.info(f"Scraping page {page} -> {url}")

    try:
        res = session.get(url, timeout=20)
        res.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Request failed on page {page}: {e}")
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    items = []

    for book in soup.select("article.product_pod"):
        title = book.h3.a["title"]
        link = "https://books.toscrape.com/catalogue/" + book.h3.a["href"]

        items.append({
            "title": title,
            "url": link
        })

    logging.info(f"Found {len(items)} books on page {page}")
    return items

def scrape_all(max_pages=3):
    """Iterera genom flera sidor"""
    all_items = []
    for page in range(1, max_pages + 1):
        items = scrape_page(page)
        if not items:
            logging.info(f"No items found on page {page}, stopping.")
            break
        all_items.extend(items)
        time.sleep(1)  # vänlig mot servern
    return all_items

def save_to_supabase(items):
    """Spara data i Supabase och undvik dubbletter"""
    if not items:
        logging.info("No items to insert.")
        return

    for item in items:
        # Kontrollera om posten redan finns
        try:
            existing = supabase.table("articles").select("id").eq("url", item["url"]).execute()
            if existing.data:
                logging.info(f"Skipping duplicate: {item['url']}")
                continue

            response = supabase.table("articles").insert(item).execute()
            # Kontrollera om data finns
            if not response.data:
                logging.error(f"Failed to insert item: {item}")
            else:
                logging.info(f"Inserted: {item['title']}")

        except Exception as e:
            logging.error(f"Supabase exception: {e}")

if __name__ == "__main__":
    logging.info("Starting scraper job for Books to Scrape...")
    items = scrape_all(max_pages=3)  # Justera antal sidor efter behov
    save_to_supabase(items)
    logging.info("Scraper job finished ✅")
