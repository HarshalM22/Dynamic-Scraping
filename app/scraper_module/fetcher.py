import requests
import time
import os
import json
import re
from urllib.parse import urlparse, urljoin
from playwright.sync_api import sync_playwright, Page
from config.settings import SETTINGS 
from app.scraper_module.parser import parse_html_content, parse_api_content, extract_links
from app.scraper_module.filter import is_social_url

# -------------------------------------------------
# GLOBAL CONFIG
# -------------------------------------------------

DOWNLOADABLE_EXTENSIONS = (
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.csv', '.zip'
)


DOWNLOAD_DIR = "my_downloads"

PRICE_KEYWORDS = [
    "price", "pricing", "charges", "charge", "cost",
    "rates", "rate", "fee", "fees", "tariff",
    "subscription", "plan", "transparency",
    "chargemaster", "standard", "machine", "readable",
    "shoppable", "service", "services", "mrf", "cdm","click here","download","policy"
]

PROCESSED_FILE_URLS = set()
GLOBAL_CLICKED_ELEMENTS = set()

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def normalize_url(url: str) -> str:
    """Remove query params and fragments for deduping."""
    return url.split("?")[0].split("#")[0]

def is_price_related_url(url: str) -> bool:
    u = url.lower()
    return any(k in u for k in PRICE_KEYWORDS)

def is_price_related_file(url: str) -> bool:
    name = urlparse(url).path.lower()
    return any(k in name for k in PRICE_KEYWORDS)

def is_downloadable_file_link(url: str) -> bool:
    path = urlparse(url).path.lower()
    return path.endswith(DOWNLOADABLE_EXTENSIONS)



# -------------------------------------------------
# FILE DOWNLOAD
# -------------------------------------------------

def download_file(file_url: str) -> str | None:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    parsed = urlparse(file_url)
    filename = os.path.basename(parsed.path)
    filename = filename.split("?")[0]
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    try:
        response = requests.get(
            file_url,
            stream=True,
            headers={"User-Agent": SETTINGS.USER_AGENT},
            timeout=SETTINGS.DEFAULT_TIMEOUT_MS / 1000
        )
        response.raise_for_status()

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)

        print(f"    ✅ Downloaded: {filename}")
        return file_path

    except Exception as e:
        print(f"    ❌ Error downloading {file_url}: {e}")
        return None

# -------------------------------------------------
# PDF + DOC PROCESSING
# -------------------------------------------------

def process_pdf_with_plumber(path: str):
    import pdfplumber
    data = {"content_text": "", "content_tables": []}

    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                txt = page.extract_text() or ""
                data["content_text"] += txt + "\n"
                tables = page.extract_tables() or []
                data["content_tables"].extend(tables)

        return data

    except Exception as e:
        print(f"    ❌ PDF processing failed: {e}")
        return None

def process_document_content(path: str, file_url: str):
    ext = os.path.splitext(path)[1].lower()
    fname = os.path.basename(path)

    if ext == ".pdf":
        extracted = process_pdf_with_plumber(path)
        if extracted:
            return {
                "file_name": fname,
                "file_type": "pdf",
                "content_text": extracted["content_text"],
                "content_tables_json": json.dumps(extracted["content_tables"])
            }

    return {
        "file_name": fname,
        "file_type": ext.strip("."),
        "content_text": None,
        "content_tables_json": None
    }

# -------------------------------------------------
# CLICK ENGINE (Playwright)
# -------------------------------------------------

def simulate_clicks_on_tabs(page: Page, url: str, pre_links: set, new_links: set):
    selectors = [
        "a[role='tab']", "button[role='tab']",
        "a[data-toggle]", "button[data-toggle]",
        "a[data-bs-toggle]", "button[data-bs-toggle]",
        "//li[contains(@class, 'tab')]/a",
        "//div[contains(@class, 'accordion')]",
        "//a[contains(@class, 'btn')]"
    ]

    SOCIAL_PATTERNS = [
        "facebook.com", "instagram.com", "twitter.com", "x.com",
        "linkedin.com", "youtube.com", "tiktok.com",
        "pinterest.com", "snapchat.com", "threads.net"
    ]

    total_clicks = 0

    for selector in selectors:
        try:
            elements = page.locator(selector + ":visible")
            count = elements.count()

            for i in range(count):
                el = elements.nth(i)
                text = (el.inner_text() or "").strip()
                identifier = el.get_attribute("href") or text or ""

                if not identifier:
                    continue

                # --- 🚫 Block all social media clicks (MOST IMPORTANT) ---
                lowered = identifier.lower()
                if any(s in lowered for s in SOCIAL_PATTERNS):
                    print(f"    ⛔ Skipping social element: {identifier}")
                    continue
                # ---------------------------------------------------------

                key = f"{url}::{identifier}"
                if key in GLOBAL_CLICKED_ELEMENTS:
                    continue

                is_active = el.evaluate("""
                    e => e.classList.contains("active") ||
                         e.getAttribute("aria-selected") === "true"
                """)

                if not is_active:
                    print(f"    🖱️ Clicking: {text}")
                    try:
                        el.click(timeout=3000)
                    except Exception:
                        continue

                    page.wait_for_timeout(3000)
                    total_clicks += 1

                    # Extract links after click
                    links_after = extract_links(page.content(), url)
                    diff = links_after - pre_links

                    new_links.update(diff)
                    pre_links.update(links_after)

                    GLOBAL_CLICKED_ELEMENTS.add(key)

        except Exception:
            continue

    # Detect <a download> links
    download_elems = page.locator("a[download]").all()
    for d in download_elems:
        href = d.get_attribute("href")
        if href:
            full = urljoin(url, href)
            lowered = full.lower()

            # Block social download anchors too
            if any(s in lowered for s in SOCIAL_PATTERNS):
                print(f"    ⛔ Skipping social-download link: {full}")
                continue

            new_links.add(full)

    if total_clicks:
        print(f"    ✨ Clicked {total_clicks} hidden elements")

# -------------------------------------------------
# MAIN FETCHER
# -------------------------------------------------

def fetch_content_and_links(url: str):
    scraped_files = []

    # --------------- STATIC / API ---------------
    if any(k in url.lower() for k in ["api", ".json", ".xml"]) or url.lower().endswith((".html", ".htm")):
        try:
            res = requests.get(url, headers={"User-Agent": SETTINGS.USER_AGENT},
                               timeout=SETTINGS.DEFAULT_TIMEOUT_MS / 1000)
            res.raise_for_status()

            ctype = res.headers.get("Content-Type", "").lower()

            if "json" in ctype or "xml" in ctype:
                data = parse_api_content(res.text)
                return {"url": url, "type": "API", "data": data, "links": set(), "scraped_files": []}

            # HTML
            html = res.text
            data = parse_html_content(html)
            all_links = extract_links(html, url)

            # Filter for price-related docs
            file_links = {
                link for link in all_links
                if is_downloadable_file_link(link)
            }

            # Process files
            for link in file_links:
                clean = normalize_url(link)
                if clean in PROCESSED_FILE_URLS:
                    continue

                local = download_file(link)
                if local:
                    doc = process_document_content(local, link)
                    # try: os.remove(local)
                    # except: pass

                    doc["file_url"] = link
                    scraped_files.append(doc)
                    PROCESSED_FILE_URLS.add(clean)

            # Price-only page links
            page_links = {
                l for l in all_links
                if is_price_related_url(l) or is_price_related_file(l)
            } - file_links

            return {
                "url": url,
                "type": "STATIC_HTML",
                "data": data,
                "links": page_links,
                "scraped_files": scraped_files
            }

        except Exception as e:
            return {"url": url, "error": str(e), "links": set(), "scraped_files": []}

    # --------------- DYNAMIC / JS (Playwright) ---------------
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=SETTINGS.HEADLESS_MODE)
        page = browser.new_page(user_agent=SETTINGS.USER_AGENT)

        pre = set()
        new = set()

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=SETTINGS.DEFAULT_TIMEOUT_MS)
            page.wait_for_timeout(1000)

            pre = extract_links(page.content(), url)

            simulate_clicks_on_tabs(page, url, pre.copy(), new)

            final_html = page.content()
            structured_data = parse_html_content(final_html)

            all_links = pre.union(new)

            # Price files only
            file_links = {
                l for l in all_links
                if is_downloadable_file_link(l)
            }

            page_links = {
                l for l in all_links
                if (is_price_related_url(l) or is_price_related_file(l)) and l not in file_links
            }

            # Process files
            for link in file_links:
                clean = normalize_url(link)
                if clean in PROCESSED_FILE_URLS:
                    continue

                local = download_file(link)
                if local:
                    doc = process_document_content(local, link)
                    # try: os.remove(local)
                    # except: pass

                    doc["file_url"] = link
                    scraped_files.append(doc)
                    PROCESSED_FILE_URLS.add(clean)

            browser.close()

            return {
                "url": url,
                "type": "DYNAMIC_HTML",
                "raw_html": final_html,
                "data": structured_data,
                "links": page_links,
                "scraped_files": scraped_files
            }

        except Exception as e:
            browser.close()
            return {"url": url, "error": str(e), "links": set(pre), "scraped_files": scraped_files}
