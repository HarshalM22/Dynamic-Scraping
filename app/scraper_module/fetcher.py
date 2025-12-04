import requests
import time
import os
import json
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, Page
import pdfplumber
from config.settings import SETTINGS
# Assuming these are defined elsewhere:
from app.scraper_module.parser import parse_html_content, parse_api_content, extract_links


# --- UTILITY CONSTANTS AND GLOBAL TRACKER ---

DOWNLOADABLE_EXTENSIONS = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.mp4', '.jpg', '.png')
DOWNLOAD_DIR = 'my_downloads'

# GLOBAL SET: Tracks file URLs that have been successfully downloaded and processed 
# in this session to prevent redundant downloads.
PROCESSED_FILE_URLS = set()

def is_downloadable_file_link(url: str) -> bool:
    """Checks if a URL points to a downloadable file based on its extension."""
    path = urlparse(url).path
    return path.lower().endswith(DOWNLOADABLE_EXTENSIONS)

def download_file(file_url: str) -> str | None:
    """Fetches a file's binary content and saves it locally."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    # Use a clean, consistent filename for storage
    filename = os.path.basename(urlparse(file_url).path)
    safe_filename = filename.split('?')[0]
    file_path = os.path.join(DOWNLOAD_DIR, safe_filename)

    try:
        response = requests.get(
            file_url, 
            stream=True, 
            headers={'User-Agent': SETTINGS.USER_AGENT}, 
            timeout=SETTINGS.DEFAULT_TIMEOUT_MS / 1000
        )
        response.raise_for_status()

        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"    ✅ Downloaded file: {safe_filename}")
        return file_path
        
    except requests.exceptions.RequestException as e:
        print(f"    ❌ Error downloading {file_url}: {e}")
        return None

def process_pdf_with_plumber(file_path: str) -> dict | None:
    """Uses pdfplumber to extract text and structured tables from a PDF."""
    extracted_data = {'content_text': "", 'content_tables': []}
    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                extracted_data['content_text'] += page.extract_text() + "\n\n"
                page_tables = page.extract_tables()
                if page_tables:
                    extracted_data['content_tables'].extend(page_tables)
        print(f"    📄 Successfully extracted text from PDF: {file_path}")
        return extracted_data
    except Exception as e:
        print(f"    ❌ Error processing PDF {file_path}: {e}")
        return None
    finally:
        try:
            os.remove(file_path)
            # print(f"    🗑️ Cleaned up local file: {file_path}")
        except OSError as e:
            print(f"    ❌ Error deleting file {file_path}: {e}")

def process_document_content(file_path: str, file_url: str) -> dict | None:
    """
    Generalized document processor. Extracts text/tables from PDFs.
    Returns metadata for other file types.
    """
    file_extension = os.path.splitext(file_path)[1].lower()
    file_name = os.path.basename(urlparse(file_url).path)

    if file_extension == '.pdf':
        extracted_data = process_pdf_with_plumber(file_path)
        if extracted_data:
            return {
                'file_name': file_name,
                'file_type': 'pdf',
                'content_text': extracted_data['content_text'],
                'content_tables_json': json.dumps(extracted_data['content_tables'])
            }
    
    # For all other document types, return metadata and a placeholder for content
    print(f"    💾 Skipping text extraction for file type: {file_extension}. Returning metadata only.")
    return {
        'file_name': file_name,
        'file_type': file_extension.strip('.'),
        'content_text': None,
        'content_tables_json': None,
    }
    
    return None

def simulate_clicks_on_tabs(page: Page, url: str) -> None:
    """
    Looks for common tab/accordion elements globally and simulates clicks on ALL
    non-active elements to reveal hidden content, then waits for load.
    
    This function uses generic selectors to maximize compatibility across different sites.
    """
    # XPaths and selectors targeting visible, clickable navigation elements (GLOBALIZED)
    tab_selectors = [
        # Generic tab/link selectors
        "//a[contains(@class, 'tab-link') or contains(@class, 'nav-link') or contains(@class, 'sidebar-link') or contains(@class, 'btn')]",
        "//div[contains(@class, 'tab') or contains(@class, 'accordion') or contains(@class, 'menu-item') or contains(@class, 'collapsible')]",
        "//li[contains(@class, 'tab')]/a",
        "button[role='tab']",
        "button[data-toggle]",
    ]
    
    total_clicks = 0
    
    # Use a set to track elements by their unique text content to avoid processing the same element multiple times
    clicked_texts = set() 

    for selector in tab_selectors:
        try:
            # Find all elements matching the selector that are visible
            elements = page.locator(selector + ":visible")
            count = elements.count()
            
            for i in range(count):
                element = elements.nth(i)
                element_text = element.inner_text().strip()
                
                # Skip if already processed or if the element is empty
                if not element_text or element_text in clicked_texts:
                    continue
                    
                # Heuristic Check: Skip if the element appears to be the currently active tab
                # We check for common active classes, aria attributes, or inline background styling
                is_active = element.evaluate('e => e.classList.contains("active") || e.getAttribute("aria-current") === "page" || e.style.backgroundColor !== ""')
                
                if not is_active:
                    print(f"    🖱️ Simulating click on potential hidden content trigger: {element_text}")
                    element.click(timeout=3000) 
                    time.sleep(1.5) # Wait for the new content to load
                    total_clicks += 1
                    clicked_texts.add(element_text)
            
        except Exception:
            # Silently fail and continue to the next selector
            continue
            
    if total_clicks > 0:
        print(f"    ✨ Finished aggressive clicking. Total new tabs clicked: {total_clicks}")
        # One final scroll/wait after all clicks to ensure dynamic links are rendered
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1)

# --- MAIN FETCHING FUNCTION ---

def fetch_content_and_links(url: str) -> dict:
    """
    Fetches content, simulates clicks, extracts all links, 
    processes linked documents, and SKIPS PREVIOUSLY PROCESSED FILES.
    """
    
    # ... (Static/API Logic remains unchanged)
    if any(keyword in url.lower() for keyword in ['api', '.json', '.xml']):
        print(f"    -> Fetching {url} as STATIC/API (Skipping file processing)...")
        try:
            response = requests.get(url, headers={'User-Agent': SETTINGS.USER_AGENT}, timeout=10)
            response.raise_for_status() 
            content_type = response.headers.get('Content-Type', '').lower()
            
            if 'application/json' in content_type or 'xml' in content_type:
                data = parse_api_content(response.text)
                return {'url': url, 'type': 'API', 'data': data, 'links': set(), 'scraped_files': []}
            else:
                data = parse_html_content(response.text)
                links = extract_links(response.text, url)
                return {'url': url, 'type': 'STATIC_HTML', 'data': data, 'links': links, 'scraped_files': []}

        except requests.RequestException as e:
            return {'url': url, 'error': f"Request failed: {e}", 'links': set(), 'scraped_files': []}


    # ----------------------------------------------------
    # 2. DYNAMIC HTML FETCHING LOGIC (Playwright) 
    # ----------------------------------------------------
    else:
        print(f"    -> Fetching {url} as DYNAMIC/HTML (using Playwright)...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=SETTINGS.HEADLESS_MODE)
            page = browser.new_page(user_agent=SETTINGS.USER_AGENT)
            
            try:
                # 1. Navigate and initial scroll/wait
                page.goto(url, wait_until="domcontentloaded", timeout=SETTINGS.DEFAULT_TIMEOUT_MS)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1) 
                
                # 2. *** Simulate ALL non-active clicks ***
                simulate_clicks_on_tabs(page, url)
                
                # 3. Get the final, fully rendered HTML content (which now includes all revealed links)
                final_html = page.content()
                
                # 4-8. Link extraction, file processing, and cleanup
                # parse_html_content and extract_links must be imported/defined.
                structured_data = parse_html_content(final_html)
                all_links = extract_links(final_html, url)
                
                file_links = {link for link in all_links if is_downloadable_file_link(link)}
                page_links = all_links - file_links
                
                scraped_files = []
                if file_links:
                    print(f"    -> Found {len(file_links)} files to download and process...")
                    for file_url in file_links:
                        
                        if file_url in PROCESSED_FILE_URLS:
                            print(f"    ⏭️ Skipping already processed file: {file_url}")
                            continue
                        
                        local_path = download_file(file_url)
                        
                        if local_path:
                            doc_data = process_document_content(local_path, file_url)
                            
                            if doc_data:
                                doc_data['file_url'] = file_url
                                scraped_files.append(doc_data)
                                PROCESSED_FILE_URLS.add(file_url) # Mark as processed

                browser.close()
                return {
                    'url': url, 
                    'type': 'DYNAMIC_HTML', 
                    'raw_html': final_html,
                    'data': structured_data, 
                    'links': page_links,
                    'scraped_files': scraped_files
                }
                
            except Exception as e:
                browser.close()
                return {'url': url, 'error': f"Playwright failed: {e}", 'links': set(), 'scraped_files': []}