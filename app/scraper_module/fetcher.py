import requests
import time
import os
import json
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, Page
import pdfplumber
from config.settings import SETTINGS
from app.scraper_module.parser import parse_html_content, parse_api_content, extract_links


# --- UTILITY CONSTANTS AND GLOBAL TRACKER ---

DOWNLOADABLE_EXTENSIONS = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.mp4', '.jpg', '.png')
DOWNLOAD_DIR = 'my_downloads'

# GLOBAL SET: Tracks file URLs that have been successfully downloaded and processed 
# in this session to prevent redundant downloads.
PROCESSED_FILE_URLS = set()

GLOBAL_CLICKED_ELEMENTS = set()

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

# Assuming parse_html_content and extract_links are available globally

def simulate_clicks_on_tabs(page: Page, url: str, newly_discovered_links: set) -> None:
    """
    Simulates clicks, extracts *new* links after each click, and adds them 
    to the newly_discovered_links set passed by reference.
    """
    tab_selectors = [
        "//a[contains(@class, 'tab-link') or contains(@class, 'nav-link') or contains(@class, 'sidebar-link') or contains(@class, 'btn')]",
        "//div[contains(@class, 'tab') or contains(@class, 'accordion') or contains(@class, 'menu-item') or contains(@class, 'collapsible')]",
        "//li[contains(@class, 'tab')]/a",
        "button[role='tab']",
        "a[role='tab']",
        "[role='button']",
        "button[data-toggle]",
        "a[data-toggle]",
        "button[data-bs-toggle]",
        "a[data-bs-toggle]",
    ]
    
    total_clicks = 0
    # LOCAL SET: Tracks clicked elements on *this page visit* to avoid same-page repetition
    clicked_elements_key = set()
    # Store links *before* the click to detect which ones are new
    links_before_click = extract_links(page.content(), url) 

    for selector in tab_selectors:
        try:
            elements = page.locator(selector + ":visible")
            count = elements.count()
            
            for i in range(count):
                element = elements.nth(i)
                element_text = element.inner_text().strip()
                element_key = (selector, element_text)
                
                if not element_text or element_key in clicked_elements_key:
                    continue
                    
                # Check if the element is currently active (skip active elements)
                is_active = element.evaluate("""
                    e => e.classList.contains("active") || 
                         e.classList.contains("selected") || 
                         e.getAttribute("aria-current") === "page" || 
                         e.getAttribute("aria-selected") === "true"
                """)
                
                if not is_active:
                    print(f"    🖱️ Simulating click on potential hidden content trigger: {element_text}")
                    element.click(timeout=3000) 
                    
                    # Wait for the DOM to settle
                    page.wait_for_timeout(2000) # Wait 2 seconds

                    # --- INCREMENTAL LINK EXTRACTION ---
                    links_after_click = extract_links(page.content(), url)
                    new_links = links_after_click - links_before_click
                    
                    if new_links:
                        print(f"    🔗 Discovered {len(new_links)} new links after clicking '{element_text}'.")
                        newly_discovered_links.update(new_links)
                        # Update the baseline links for the next click
                        links_before_click.update(new_links)

                    # --- END INCREMENTAL LINK EXTRACTION ---
                    
                    total_clicks += 1
                    clicked_elements_key.add(element_key)
                
        except Exception:
            continue
            
    if total_clicks > 0:
        print(f"    ✨ Finished aggressive clicking. Total new tabs clicked: {total_clicks}")
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

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
                # --- STATIC HTML FETCHING & FILE PROCESSING LOGIC IMPROVED ---
                
                # 1. Parse content and extract all links
                data = parse_html_content(response.text)
                all_links = extract_links(response.text, url)
                
                # 2. Separate file links from standard page links
                file_links = {link for link in all_links if is_downloadable_file_link(link)}
                page_links = all_links - file_links
                
                scraped_files = []
                
                # 3. Download and process files found on the static page
                if file_links:
                    print(f"    -> Found {len(file_links)} files on static page to download and process...")
                    for file_url in file_links:
                        
                        # Use global tracker to prevent redundant downloads (if the file was crawled dynamically elsewhere)
                        if file_url in PROCESSED_FILE_URLS:
                            print(f"    ⏭️ Skipping already processed file: {file_url}")
                            continue
                        
                        local_path = download_file(file_url) # Downloads the file
                        
                        if local_path:
                            # Processes the document (e.g., extracts text from PDF)
                            doc_data = process_document_content(local_path, file_url)
                            
                            if doc_data:
                                doc_data['file_url'] = file_url
                                scraped_files.append(doc_data)
                                PROCESSED_FILE_URLS.add(file_url) # Mark as processed globally
                
                return {
                    'url': url, 
                    'type': 'STATIC_HTML', 
                    'data': data, 
                    'links': page_links, 
                    'scraped_files': scraped_files
                }

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
                
                # Set up the container for links discovered during clicks
                newly_discovered_links = set()
                
                # 2. *** Simulate ALL non-active clicks & Discover Links Incrementally ***
                simulate_clicks_on_tabs(page, url, newly_discovered_links)
                
                # 3. Process incrementally discovered files immediately (Priority Processing)
                # These links were found and added to the set during the clicks above.
                scraped_files = []
                
                if newly_discovered_links:
                    print(f"    -> PRIORITY PROCESSING {len(newly_discovered_links)} incrementally discovered links...")
                    
                    # Separate files from pages in the incrementally discovered links
                    new_file_links = {link for link in newly_discovered_links if is_downloadable_file_link(link)}
                    # Keep only non-file links for final consolidation
                    incremental_page_links = newly_discovered_links - new_file_links
                    
                    for file_url in new_file_links:
                        if file_url in PROCESSED_FILE_URLS:
                            print(f"    ⏭️ Skipping already processed file: {file_url}")
                            continue
                        
                        local_path = download_file(file_url)
                        if local_path:
                            doc_data = process_document_content(local_path, file_url)
                            if doc_data:
                                doc_data['file_url'] = file_url
                                scraped_files.append(doc_data)
                                PROCESSED_FILE_URLS.add(file_url) # Mark as processed globally

                # 4. Final HTML Capture and Full Link Extraction (Catch any non-link content/links missed by incremental logic)
                final_html = page.content()
                structured_data = parse_html_content(final_html)
                
                # Get ALL links again from the final HTML
                all_final_links = extract_links(final_html, url)
                
                # 5. Consolidate Links
                # Start with the incremental page links found earlier
                page_links = incremental_page_links 
                # Add any remaining page links found in the final full HTML, excluding files already processed
                for link in all_final_links:
                    if not is_downloadable_file_link(link) and link not in page_links:
                        page_links.add(link)


                browser.close()
                return {
                    'url': url, 
                    'type': 'DYNAMIC_HTML', 
                    'raw_html': final_html,
                    'data': structured_data, 
                    # Use the consolidated page links and the files processed during the loop
                    'links': page_links,
                    'scraped_files': scraped_files 
                }
                
            except Exception as e:
                browser.close()
                # Use incremental_page_links if they were defined before the exception
                links_to_return = incremental_page_links if 'incremental_page_links' in locals() else set()
                return {'url': url, 'error': f"Playwright failed: {e}", 'links': links_to_return, 'scraped_files': scraped_files if 'scraped_files' in locals() else []}