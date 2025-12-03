import requests
import time
import os
import json
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
import pdfplumber
from pdfplumber.pdf import PDFPage
from config.settings import SETTINGS
from app.scraper_module.parser import parse_html_content, parse_api_content, extract_links
# Assuming these are defined elsewhere:
# from app.scraper_module.parser import parse_html_content, parse_api_content, extract_links


# --- UTILITY CONSTANTS AND HELPERS ---

# Define the file types we want to download and process
DOWNLOADABLE_EXTENSIONS = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.mp4', '.jpg', '.png')
DOWNLOAD_DIR = 'my_downloads'

def is_downloadable_file_link(url: str) -> bool:
    """Checks if a URL points to a downloadable file based on its extension."""
    path = urlparse(url).path
    return path.lower().endswith(DOWNLOADABLE_EXTENSIONS)

def download_file(file_url: str) -> str | None:
    """Fetches a file's binary content and saves it locally."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    filename = os.path.basename(urlparse(file_url).path)
    safe_filename = filename.split('?')[0] # Remove query parameters
    file_path = os.path.join(DOWNLOAD_DIR, safe_filename)

    try:
        response = requests.get(
            file_url, 
            stream=True, 
            headers={'User-Agent': SETTINGS.USER_AGENT}, 
            timeout=SETTINGS.DEFAULT_TIMEOUT_MS / 1000 # Convert ms to seconds
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
    
    extracted_data = {
        'content_text': "",
        'content_tables': [] # List of structured tables (list of lists)
    }
    
    try:
        with pdfplumber.open(file_path) as pdf:
            print(f"    📄 Processing {len(pdf.pages)} pages...")
            
            for page in pdf.pages:
                # 1. Extract general text
                extracted_data['content_text'] += page.extract_text() + "\n\n"
                
                # 2. Extract tables
                page_tables = page.extract_tables()
                if page_tables:
                    print(f"      -> Found {len(page_tables)} table(s) on page {page.page_number}")
                    extracted_data['content_tables'].extend(page_tables)
                
        return extracted_data
        
    except Exception as e:
        print(f"    ❌ Error processing PDF {file_path}: {e}")
        return None
    finally:
        # Clean up the local file after processing
        try:
            os.remove(file_path)
            print(f"    🗑️ Cleaned up local file: {file_path}")
        except OSError as e:
            print(f"    ❌ Error deleting file {file_path}: {e}")


# --- MAIN FETCHING FUNCTION ---

def fetch_content_and_links(url: str) -> dict:
    """
    Determines the best method (static/dynamic) to scrape the link, 
    extracts content, links, and scrapes content from linked files (PDFs).
    """
    
    # ----------------------------------------------------
    # 1. STATIC/API FETCHING LOGIC (Requests)
    # ----------------------------------------------------
    if any(keyword in url.lower() for keyword in ['api', '.json', '.xml']):
        print(f"    -> Fetching {url} as STATIC/API...")
        
        try:
            response = requests.get(url, headers={'User-Agent': SETTINGS.USER_AGENT}, timeout=10)
            response.raise_for_status() 
            content_type = response.headers.get('Content-Type', '').lower()
            
            if 'application/json' in content_type or 'xml' in content_type:
                data = parse_api_content(response.text)
                return {'url': url, 'type': 'API', 'data': data, 'links': set(), 'scraped_pdfs': []}
            else:
                data = parse_html_content(response.text)
                links = extract_links(response.text, url)
                return {'url': url, 'type': 'STATIC_HTML', 'data': data, 'links': links, 'scraped_pdfs': []}

        except requests.RequestException as e:
            return {'url': url, 'error': f"Request failed: {e}", 'links': set(), 'scraped_pdfs': []}

    # ----------------------------------------------------
    # 2. DYNAMIC HTML FETCHING LOGIC (Playwright)
    # ----------------------------------------------------
    else:
        print(f"    -> Fetching {url} as DYNAMIC/HTML (using Playwright)...")
         
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=SETTINGS.HEADLESS_MODE)
            page = browser.new_page(user_agent=SETTINGS.USER_AGENT)
            
            try:
                # 1. Navigate and wait for content
                page.goto(url, wait_until="domcontentloaded", timeout=SETTINGS.DEFAULT_TIMEOUT_MS)
                
                # 2. Simulate scrolling to load lazy content (optional but robust)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1) 

                # 3. Get the final, rendered HTML content
                final_html = page.content() # <--- ALWAYS capture the HTML content
                
                # 4. Extract data and all links
                structured_data = parse_html_content(final_html) # <--- ALWAYS process the HTML data
                all_links = extract_links(final_html, url)
                
                # 5. Filter links: Separate navigation links from file links
                file_links = {link for link in all_links if is_downloadable_file_link(link)}
                page_links = all_links - file_links # These are the navigation links for subsequent crawling
                
                # 6. Process Downloadable Files (PDFs)
                scraped_pdfs = []
                if file_links:
                    print(f"    -> Found {len(file_links)} files to download and process...")
                    for file_url in file_links:
                        # Download, process, and cleanup (download_file handles cleanup now)
                        local_path = download_file(file_url)
                        
                        if local_path:
                            pdf_content = process_pdf_with_plumber(local_path)
                            
                            if pdf_content:
                                # Prepare data for dumping (ensure tables are JSON string)
                                json_tables = json.dumps(pdf_content['content_tables'])
                                
                                scraped_pdfs.append({
                                    'file_url': file_url, 
                                    'file_name': os.path.basename(urlparse(file_url).path), # Include the file name
                                    'content_text': pdf_content['content_text'], 
                                    'content_tables_json': json_tables
                                })

                browser.close()
                return {
                    'url': url, 
                    'type': 'DYNAMIC_HTML', 
                    'raw_html': final_html, # <--- Added the raw HTML content
                    'data': structured_data, 
                    'links': page_links, # Navigation links for the crawler
                    'scraped_pdfs': scraped_pdfs # Processed file data for the DB
                }
                
            except Exception as e:
                browser.close()
                return {'url': url, 'error': f"Playwright failed: {e}", 'links': set(), 'scraped_pdfs': []}



