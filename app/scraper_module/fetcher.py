import requests
from playwright.sync_api import sync_playwright
from config.settings import SETTINGS
from app.scraper_module.parser import parse_html_content, parse_api_content, extract_links
import time

def fetch_content_and_links(url: str) -> dict:
    """
    Determines the best method (static/dynamic) to scrape the link, 
    extracts content, and extracts all new links from the page.
    """
    
    # We use a heuristic: if the URL looks like an API or static file, use requests.
    # Otherwise, default to Playwright for the safest result.
    if any(keyword in url.lower() for keyword in ['api', '.json', '.xml']):
        print(f"    -> Fetching {url} as STATIC/API...")
        
        try:
            response = requests.get(url, headers={'User-Agent': SETTINGS.USER_AGENT}, timeout=10)
            response.raise_for_status() 
            content_type = response.headers.get('Content-Type', '').lower()
            
            if 'application/json' in content_type or 'xml' in content_type:
                data = parse_api_content(response.text)
                return {'url': url, 'type': 'API', 'data': data, 'links': set()}
            else:
                data = parse_html_content(response.text)
                # For non-dynamic static pages, extract links from the static HTML
                links = extract_links(response.text, url)
                return {'url': url, 'type': 'STATIC_HTML', 'data': data, 'links': links}

        except requests.RequestException as e:
            return {'url': url, 'error': f"Request failed: {e}", 'links': set()}

    else:
        print(f"    -> Fetching {url} as DYNAMIC/HTML (using Playwright)...")
        
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
                final_html = page.content()
                
                # 4. Extract data and links
                structured_data = parse_html_content(final_html)
                links = extract_links(final_html, url)

                browser.close()
                return {'url': url, 'type': 'DYNAMIC_HTML', 'data': structured_data, 'links': links}
                
            except Exception as e:
                browser.close()
                return {'url': url, 'error': f"Playwright failed: {e}", 'links': set()}