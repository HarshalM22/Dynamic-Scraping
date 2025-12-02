import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
import re

def parse_html_content(html_content: str) -> dict:
    """Extracts structured data and full raw content from HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 1. RAW Data Capture (as requested)
    full_html = str(soup)
    
    # 2. Surface-Level Text (Visible text only, ignoring scripts/styles)
    full_text = soup.body.get_text(separator=' ', strip=True) if soup.body else ""

    # 3. Metadata (Key fields for indexing)
    title = soup.title.string if soup.title else ""
    meta_description = soup.find('meta', attrs={'name': 'description'})
    description = meta_description.get('content') if meta_description else ""

    # 4. Image URLs
    image_urls = [
        img.get('src') for img in soup.find_all('img') if img.get('src')
    ]

    return {
        'title': title,
        'description': description,
        'full_text_content': full_text,
        'image_urls': image_urls,
        'raw_html_dump': full_html, # Dump the entire rendered HTML as raw data
    }

def parse_api_content(json_content: str) -> dict:
    """Parses raw JSON content from an API endpoint."""
    try:
        data = json.loads(json_content)
        return {"api_raw_data": data}
    except json.JSONDecodeError:
        return {"api_raw_data": "Invalid JSON"}

def extract_links(html_content: str, base_url: str) -> set:
    """Extracts and normalizes unique, same-domain links from HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')
    base_domain = urlparse(base_url).netloc
    
    extracted_links = set()
    
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href')
        
        # 1. Resolve to full URL
        full_url = urljoin(base_url, href)
        
        # 2. Parse the new URL
        parsed_link = urlparse(full_url)
        
        # 3. Filter criteria: Must be http/https and stay on the same domain
        if parsed_link.scheme in ['http', 'https'] and parsed_link.netloc == base_domain:
            
            # 4. Clean URL for deduplication (remove fragments/queries)
            clean_url = urlunparse(parsed_link._replace(fragment='', query=''))
            
            # 5. Filter out common non-content links (e.g., mailto, javascript, image/file extensions)
            if not re.search(r'\.(jpg|jpeg|png|gif|pdf|zip|mailto)$', clean_url, re.IGNORECASE):
                extracted_links.add(clean_url)
            
    return extracted_links