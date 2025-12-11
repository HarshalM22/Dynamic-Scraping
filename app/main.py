from app.scraper_module.fetcher import fetch_content_and_links 
from app.scraper_module.filter import is_social_url
from app.storage_module.mongodb_handler import DB_HANDLER
from config.settings import SETTINGS
import time
from collections import deque
from urllib.parse import urlparse
import sys
from typing import Tuple, Dict, Any, List, Set

# You can put 5-100 links in this list.
INPUT_LINK: List[str] = [
    "https://www.namccares.com/",
    "https://www.mizellmh.com/",
    "https://crenshawcommunityhospital.com/",
    "https://uabstvincents.org/locations/uab-st-vincents-east/",
    "https://www.baptisthealthal.com/facilities/shelby-hospital",
    "https://www.uabmedicine.org/locations/uab-hospital-callahan-eye/",
    # Add more links here, up to 100 or more if needed
    # "https://example.com/hospital7",
    # "https://example.com/hospital8",
]

def normalize_url(url: str) -> str:
    """Cleans a URL by removing query parameters and fragments."""
    try:
        parsed = urlparse(url)
        # rstrip('/') ensures consistent normalization (e.g., 'a.com/' becomes 'a.com')
        clean_url = parsed.scheme + "://" + parsed.netloc + parsed.path.rstrip('/')

        # print("normalized url : ", clean_url) 
        return clean_url
    except Exception:
        return url 

def run_system():
    """
    Main function to run the recursive web crawl (BFS) and store data.
    Correctly handles and stores HTML content, structured HTML data,
    and scraped content from linked document files (PDFs/Docs).
    
    This version supports multiple seed URLs.
    """
    print("Starting Recursive Web Crawler (Multi-Seed)..")
    print(f"Total Seed URLs: {len(INPUT_LINK)}")
    print(f"Max Depth: {SETTINGS.MAX_CRAWL_DEPTH}")
    print(f"Crawl Delay: {SETTINGS.CRAWL_DELAY_SECONDS}s\n")
    
    if not DB_HANDLER.is_connected():
        print("CRITICAL: MongoDB connection failed on startup. Cannot proceed. Exiting.")
        sys.exit(1)
    
    # Queue stores tuples of (normalized_url, depth, base_domain)
    # The base_domain is crucial for checking if a discovered link is 'internal' to the current crawl.
    crawl_queue: deque[Tuple[str, int, str]] = deque()
    
    # Set of URLs that have been successfully fetched (processed)
    visited_urls: Set[str] = set() 
    
    # Set of URLs currently in the crawl_queue (waiting to be processed)
    queued_urls: Set[str] = set()

    # --- Initialization for Multiple Seeds ---
    for link in INPUT_LINK:
        normalized_link = normalize_url(link)
        base_domain = urlparse(normalized_link).netloc
        
        # Only add if not already in the queue/visited (in case of duplicate inputs)
        if normalized_link not in queued_urls and normalized_link not in visited_urls:
            crawl_queue.append((normalized_link, 0, base_domain))
            queued_urls.add(normalized_link)
    
    print(f"Initialized queue with {len(crawl_queue)} unique seed URLs.")
    # --- End Initialization ---
    
    while crawl_queue:
        # Get the next URL, its depth, AND its original base domain
        current_url, current_depth, current_base_domain = crawl_queue.popleft() 
        queued_urls.discard(current_url)
        
        # 1. Redundancy Check
        if current_url in visited_urls:
            continue
            
        # 2. Depth Check
        if current_depth > SETTINGS.MAX_CRAWL_DEPTH:
            print(f"Skipping {current_url} due to depth limit ({current_depth}).")
            continue
            
        # Add to visited set immediately.
        visited_urls.add(current_url)
        
        print(f"[Depth {current_depth}] Processing: {current_url} (Base Domain: {current_base_domain})")
        
        start_time = time.time()
        
        # 3. Fetch and Scrape
        # The result includes 'raw_html', 'data', 'links', and 'scraped_files'
        crawl_result: Dict[str, Any] = fetch_content_and_links(current_url)
        
        # 4. Prepare Data for DB 
        db_document = {
            'input_url': current_url,
            'crawl_depth': current_depth,
            'collection_timestamp': time.time(),
            # Status based on fetcher result
            'status': 'SUCCESS' if not crawl_result.get('error') else 'FAILED', 
            'error_message': crawl_result.get('error', None),
            
            # --- HTML Content and Structure ---
            'raw_html_content': crawl_result.get('raw_html', None),     # Full HTML dump
            'html_structured_data': crawl_result.get('data'),           # Parsed data from HTML ( specific fields)
            
            # --- File Content (PDFs/Docs) ---
            'scraped_files': crawl_result.get('scraped_files', []),      # List of dictionaries with file content/metadata
        }
        
        # 5. Store Data 
        insert_id = None
        db_status_message = db_document['status'] # Start with the crawl status

        try:
            # Insert the complete document containing all HTML and file data
            insert_id = DB_HANDLER.insert_data(db_document)
            
            if insert_id is None:
                db_status_message = "DB_INSERT_FAILED: Handler returned None (Likely connection issue)."

        except Exception as e:
            db_status_message = f"DB_CRITICAL_ERROR: {e.__class__.__name__}: {str(e)}"
            insert_id = None
            
        finally:
            end_time = time.time()
            
            # Print the final status of the database operation for the current URL
            print(f" -> DB ID: {insert_id}. Status: **{db_status_message}**. Time: {end_time - start_time:.2f}s")
        
        # 6. Extract and queue new links for the next depth level
        if 'links' in crawl_result and crawl_result['links']:
            newly_discovered_count = 0
            next_depth = current_depth + 1
            
            # Only queue links if the next depth is within the maximum limit
            if next_depth <= SETTINGS.MAX_CRAWL_DEPTH:
                for link in crawl_result['links']:
                    normalized_link = normalize_url(link)
                    
                    # **CRUCIAL CHANGE**: Check against the current URL's base domain
                    link_domain = urlparse(link).netloc
                    is_internal = link_domain == current_base_domain
                    
                    is_new = normalized_link not in visited_urls and normalized_link not in queued_urls
                    
                    if is_internal and is_new and not is_social_url(normalized_link):
                        
                        # Add the normalized link, the next depth, and the existing base domain
                        crawl_queue.append((normalized_link, next_depth, current_base_domain))
                        
                        # Add to the set of links waiting in the queue
                        queued_urls.add(normalized_link)
                        
                        newly_discovered_count += 1
                        
                print(f"  -> Discovered {newly_discovered_count} internal links for depth {next_depth}.")
            else:
                print(f"  -> Skipped link discovery: Next depth ({next_depth}) exceeds max limit.")
        
        # 7. Politeness Delay
        time.sleep(SETTINGS.CRAWL_DELAY_SECONDS)
            
    print("\n--- Crawl Finished ---")
    print(f"Total Unique Pages Visited: {len(visited_urls)}")
    print(f"Total Unique Pages Queued (Unprocessed): {len(queued_urls)}")


if __name__ == "__main__":
    run_system()