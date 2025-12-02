from app.scraper_module.fetcher import fetch_content_and_links
from app.storage_module.mongodb_handler import DB_HANDLER
from config.settings import SETTINGS
import time
from collections import deque
from urllib.parse import urlparse
import sys
from typing import Tuple

# --- Define the single input link as requested ---
INPUT_LINK = "https://playwright.dev/"

def normalize_url(url: str) -> str:
    """Cleans a URL by removing query parameters and fragments."""
    try:
        parsed = urlparse(url)
        # Reconstructs the URL using only scheme, netloc, and path
        clean_url = parsed.scheme + "://" + parsed.netloc + parsed.path.rstrip('/')

        print("normalized url : ", clean_url) 

        return clean_url
    except Exception:
        return url # Return original if parsing fails

def run_system():
    """Main function to run the recursive web crawl and store data."""
    print("Starting Recursive Web Crawler...")
    print(f"Seed URL: {INPUT_LINK}")
    print(f"Max Depth: {SETTINGS.MAX_CRAWL_DEPTH}")
    print(f"Crawl Delay: {SETTINGS.CRAWL_DELAY_SECONDS}s\n")
    
    # CRITICAL: Check DB connection status before starting the crawl
    if not DB_HANDLER.is_connected():
        print("CRITICAL: MongoDB connection failed on startup. Cannot proceed. Exiting.")
        sys.exit(1)
    
    # Normalize the seed link immediately
    normalized_input_link = normalize_url(INPUT_LINK)
    
    # Queue stores tuples of (normalized_url, depth)
    crawl_queue = deque([(normalized_input_link, 0)]) 
    # Set stores normalized URLs for fast O(1) checking
    visited_urls = set() 
    
    # Get the base domain for filtering internal links
    base_domain = urlparse(INPUT_LINK).netloc
    
    while crawl_queue:
        # Get the next URL and its depth
        current_url, current_depth = crawl_queue.popleft() 
        
        # 1. Politeness Check: Don't visit the same URL twice (using the normalized URL)
        if current_url in visited_urls:
            continue
            
        # 2. Depth Check: Stop if we exceed the defined limit
        if current_depth > SETTINGS.MAX_CRAWL_DEPTH:
            print(f"Skipping {current_url} due to depth limit ({current_depth}).")
            continue
            
        # Add to visited set now to prevent other threads/processes from queuing it
        visited_urls.add(current_url) 
        
        print(f"[Depth {current_depth}] Processing: {current_url}")
        
        start_time = time.time()
        
        # 3. Fetch and Scrape
        crawl_result = fetch_content_and_links(current_url)
        
        # 4. Prepare Data for DB
        # Note: The current_url in the document is the normalized version
        db_document = {
            'input_url': current_url,
            'crawl_depth': current_depth,
            'collection_timestamp': time.time(),
            # Check for fetcher error status
            'status': 'SUCCESS' if not crawl_result.get('error') else 'FAILED', 
            'error_message': crawl_result.get('error', None),
            'raw_data_dump': crawl_result.get('data') 
        }
        
        # 5. Store Data with Robust Error Handling
        insert_id = None
        db_status_message = db_document['status'] # Start with the crawl status

        try:
            insert_id = DB_HANDLER.insert_data(db_document)
            print("DB handler", DB_HANDLER.collection)
            
            # Check if handler returned a non-ID value (like None)
            if not insert_id or insert_id in ("DB_OP_ERROR", "UNKNOWN_DB_ERROR"):
                db_status_message = "DB_INSERT_FAILED: Handler returned None/Error"

        except Exception as e:
            # Catches OperationFailure or other handler-raised errors
            db_status_message = f"DB_CRITICAL_ERROR: {e.__class__.__name__}: {str(e)}"
            
        end_time = time.time()
        print(f" -> DB ID: {insert_id}. Status: **{db_status_message}**. Time: {end_time - start_time:.2f}s")
        
        # 6. Extract and queue new links
        if 'links' in crawl_result and crawl_result['links']:
            newly_discovered_count = 0
            next_depth = current_depth + 1
            
            for link in crawl_result['links']:
                normalized_link = normalize_url(link)
                
                # Filter out external links and check if normalized link is already visited
                if urlparse(link).netloc == base_domain and normalized_link not in visited_urls:
                    
                    # Add the link to the visited set immediately 
                    visited_urls.add(normalized_link)
                    
                    # Add the normalized link to the queue
                    crawl_queue.append((normalized_link, next_depth))
                    newly_discovered_count += 1
            
            print(f"  -> Discovered {newly_discovered_count} new links for depth {next_depth}.")
        
        # 7. Politeness Delay
        time.sleep(SETTINGS.CRAWL_DELAY_SECONDS)
            
    print("\n--- Crawl Finished ---")
    print(f"Total Unique Pages Visited: {len(visited_urls)}")


if __name__ == "__main__":
    run_system()