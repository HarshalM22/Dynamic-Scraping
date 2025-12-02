from app.scraper_module.fetcher import fetch_content_and_links
from app.storage_module.mongodb_handler import DB_HANDLER
from config.settings import SETTINGS
import time
from collections import deque
from urllib.parse import urlparse
import sys
from typing import Tuple, Dict, Any

# --- Define the single input link as requested ---
INPUT_LINK = "https://www.skillsetmaster.com/"

def normalize_url(url: str) -> str:
    """Cleans a URL by removing query parameters and fragments."""
    try:
        parsed = urlparse(url)
        # Reconstructs the URL using only scheme, netloc, and path
        # rstrip('/') ensures consistent normalization (e.g., 'a.com/' becomes 'a.com')
        clean_url = parsed.scheme + "://" + parsed.netloc + parsed.path.rstrip('/')

        # print("normalized url : ", clean_url) 
        # Printing only the normalized URL when processed to reduce console noise
        return clean_url
    except Exception:
        # Return original if parsing fails (fallback for malformed URLs)
        return url 

def run_system():
    """
    Main function to run the recursive web crawl (BFS) and store data.
    Uses 'visited_urls' and 'queued_urls' to ensure efficient, non-redundant crawling.
    """
    print("Starting Recursive Web Crawler...")
    print(f"Seed URL: {INPUT_LINK}")
    print(f"Max Depth: {SETTINGS.MAX_CRAWL_DEPTH}")
    print(f"Crawl Delay: {SETTINGS.CRAWL_DELAY_SECONDS}s\n")
    
    # CRITICAL: Check DB connection status before starting the crawl
    if not DB_HANDLER.is_connected():
        print("CRITICAL: MongoDB connection failed on startup. Cannot proceed. Exiting.")
        sys.exit(1)
    
    normalized_input_link = normalize_url(INPUT_LINK)
    
    # Queue stores tuples of (normalized_url, depth)
    crawl_queue = deque([(normalized_input_link, 0)]) 
    
    # Set of URLs that have been successfully fetched (processed)
    visited_urls = set() 
    
    # Set of URLs currently in the crawl_queue (waiting to be processed)
    # This prevents the same link from being added to the queue multiple times
    queued_urls = {normalized_input_link}
    
    # Get the base domain for filtering internal links
    base_domain = urlparse(INPUT_LINK).netloc
    
    while crawl_queue:
        # Get the next URL and its depth
        current_url, current_depth = crawl_queue.popleft() 
        # Remove from queued set as we are about to process it
        queued_urls.discard(current_url)
        
        # 1. Redundancy Check: Already fetched? (This should only catch previously failed fetches if logic changes)
        if current_url in visited_urls:
            continue
            
        # 2. Depth Check: Stop if we exceed the defined limit
        if current_depth > SETTINGS.MAX_CRAWL_DEPTH:
            print(f"Skipping {current_url} due to depth limit ({current_depth}).")
            continue
            
        # Add to visited set immediately. This prevents a race condition
        # where the same URL is queued by another process/thread (if multi-threaded), 
        # but in this single-threaded model, it signals we are processing it.
        visited_urls.add(current_url)
        
        print(f"[Depth {current_depth}] Processing: {current_url}")
        
        start_time = time.time()
        
        # 3. Fetch and Scrape
        # The result includes 'data' (content) and 'links' (discovered URLs)
        crawl_result: Dict[str, Any] = fetch_content_and_links(current_url)
        
        # 4. Prepare Data for DB
        db_document = {
            'input_url': current_url,
            'crawl_depth': current_depth,
            'collection_timestamp': time.time(),
            # Status based on fetcher result
            'status': 'SUCCESS' if not crawl_result.get('error') else 'FAILED', 
            'error_message': crawl_result.get('error', None),
            'raw_data_dump': crawl_result.get('data') 
        }
        
        # 5. Store Data with Robust Error Handling
        insert_id = None
        db_status_message = db_document['status'] # Start with the crawl status

        try:
            # The fixed DB_HANDLER.insert_data now correctly uses 'is None' for checks
            insert_id = DB_HANDLER.insert_data(db_document)
            
            # Check if handler returned a non-ID value (None usually means connection failure inside insert_data)
            if insert_id is None:
                db_status_message = "DB_INSERT_FAILED: Handler returned None (Likely connection issue)."

        except Exception as e:
            # Catches OperationFailure (DB issues) or other errors re-raised by the handler
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
                    
                    is_internal = urlparse(link).netloc == base_domain
                    # Check if the link is internal AND has not been visited AND is not already waiting in the queue
                    is_new = normalized_link not in visited_urls and normalized_link not in queued_urls
                    
                    if is_internal and is_new:
                        
                        # Add the normalized link to the queue
                        crawl_queue.append((normalized_link, next_depth))
                        
                        # Add to the set of links waiting in the queue
                        queued_urls.add(normalized_link)
                        
                        newly_discovered_count += 1
                
                print(f"  -> Discovered {newly_discovered_count} new links for depth {next_depth}.")
            else:
                print(f"  -> Skipped link discovery: Next depth ({next_depth}) exceeds max limit.")
        
        # 7. Politeness Delay
        time.sleep(SETTINGS.CRAWL_DELAY_SECONDS)
            
    print("\n--- Crawl Finished ---")
    print(f"Total Unique Pages Visited: {len(visited_urls)}")
    # If the crawl queue empties correctly, queued_urls should be 0, but shows any waiting links if loop breaks unexpectedly
    print(f"Total Unique Pages Queued (Unprocessed): {len(queued_urls)}")


if __name__ == "__main__":
    run_system()