import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from typing import Final 

load_dotenv()

class EnvKeys:
    MONGO_USER: Final[str] = "MONGO_USER"
    MONGO_PASS: Final[str] = "MONGO_PASS"
    MONGO_DB_NAME: Final[str] = "MONGO_DB_NAME"
    MONGO_COLLECTION: Final[str] = "MONGO_COLLECTION"
    HEADLESS_MODE: Final[str] = "HEADLESS_MODE"
    MAX_CRAWL_DEPTH: Final[str] = "MAX_CRAWL_DEPTH"

class Settings:
    """Configuration settings for the data scraper application."""

    
    MONGO_USER: Final[str] = os.getenv(EnvKeys.MONGO_USER, "default_user")
    MONGO_PASS_RAW: Final[str] = os.getenv(EnvKeys.MONGO_PASS, "default_pass")

    
    MONGO_HOST: Final[str] = "cluster0.l3ldkmn.mongodb.net"
    
    MONGO_PASS_ENCODED: Final[str] = quote_plus(MONGO_PASS_RAW)

    MONGO_URI: Final[str] = (
        f"mongodb+srv://{MONGO_USER}:{MONGO_PASS_ENCODED}@{MONGO_HOST}"
    )
    print(f"MongoDB URL Constructed: {MONGO_URI}")
    MONGO_DB_NAME: Final[str] = os.getenv(EnvKeys.MONGO_DB_NAME, "raw_data_db")
    MONGO_COLLECTION: Final[str] = os.getenv(EnvKeys.MONGO_COLLECTION, "link_data")

    # --- Scraper Configuration ---
    HEADLESS_MODE: Final[bool] = os.getenv(EnvKeys.HEADLESS_MODE, "True").lower() == 'true'
    DEFAULT_TIMEOUT_MS: Final[int] = 15000  # 15 seconds
    # Added MAX_CRAWL_DEPTH, defaulting to 2
    MAX_CRAWL_DEPTH: Final[int] = int(os.getenv(EnvKeys.MAX_CRAWL_DEPTH, 2)) 
    CRAWL_DELAY_SECONDS: Final[float] = 1.0 # New setting for politeness delay
    
    USER_AGENT: Final[str] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 (Custom WebCrawler)"
    )

# Instantiate the settings object for application-wide use
SETTINGS = Settings()



# mongodb+srv://harshalmankar61:Harshal%4005@cluster0.l3ldkmn.mongodb.net/?appName=Cluster0