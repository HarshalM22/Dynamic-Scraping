# filters.py

SOCIAL_DOMAINS = [
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "youtube.com",
    "tiktok.com",
    "pinterest.com",
    "snapchat.com",
    "threads.net"
]

def is_social_url(url: str) -> bool:
    if not url:
        return False
    lowered = url.lower()
    return any(domain in lowered for domain in SOCIAL_DOMAINS)
