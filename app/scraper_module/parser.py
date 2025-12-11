import json
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
from typing import Any, Dict, Optional, List, Set
from app.scraper_module.filter import is_social_url


# --- GLOBAL EXTRACTION SCHEMA ---
# You can freely extend this; missing/malformed entries will never crash parsing.

GLOBAL_EXTRACTION_SCHEMA: Dict[str, Dict[str, Any]] = {
    "price": {
        "keywords": ["price", "cost", "for sale", "buy now"],
        # Regex to find currency amounts (e.g., $19.99, € 1.000)
        "value_pattern": r"(\$|€)\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)",
        "multi_value": True,
    },
    "specialities": {
        "keywords": ["specialities", "features", "conditions", "treatments"],
        # Very generic word/phrase pattern – you can refine later.
        # Having a pattern avoids KeyError and lets extraction work.
        "value_pattern": r"[A-Za-z][A-Za-z0-9 ,\-]{2,}",
        "multi_value": True,
    },
    "date_posted": {
        "keywords": ["posted on", "published", "date", "available since"],
        # Regex for YYYY-MM-DD or MM/DD/YYYY
        "value_pattern": r"\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}",
        "multi_value": False,
    },
}


# --- INTERNAL UTILITIES ---


def _safe_get_visible_text(soup: BeautifulSoup) -> str:
    """
    Extract visible text only (ignores scripts/styles etc.).
    Never raises; always returns a string.
    """
    if not soup or not soup.body:
        return ""

    # Remove non-content tags
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.body.get_text(separator=" ", strip=True)
    return text or ""


# --- PUBLIC PARSERS ---


def parse_html_content(html_content: str) -> Dict[str, Any]:
    """
    Extracts structured data and general text from HTML.
    Robust against malformed HTML or schema issues.
    """
    try:
        soup = BeautifulSoup(html_content or "", "html.parser")
    except Exception:
        # If BeautifulSoup itself fails (very rare), fall back to minimal structure
        return {
            "title": "",
            "description": "",
            "full_text_content": html_content or "",
            "contextual_data": {},
        }

    # 1. Visible text
    full_text = _safe_get_visible_text(soup)

    # 2. Basic metadata
    try:
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
    except Exception:
        title = ""

    try:
        meta_description = soup.find("meta", attrs={"name": "description"})
        description = (meta_description.get("content") or "").strip() if meta_description else ""
    except Exception:
        description = ""

    contextual_data: Dict[str, Any] = {}

    # 3. Contextual extractions driven by GLOBAL_EXTRACTION_SCHEMA
    for var_name, config in GLOBAL_EXTRACTION_SCHEMA.items():
        if not isinstance(config, dict):
            # Skip if schema entry is malformed
            continue

        try:
            result = contextual_extract(full_text, config)
        except Exception:
            # Never let one bad schema entry break everything
            result = None

        if result is not None:
            contextual_data[var_name] = result

    return {
        "title": title,
        "description": description,
        "full_text_content": full_text,
        "contextual_data": contextual_data,
    }


def parse_api_content(json_content: str) -> Dict[str, Any]:
    """
    Parses raw JSON content from an API endpoint.
    Robust to invalid JSON.
    """
    try:
        data = json.loads(json_content)
        return {"api_raw_data": data}
    except Exception:
        return {"api_raw_data": "Invalid JSON"}


def extract_links(html_content: str, base_url: str) -> Set[str]:
    """
    Extracts and normalizes unique, same-domain links from HTML.
    Never raises; returns an empty set on any parsing issue.
    """
    try:
        soup = BeautifulSoup(html_content or "", "html.parser")
    except Exception:
        return set()

    try:
        base_parsed = urlparse(base_url or "")
        base_domain = base_parsed.netloc
    except Exception:
        base_domain = ""

    extracted_links: Set[str] = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href")

        if not href:
            continue

        # Resolve relative URL
        try:
            full_url = urljoin(base_url, href)
            parsed_link = urlparse(full_url)
        except Exception:
            continue

        # Only http/https and same domain (internal links)
        if parsed_link.scheme not in ["http", "https"]:
            continue
        if base_domain and parsed_link.netloc != base_domain:
            continue

        # Remove fragment and query for normalization
        try:
            clean_url = urlunparse(parsed_link._replace(fragment="", query=""))
        except Exception:
            continue

        # Filter out mailto:, javascript:, etc.
        if re.match(r"^(mailto|javascript):", clean_url, re.IGNORECASE):
            continue

        if not is_social_url(clean_url):
            extracted_links.add(clean_url)


    return extracted_links


# --- CONTEXTUAL EXTRACTION ENGINE ---


def contextual_extract(
    text: str,
    variable_config: Dict[str, Any],
    window_chars: int = 50,
) -> Optional[Any]:
    """
    Generic contextual extractor:
    - text: full page text
    - variable_config:
        - keywords: list[str] (optional, can be empty)
        - value_pattern: regex pattern string (required for actual extraction)
        - multi_value: bool (optional, defaults True)
    - window_chars: size of left/right context window used for scoring

    Robust:
    - If pattern missing/invalid -> returns None
    - If no matches -> returns None
    - Never throws
    """
    if not text or not isinstance(text, str):
        return None

    keywords: List[str] = variable_config.get("keywords") or []
    pattern: Optional[str] = variable_config.get("value_pattern")
    is_multi: bool = bool(variable_config.get("multi_value", True))

    if not pattern or not isinstance(pattern, str):
        # No usable pattern => skip this variable
        return None

    try:
        regex = re.compile(pattern, re.IGNORECASE)
    except re.error:
        # Invalid regex pattern; don't crash
        return None

    scored_candidates: List[Dict[str, Any]] = []

    # Use finditer so we get correct spans even with capturing groups
    for match in regex.finditer(text):
        try:
            value = match.group(0).strip()
            if not value:
                continue

            start_index, end_index = match.span()
            context_start = max(0, start_index - window_chars)
            context_end = min(len(text), end_index + window_chars)
            context_window = text[context_start:context_end].lower()

            # Simple score: +1 for every keyword in context
            score = 0
            if keywords:
                for kw in keywords:
                    if kw.lower() in context_window:
                        score += 1
            # If no keywords defined, treat all matches as score 1
            else:
                score = 1

            if score > 0:
                scored_candidates.append({"value": value, "score": score})
        except Exception:
            # Ignore any weird match error and continue
            continue

    if not scored_candidates:
        return None

    if not is_multi:
        # Single best candidate by score
        best = max(scored_candidates, key=lambda c: c.get("score", 0))
        return best["value"]

    # Multi-value: return unique values preserving order
    seen = set()
    results: List[str] = []
    for c in scored_candidates:
        v = c["value"]
        if v not in seen:
            seen.add(v)
            results.append(v)

    return results or None
