from urllib.parse import urlparse

class URLRanker:

    @staticmethod
    def rank(hospital_name: str, urls: list):
        results = []
        name_parts = hospital_name.lower().split()

        for url in urls:
            score = 0
            domain = urlparse(url).netloc.lower()

            # Domain relevance
            if any(part in domain for part in name_parts):
                score += 40

            # Keyword relevance
            keywords = ["hospital", "health", "medical", "care"]
            if any(k in url.lower() for k in keywords):
                score += 20

            # HTTPS quality
            if url.startswith("https"):
                score += 10

            results.append((url, score))

        return sorted(results, key=lambda x: x[1], reverse=True)
