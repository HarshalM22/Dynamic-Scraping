from app.seed_resolver.serpapi_service import SerpAPIService
from app.seed_resolver.filter_service import URLFilter
from app.seed_resolver.rank_service import URLRanker
from app.seed_resolver.validator_service import URLValidator

class SeedURLResolver:

    @staticmethod
    def resolve(hospital_name: str):
        # Step 1: Fetch possible URLs
        urls = SerpAPIService.search(hospital_name)

        # Step 2: Filter bad domains
        urls = URLFilter.clean(urls)

        if not urls:
            return None

        # Step 3: Rank URLs by relevance
        ranked = URLRanker.rank(hospital_name, urls)

        # Step 4: Validate top candidates
        for url, score in ranked:
            if URLValidator.validate(url, hospital_name):
                return {
                    "hospital": hospital_name,
                    "seed_url": url,
                    "confidence": score / 100
                }

        # If no validated match, return best scored
        url, score = ranked[0]
        return {
            "hospital": hospital_name,
            "seed_url": url,
            "confidence": score / 100
        }
