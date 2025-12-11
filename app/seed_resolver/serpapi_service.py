import requests
from config.settings import Settings

class SerpAPIService:

    @staticmethod
    def search(hospital_name: str):
        url = "https://serpapi.com/search.json"
        params = {
            "engine": "google",
            "q": hospital_name,
            "api_key": Settings.SERPAPI_KEY,
            "hl": "en",
            "gl": "in"
        }

        response = requests.get(url, params=params, timeout=Settings.REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()

        urls = set()

        # Knowledge graph (highest accuracy)
        kg = data.get("knowledge_graph", {})
        if "website" in kg:
            urls.add(kg["website"])

        # Organic results
        for item in data.get("organic_results", []):
            if "link" in item:
                urls.add(item["link"])

        # Local result websites
        for item in data.get("local_results", []):
            if "website" in item:
                urls.add(item["website"])

        return list(urls)
