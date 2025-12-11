import requests
from bs4 import BeautifulSoup
from config.settings import Settings

class URLValidator:

    @staticmethod
    def validate(url: str, hospital_name: str):
        try:
            res = requests.get(url, headers={"User-Agent": Settings.USER_AGENT}, timeout=8)

            if res.status_code != 200:
                return False

            soup = BeautifulSoup(res.text, "html.parser")

            title = soup.title.string.lower() if soup.title else ""
            name_parts = hospital_name.lower().split()

            # Title match
            if any(part in title for part in name_parts):
                return True

            # Keyword match inside body
            body = soup.get_text().lower()
            keywords = ["doctor", "department", "treatment", "care", "hospital"]

            matches = sum(1 for k in keywords if k in body)
            return matches >= 2

        except Exception:
            return False
