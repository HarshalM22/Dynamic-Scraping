BLACKLIST = [
    "practo", "justdial", "lybrate", "zocdoc", "facebook",
    "linkedin", "youtube", "twitter", "1mg", "pharmeasy",
    "sulekha", "docprime"
]

class URLFilter:

    @staticmethod
    def clean(urls):
        cleaned = []
        for url in urls:
            if not url:
                continue

            url_low = url.lower()
            if any(bad in url_low for bad in BLACKLIST):
                continue

            cleaned.append(url)

        return cleaned
