import requests


class DiffbotEnricher():
    API_KEY = "e5d5d00e7be4d284b1d5bd3a33e92e02"
    RESPONSE_FIELDS = []

    @classmethod
    def enrich_article(cls, url, article_dict):
        diffbot_dict = {"diffbot": cls.download_article(url)["objects"]}
        merged_dict = article_dict.copy()
        merged_dict.update(diffbot_dict)
        return merged_dict

    @classmethod
    def download_article(cls, url):
        payload = {"url": url, "token": cls.API_KEY, "fields": cls.RESPONSE_FIELDS}

        response = requests.get("http://api.diffbot.com/v3/article", params=payload)
        if response:
            return response.json()
        else:
            return {}
