#from item_enrichers.alchemy_api.alchemy_api import AlchemyApiEnricher
from item_enrichers.diffbot.diffbot import DiffbotEnricher


class Enricher:
    @staticmethod
    def enrich_article(url, article_dict={}):
        #article_dict = AlchemyApiEnricher.enrich_article(url, article_dict)
        # article_dict = DiffbotEnricher.enrich_article(url, article_dict)
        return article_dict
