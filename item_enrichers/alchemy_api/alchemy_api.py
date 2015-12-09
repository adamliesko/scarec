#!/usr/bin/env python
from item_enrichers.alchemyapi import Auth, AlchemyAPI


class AlchemyApiEnricher:
    API_KEY = "a95f4a1a6bac4f2ebb9360202e887e5d6eb8c328"

    @classmethod
    def enrich_article(cls, url, article_dict):
        alchemy_dict = {"alchemy": cls.download_article(url)}
        merged_dict = article_dict.copy()
        merged_dict.update(alchemy_dict)
        return merged_dict

    @classmethod
    def download_article(cls, url, image_url=None):
        auth = Auth(cls.API_KEY)
        api = AlchemyAPI(auth)
        response_dict = api.interface('combined', 'url', url, sentiment=1)
        title = api.interface('title', 'url', url)
        text = api.interface('text', 'url', url)
        if image_url:
            image_tags = api.interface('image_tagging', 'url', image_url)['imageKeywords']
            response_dict['image_keywords'] = image_tags
        response_dict['text'] = text['text']
        response_dict['title'] = title['title']
        return response_dict
