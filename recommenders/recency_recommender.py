from elasticsearcher import es
from rediser import redis
from models.item import Item


class RecencyRecommender:
    @classmethod
    def most_recent_n(cls, origin_timestamp, count=30, scale="24h", offset="2h", decay=0.5):
        body = {
            "query": {
                "function_score": {
                    "functions": [
                        {
                            "gauss": {
                                "created_at": {
                                    "origin": origin_timestamp,
                                    "offset": offset,
                                    "scale": scale,
                                    "decay": decay
                                }
                            },
                            "weight": 2
                        },
                        {
                            "gauss": {
                                "updated_at": {
                                    "origin": origin_timestamp,
                                    "offset": offset,
                                    "scale": scale,
                                    "decay": decay
                                }
                            },
                            "weight": 1
                        },
                    ],
                    "score_mode": "multiply",
                    "boost_mode": "multiply",
                },
            },
            "size": count
        }
        res = es.search(index=Item.ES_ITEM_INDEX, body=body)
        return res['hits']['hits']

    @classmethod
    def most_recent_per_attribute_n(cls, origin_timestamp, attribute, attribute_value, count=30, scale="24h",
                                    offset="2h", decay=0.5):
        body = {
            "query": {
                "function_score": {
                    "query": {
                        "filtered": {
                            "query": {
                                "match": {attribute: attribute_value}}}},
                    "functions": [
                        {
                            "gauss": {
                                "created_at": {
                                    "origin": origin_timestamp,
                                    "offset": offset,
                                    "scale": scale,
                                    "decay": decay
                                }
                            },
                            "weight": 2
                        },
                        {
                            "gauss": {
                                "updated_at": {
                                    "origin": origin_timestamp,
                                    "offset": offset,
                                    "scale": scale,
                                    "decay": decay
                                }
                            },
                            "weight": 1
                        },
                    ],
                    "score_mode": "multiply",
                    "boost_mode": "multiply",
                },
            },
            "size": count
        }
        res = es.search(index=Item.ES_ITEM_INDEX, body=body)
        return res['hits']['hits']

    @classmethod
    def update_recent_articles(cls, origin_timestamp):
        cls.__update_recent_articles_global(origin_timestamp)
        cls.__update_recent_articles_domains(origin_timestamp)
        cls.__update_recent_articles_publishers(origin_timestamp)

    @classmethod
    def __update_recent_articles_global(cls, origin_timestamp):
        key = 'most_recent_articles'
        articles = cls.most_recent_n(origin_timestamp)
        r = redis.pipeline()
        r.delete(key)
        for article in articles:
            r.zadd(key, str(article['_id']), article['_score'])
        r.execute()

    @classmethod
    def __update_recent_articles_domains(cls, origin_timestamp):
        domains = redis.hgetall('domains')
        for domain in domains:
            key = 'most_recent_articles:domain:' + domain
            articles = cls.most_recent_per_attribute_n(origin_timestamp, 'domain', domain)
            r = redis.pipeline()
            r.delete(key)
            for article in articles:
                r.zadd(key, str(article['_id']), article['_score'])
            r.execute()

    @classmethod
    def __update_recent_articles_publishers(cls, origin_timestamp):
        publishers = redis.hgetall('publishers')
        for publisher in publishers:
            key = 'most_recent_articles:publisher:' + publisher
            articles = cls.most_recent_per_attribute_n(origin_timestamp, 'publisher', publisher)
            r = redis.pipeline()
            r.delete(key)
            for article in articles:
                r.zadd(key, str(article['_id']), article['_score'])
            r.execute()
