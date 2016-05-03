from elasticsearcher import es
from rediser import redis
from models.item import Item
from recommenders.abstract_recommender import AbstractRecommender


class RecencyRecommender(AbstractRecommender):
    DEFAULT_RECENCY_ATTRIBUTES = ['publisher_id', 'domain_id', 'category_id', 'channel_id', 'cluster_id']

    @classmethod
    def most_recent_n(cls, origin_timestamp, count=500, scale="24h", offset="2h", decay=0.5):
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
    def most_recent_per_attribute_n(cls, origin_timestamp, attribute, attribute_value, count=500, scale="24h",
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
    def update_recent_articles(cls, origin_timestamp, attributes=DEFAULT_RECENCY_ATTRIBUTES):
        cls.__update_recent_articles_global(origin_timestamp)
        # for attribute in attributes:
        # cls.__update_recent_articles_attribute(attribute, origin_timestamp)

    @classmethod
    def __update_recent_articles_global(cls, origin_timestamp):
        key = cls.redis_key_for_global()
        articles = cls.most_recent_n(origin_timestamp)
        r = redis.pipeline()
        r.delete(key)
        for article in articles:
            r.zadd(key, str(article['_id']), article['_score'])
        r.execute()

    @classmethod
    def __update_recent_articles_attribute(cls, attribute, origin_timestamp):
        # TODO get actual values for attributes and compute it per attribute:attribute_value key set
        attribute_values = cls.most_recent_per_attribute_n(attribute, origin_timestamp)
        for value in attribute_values:
            key = cls.redis_key_for_attribute(attribute, value)
            r = redis.pipeline()
            r.delete(key)
            for article in attribute_values[value]:
                r.zadd(key, article, attribute_values[value][article])
            r.execute()

    @classmethod
    def __update_popular_articles_global(cls, origin_timestamp):
        key = cls.redis_key_for_global()
        articles = cls.most_recent_n(origin_timestamp)
        r = redis.pipeline()
        r.delete(key)
        for article_key in articles:
            r.zadd(key, article_key, articles[article_key])
        r.execute()

    @classmethod
    def get_most_recent_articles_global(cls, count=500):
        redis.zrange(cls.redis_key_for_global(), 0, count, withscores=True, desc=True)

    @staticmethod
    def redis_key_for_global():
        return 'most_recent_articles:'

    @staticmethod
    def redis_key_for_attribute(attribute, value):
        return 'most_recent_articles:' + str(attribute) + ':' + str(value)
