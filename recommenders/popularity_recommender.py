from elasticsearcher import es
from rediser import redis


class PopularityRecommender:
    MAX_ARTICLES_RETRIEVED_FROM_REDIS = 50
    DEFAULT_POPULARITY_ATTRIBUTES = ['publisher_id', 'domain_id', 'category_id', 'channel_id', 'cluster_id']

    @classmethod
    def current_most_popular_global(cls, time_interval):
        key = cls.redis_key_for_global(time_interval)
        return cls.read_popular_articles_from_redis(key)

    @classmethod
    def current_most_popular_attribute(cls, time_interval, attribute, value):
        key = cls.redis_key_for_attribute(time_interval, attribute, value)
        return cls.read_popular_articles_from_redis(key)

    @classmethod
    def read_popular_articles_from_redis(cls, key):
        articles_with_scores_ary = redis.zrevrange(key, 0, cls.MAX_ARTICLES_RETRIEVED_FROM_REDIS, withscores=True)
        articles_with_scores = dict((article, score) for article, score in articles_with_scores_ary)
        return articles_with_scores

    @classmethod
    def most_popular_n(cls, origin_timestamp, count=50):
        body = {
            "size": 0,
            "query": {
                "range": {
                    "timestamp": {
                        "gte": origin_timestamp,
                        "format": "epoch_second||dateOptionalTime"
                    }
                }
            },
            "aggs": {
                "visits_count": {
                    "terms": {
                        "field": "item_id",
                        "size": count
                    }
                }
            }
        }

        res = es.search(index='impressions', body=body)
        return cls.build_aggs_dict_global(res)

    @classmethod
    def most_popular_per_attribute_n(cls, attribute, origin_timestamp, count=50):
        body = {
            "size": 0,
            "query": {
                "range": {
                    "timestamp": {
                        "gte": origin_timestamp,
                        "format": "epoch_second||dateOptionalTime"
                    }
                }
            },
            "aggs": {
                attribute + "_agg_count": {
                    "terms": {

                        "field": attribute
                    },
                    "aggs": {
                        "visits_count": {
                            "terms": {
                                "field": "item_id",
                                "size": count
                            }

                        }

                    }
                }
            }
        }
        res = es.search(index='impressions', body=body)
        return cls.build_aggs_dict(res, attribute)

    @classmethod
    def build_aggs_dict(cls, res, attribute=None):
        res_dict = {}
        for publisher in res['aggregations'][attribute + "_agg_count"]['buckets']:
            key = publisher['key']
            res_dict[key] = {}
            for doc in publisher['visits_count']['buckets']:
                res_dict[key][doc['key']] = doc['doc_count']

        return res_dict

    @classmethod
    def build_aggs_dict_global(cls, res):
        res_dict = {}
        for bucket in res['aggregations']['visits_count']['buckets']:
            res_dict[bucket['key']] = bucket['doc_count']

        return res_dict

    @classmethod
    def update_popular_articles(cls, origin_timestamp, time_interval,
                                attributes=DEFAULT_POPULARITY_ATTRIBUTES):
        cls.__update_popular_articles_global(origin_timestamp, time_interval)
        for attr in attributes:
            cls.__update_popular_articles_attribute(attr, origin_timestamp, time_interval)

    @classmethod
    def __update_popular_articles_attribute(cls, attribute, origin_timestamp, time_interval):
        attribute_values = cls.most_popular_per_attribute_n(attribute, origin_timestamp)
        for value in attribute_values:
            key = cls.redis_key_for_attribute(time_interval, attribute, value)
            r = redis.pipeline()
            r.delete(key)
            for article in attribute_values[value]:
                r.zadd(key, article, attribute_values[value][article])
            r.execute()

    @classmethod
    def __update_popular_articles_global(cls, origin_timestamp, time_interval):
        key = cls.redis_key_for_global(time_interval)
        articles = cls.most_popular_n(origin_timestamp)
        r = redis.pipeline()
        r.delete(key)
        for article_key in articles:
            r.zadd(key, article_key, articles[article_key])
        r.execute()

    @staticmethod
    def redis_key_for_global(time_interval):
        return 'most_popular_articles:' + time_interval

    @staticmethod
    def redis_key_for_attribute(time_interval, attribute, value):
        return 'most_popular_articles:' + str(time_interval) + ':' + str(attribute) + ':' + str(value)

    @classmethod
    def get_most_popular_articles_global(cls, time_interval, count=50):
        redis.zrange(cls.redis_key_for_global(time_interval), 0, count, withscores=True, desc=True)