from elasticsearcher import es
from rediser import redis
from recommenders.abstract_recommender import AbstractRecommender


class PopularityRecommender(AbstractRecommender):
    MAX_ARTICLES_RETRIEVED_FROM_REDIS = 50
    DEFAULT_POPULARITY_ATTRIBUTES = ['publisher_id', 'domain_id', 'category_id', 'channel_id', 'cluster_id']

    @classmethod
    def read_popular_articles_from_redis(cls, key):
        articles_with_scores_ary = redis.zrevrange(key, 0, cls.MAX_ARTICLES_RETRIEVED_FROM_REDIS, withscores=True)
        articles_with_scores = dict((article, score) for article, score in articles_with_scores_ary)
        return articles_with_scores

    @classmethod
    def most_popular_n(cls, origin_timestamp, count=50):
        print(origin_timestamp)
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
        print(attribute)
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
        if len(articles) > 0:  # delete and update only if there are some
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
        pop_dictie = {}
        key = cls.redis_key_for_global(time_interval)

        pop_articles = redis.zrange(key, 0, count, withscores=True, desc=True)
        if len(pop_articles) == 0:
            return {}

        max_score = pop_articles[0][1]
        for article_id, score in pop_articles:
            pop_dictie[int(article_id.decode('utf-8'))] = (float(score) / max_score) + 1

        return pop_dictie

    @classmethod
    def get_most_popular_articles_attribute(cls, time_interval, attribute, attribute_value, count=50):
        pop_dictie = {}
        key = cls.redis_key_for_attribute(time_interval, attribute, attribute_value)

        pop_articles = redis.zrange(key, 0, count, withscores=True, desc=True)
        if len(pop_articles) == 0:
            return {}

        max_score = pop_articles[0][1]
        for article_id, score in pop_articles:
            pop_dictie[int(article_id.decode('utf-8'))] = (float(score) / max_score) + 1

        return pop_dictie
