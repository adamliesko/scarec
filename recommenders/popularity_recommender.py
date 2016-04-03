from elasticsearcher import es
from rediser import redis


class PopularityRecommender:
    @classmethod
    def recommend_to_user(cls, user_id, request):
        pass

    @classmethod
    def recommend_to_request(cls, request):
        pass

    @classmethod
    def most_popular_n(cls, origin_timestamp, count=30):
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
    def most_popular_per_attribute_n(cls, attribute, origin_timestamp, count=30):
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
                                attributes=['publisher_id', 'domain_id', 'category_id', 'channel_id', 'cluster_id']):
        cls.__update_popular_articles_global(origin_timestamp, time_interval)
        for attr in attributes:
            cls.__update_popular_articles_attribute(attr, origin_timestamp, time_interval)

    @classmethod
    def __update_popular_articles_attribute(cls, attribute, origin_timestamp, time_interval):
        attribute_values = cls.most_popular_per_attribute_n(attribute, origin_timestamp)
        for value in attribute_values:
            key = 'most_popular_articles:' + time_interval + ':' + attribute + ':' + value
            r = redis.pipeline()
            r.delete(key)
            for article in attribute_values[value]:
                r.zadd(key, article, attribute_values[value][article])
            r.execute()

    @classmethod
    def __update_popular_articles_global(cls, origin_timestamp, time_interval):
        key = 'most_popular_articles:' + time_interval
        articles = cls.most_popular_n(origin_timestamp)
        r = redis.pipeline()
        r.delete(key)
        for article_key in articles:
            r.zadd(key, article_key, articles[article_key])
        r.execute()
