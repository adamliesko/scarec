import time
from elasticsearcher import es
from rediser import redis
from models import item as item


class PopularityRecommender:
    @classmethod
    def most_popular_n(cls, origin_timestamp, count=30):
        body = {"size": 0,
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
        body = {"size": 0,
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
    def update_popular_articles(cls, origin_timestamp, time_interval):
        cls.__update_popular_articles_global( origin_timestamp, time_interval)
        cls.__update_popular_articles_domains(origin_timestamp, time_interval)
        cls.__update_popular_articles_publishers(origin_timestamp, time_interval)

    @classmethod
    def __update_popular_articles_global(cls, origin_timestamp, time_interval):
        key = 'most_popular_articles:' + time_interval
        articles = cls.most_popular_n(origin_timestamp)
        r = redis.pipeline()
        r.delete(key)
        for article_key in articles:
            r.zadd(key, article_key, articles[article_key])
        r.execute()

    @classmethod
    def __update_popular_articles_domains(cls, origin_timestamp, time_interval):
        domains = cls.most_popular_per_attribute_n('domain_id', origin_timestamp)
        for domain in domains:
            key = 'most_popular_articles:' + time_interval + ':domain:' + domain
            r = redis.pipeline()
            r.delete(key)
            for article in domains[domain]:
                r.zadd(key, article, domains[domain][article])
            r.execute()

    @classmethod
    def __update_popular_articles_publishers(cls, origin_timestamp, time_interval):
        publishers = cls.most_popular_per_attribute_n('publisher_id', origin_timestamp)
        for publisher in publishers:
            key = 'most_popular_articles: ' + time_interval + ':publisher:' + str(publisher)
            r = redis.pipeline()
            r.delete(key)
            for article in publishers[publisher]:
                r.zadd(key, article, publishers[publisher][article])
            r.execute()

#print(PopularityRecommender.update_popular_articles(137029674, '1h'))

# TODO: FIXME: bacha na pomenovanie tych blbych atributov v ES a tuto .. potrebujem tu mat current_timestamp atd?
# FIXME: pozor na str vs int pri klucoch
# zabezpecit aby sa tam domena pridavala spravne pri novych clankoch a podobne, aby to nepadlo atd