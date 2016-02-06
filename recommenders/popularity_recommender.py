import time
from elasticsearcher import es
from rediser import redis
from models import item as item


class PopularityRecommender:
    # tu bude mapping typ, kluc, timestamp
    INTERVALS = {'1h': {'key': 'most_recent:1h'},
                 '4h': {},
                 '24h': {},
                 '48h': {},
                 '72h': {}}

    @classmethod
    def most_recent_n(cls, count=30, scale="24h", offset="1h", decay=0.5):
        current_epoch_time = int(time.time())
        body = {
            "query": {
                "function_score": {
                    "functions": [
                        {
                            "gauss": {
                                "created_at": {
                                    "origin": current_epoch_time,
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
                                    "origin": current_epoch_time,
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
        res = es.search(index=item.ES_ITEM_INDEX, body=body)
        return res['hits']['hits']

    @classmethod
    def most_recent_per_domain_n(cls, domain, count=30, scale="24h", offset="1h", decay=0.5):
        current_epoch_time = int(time.time())
        body = {
            "query": {
                "function_score": {
                    "query": {
                        "filtered": {
                            "query": {
                                "match": {"domainid": int(domain)}}}},
                    "functions": [
                        {
                            "gauss": {
                                "created_at": {
                                    "origin": current_epoch_time,
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
                                    "origin": current_epoch_time,
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
        res = es.search(index=item.ES_ITEM_INDEX, body=body)
        return res['hits']['hits']

    @classmethod
    def update_recent_articles(cls):
        pass

    @classmethod
    def update_recent_articles_global(cls, time_interval):
        pass

    @classmethod
    def update_recent_articles_domains(cls, time_interval):
        pass

    @classmethod
    def update_recent_articles_publisher(cls, time_interval):
        pass

    @classmethod
    def update_recent_articles_publisher(cls, time_interval):
        pass
