from rediser import redis
from elasticsearcher import es
import time

class UserVisit:
    ES_ITEM_INDEX = 'user_visits'
    ES_ITEM_TYPE = 'impression'

    def __init__(self, context, user):
        self.user = user
        self.context =

    def user_visited_items(self):
        redis.get('user_visits:' + self.user)
        pass

    def user_visited_items_intersection(self, item_ids):
        pass

    def user_has_not_visited_items(self, item_ids):
        pass

    def add_user_item_visit(self, item_id, timestamp):
        pass

    def user_most_recent_visited_items(self, item_count):
        pass

    @classmethod
    def get_item_visits_last(cls, hours, domain='global'):
        key = 'item_visits:' + domain + ':last:' + str(hours)
        redis.hgetall(key)




    def prepare_for_indexing(self):
        pass

    def process_item_change_event(self):
        #self.prepare_for_indexing()
        # enriched_content = enricher.Enricher.enrich_article(self.content["url"])
        # print(enriched_content)
        es.index(index=self.ES_ITEM_INDEX, doc_type=self.ES_ITEM_TYPE, body=self.content, id=self.content["id"])

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
        res = es.search(index=cls.ES_ITEM_INDEX, body=body)
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
        res = es.search(index=cls.ES_ITEM_INDEX, body=body)
        return res['hits']['hits']

    @classmethod
    def index_properties(cls):
        return {"mappings": {
            "article": {
                "properties": {
                    "id": {
                        "type": "string", "index": "not_analyzed"
                    },
                    "url": {
                        "type": "string", "index": "not_analyzed", "store": "true"
                    },
                    "domainid": {
                        "type": "integer", "index": "not_analyzed", "store": "true"
                    },
                    "title": {
                        "type": "string", "analyzer": "german", "store": "true"
                    },
                    "text": {
                        "type": "string", "analyzer": "german", "store": "true"
                    },
                    "title_alchemy": {
                        "type": "string", "analyzer": "german", "store": "true"
                    },
                    "text_alchemy": {
                        "type": "string", "analyzer": "german", "store": "true"
                    },
                    "title_diffbot": {
                        "type": "string", "analyzer": "german", "store": "true"
                    },
                    "text_diffbot": {
                        "type": "string", "analyzer": "german", "store": "true"
                    },
                    "created_at": {
                        "type": "date", "store": "true", "format": "epoch_second||dateOptionalTime"
                    },
                    "expires_at": {
                        "type": "date", "store": "true", "format": "epoch_second||dateOptionalTime"
                    },
                    "updated_at": {
                        "type": "date", "store": "true", "format": "epoch_second||dateOptionalTime"
                    },
                    "published_at": {
                        "type": "date", "format": "epoch_second||dateOptionalTime", "store": "true"
                    },
                    "flag": {
                        "type": "integer", "index": "not_analyzed", "store": "true"
                    },
                    "version": {
                        "type": "integer", "index": "not_analyzed"
                    },
                    "img": {
                        "type": "string", "index": "not_analyzed"
                    }
                }
            }
        }
        }
