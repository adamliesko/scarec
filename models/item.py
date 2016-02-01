from elasticsearcher import es
from rediser import redis
import item_enrichers.enricher as enricher
import time
import urllib.parse as parse


class Item:
    ES_ITEM_INDEX = 'items'
    ES_ITEM_TYPE = 'article'

    def __init__(self, content):
        self.content = content

    def prepare_for_indexing(self):
        self.parse_timestamp_fields()

    def parse_timestamp_fields(self):
        time_fields = ['created_at', 'updated_at', 'published_at']
        for time_attr in time_fields:
            if self.content[time_attr]:
                self.content[time_attr] = int(time.mktime(time.strptime(self.content[time_attr], "%Y-%m-%d %H:%M:%S")))

    def process_item_change_event(self):
        # TODO: ziskat alchemy a diffbot veci
        self.prepare_for_indexing()
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


acd = {"domainid": "418", "created_at": "2014-06-01 09:23:52", "flag": 0,
       "img": "http:\/\/www.ksta.de\/image\/view\/23088116,19853145,lowRes,urn-newsml-dpa-com-20090101-130601-99-00642_large_4_3.jpg",
       "title": "NBA: Drew neuer Trainer bei den Milwaukee Bucks",
       "text": "Larry Drew ist neuer Trainer bei den Milwaukee Bucks. Das teilte der Basketball-Club aus der nordamerikanischen Profiliga ...",
       "url": "http://www.ksta.de/sport/nba--drew-neuer-trainer-bei-den-milwaukee-bucks,15189364,23088118.html",
       "expires_at": 1385623432, "published_at": None, "version": 1, "updated_at": "2014-06-01 23:40:59",
       "id": "12805437038",}
# es.indices.create('items', body=Item.index_properties())

i = Item(acd)
i.process_item_change_event()
print(Item.most_recent_per_domain_n("418"))
