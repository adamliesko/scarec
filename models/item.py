from elasticsearcher import es
from rediser import redis
import time


class Item:
    ES_ITEM_INDEX = 'items'
    ES_ITEM_TYPE = 'article'

    def __init__(self, content):
        self.content = content

    def prepare_for_indexing(self):
        self.parse_timestamp_fields()
        self.store_item_domain_key_pair()

    #just convert ['created_at', 'updated_at', 'published_at'] to the echo timestamp (seconds, not milis)
    def parse_timestamp_fields(self):
        time_fields = ['created_at', 'updated_at', 'published_at']
        for time_attr in time_fields:
            if self.content[time_attr]:
                self.content[time_attr] = int(time.mktime(time.strptime(self.content[time_attr], "%Y-%m-%d %H:%M:%S")))

    # store item domain_id value to the redis, so that we can retrieve it later when working with user impressions
    def store_item_domain_key_pair(self):
        redis.set('item_domain_pairs:' + self.content["id"])

    def process_item_change_event(self):
        # TODO: ziskat alchemy a diffbot veci
        self.prepare_for_indexing()
        # enriched_content = enricher.Enricher.enrich_article(self.content["url"])
        # print(enriched_content)
        es.index(index=self.ES_ITEM_INDEX, doc_type=self.ES_ITEM_TYPE, body=self.content, id=self.content["id"])

    @classmethod
    def index_properties(cls):
        return {"mappings": {
            "article": {
                "properties": {
                    "publisher": {
                        "type": "string", "index": "not_analyzed", "store": "true"
                    },
                    "domaind": {
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

    @classmethod
    def visit_index_properties(cls):
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
