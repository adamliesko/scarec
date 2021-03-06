from elasticsearcher import es
from rediser import redis
# from item_enrichers.enricher import Enricher
from clustering.clustering_model import ClusteringModel
from classification.random_forester import RandomForester
from recommenders.contextual_recommender import ContextualRecommender
import time


class Item:
    ES_ITEM_INDEX = 'items'
    ES_ITEM_TYPE = 'article'
    PREDICTED_ITEMS_KEY = 'rf:predicted_items'

    def __init__(self, content):
        self.content = content
        self.id = content["id"]
        self.content['domain_id'] = self.content['domainid']

    def prepare_for_indexing(self):
        self.parse_timestamp_fields()

    # just convert ['created_at', 'updated_at', 'published_at'] to the echo timestamp (seconds, not milis)
    def parse_timestamp_fields(self):
        time_fields = ['created_at', 'updated_at', 'published_at']
        for time_attr in time_fields:
            if self.content[time_attr]:
                self.content[time_attr] = int(time.mktime(time.strptime(self.content[time_attr], "%Y-%m-%d %H:%M:%S")))

    # store item domain_id value to the redis, so that we can retrieve it later when working with user impressions
    def store_item_domain_key_pair(self):
        key = 'item_domain_pairs:' + self.id
        domain = redis.get(key)
        if domain:
            return False
        else:
            domain_id = self.content['domainid']
            redis.set('item_domain_pairs:' + self.id, domain_id)
            return True

    # handle both update/create actions for item
    def process_item_change_event(self):
        self.prepare_for_indexing()
        new_domain = self.store_item_domain_key_pair()
        if new_domain:
            pass
            # enriched_content = Enricher.enrich_article(self.content["url"])
            # TODO: add enriched content to the to be indexed item body
        es.index(index=self.ES_ITEM_INDEX, doc_type=self.ES_ITEM_TYPE, body=self.content, id=self.id)

    # TODO: we are ignoring here domain and publisher on the fly, we need to build caching for this sim. as for ALS id's (long to int conversion)
    # this is triggered after impression with new item
    @classmethod
    def predict_for_clusters(cls, vector, item_id):
        for cluster_id in ClusteringModel.CLUSTERS:
            result_score = RandomForester.predict_new_item(vector, item_id, cluster_id)
            ContextualRecommender.add_prediction(item_id, result_score, cluster_id)
        cls.add_item_as_predicted(item_id)

    @classmethod
    def is_predictable(cls, item_id):
        return not redis.sismember(cls.PREDICTED_ITEMS_KEY, item_id)

    @classmethod
    def add_item_as_predicted(cls, item_id):
        redis.sadd(cls.PREDICTED_ITEMS_KEY, item_id)

    # on each model relearning we should simply reset this cache #TODO
    @classmethod
    def reset_predicted_items_cache(cls):
        redis.delete(cls.PREDICTED_ITEMS_KEY)

    @classmethod
    def index_properties(cls):
        return {"mappings": {
            "article": {
                "properties": {
                    "publisher": {
                        "type": "string", "index": "not_analyzed", "store": "true"
                    },
                    "domain_id": {
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
