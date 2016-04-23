import time

from elasticsearcher import es
from rediser import redis
from contextual.context import Context
from contextual.context_encoder import ContextEncoder
from clustering.clustering_model import ClusteringModel
from utils import Utils


# FIRST_TIME_SETUP: es.indices.create(index=Impression.ES_ITEM_INDEX, body=Impression.index_properties())

class Impression:
    ES_ITEM_INDEX = 'impressions'
    ES_ITEM_TYPE = 'impression'
    PROPERTIES_TO_EXTRACT_AND_STORE = ['user_id', 'publisher_id', 'channel_id', 'category_id', 'item_id']
    PROPERTIES = PROPERTIES_TO_EXTRACT_AND_STORE + ['cluster_id', 'timestamp']

    @classmethod
    def index_properties(cls):
        index = {
            "mappings": {
                "impression": {
                    "properties": {
                        "publisher_id": {
                            "type": "integer", "index": "not_analyzed", "store": "true"
                        },
                        "domain_id": {
                            "type": "integer", "index": "not_analyzed", "store": "true"
                        },
                        "channel_id": {
                            "type": "integer", "index": "not_analyzed", "store": "true"
                        },
                        "category_id": {
                            "type": "integer", "index": "not_analyzed", "store": "true"
                        },
                        "user_id": {
                            "type": "long", "index": "not_analyzed", "store": "true"
                        },
                        "item_id": {
                            "type": "long", "index": "not_analyzed", "store": "true"
                        },
                        "cluster_id": {
                            "type": "integer", "index": "not_analyzed", "store": "true"
                        },
                        "timestamp": {
                            "type": "date", "store": "true", "format": "epoch_second||dateOptionalTime"
                        },
                        "encoded_context": {
                            "type": "integer", "store": 'true', "index": "not_analyzed"
                        }
                    }
                }
            }
        }
        return index

    @classmethod
    def process_new(cls, content):
        Impression(content)

    def __init__(self, content):
        self.body = {}
        self.content = content
        self.extracted_content = Context(content).extract_to_json()
        self.parse_body()
        self.persist()
        self.cluster_id = None

    def parse_body(self):
        for impr_property in self.PROPERTIES_TO_EXTRACT_AND_STORE:
            self.body[impr_property] = self.extracted_content[Context.MAPPINGS_INV[impr_property]]

        self.add_domain_id()
        self.add_timestamp()
        self.cluster_id = self.predict_context_cluster(self)
        self.body['cluster_id'] = self.cluster_id

    def add_domain_id(self):
        domain_id = redis.get('item_domain_pairs:' + str(self.body['item_id']))
        if domain_id:
            self.body['domain_id'] = domain_id

    def add_timestamp(self):
        self.body['timestamp'] = int(self.extracted_content['timestamp'] / 1000)

    def persist(self):
        self.store_impression_to_es()
        self.store_user_impression_to_redis()

    def store_user_impression_to_redis(self):
        item_id = str(self.body['item_id'])
        user_id = str(self.body['user_id'])

        key_user_visits = 'user_impressions:' + user_id
        # user_id:item_id pair, last hour
        key_visits_in_last_hour = 'visits:' + str(
            Utils.round_time_to_last_hour_as_epoch())
        redis.zadd(key_user_visits, item_id, int(time.time()))
        redis.sadd(key_visits_in_last_hour, ':'.join[user_id, item_id])

    def store_impression_to_es(self):
        print(self.body)
        res = es.index(index=self.ES_ITEM_INDEX, doc_type=self.ES_ITEM_TYPE, body=self.body)
        if res['created']:
            self.id = res['_id']

    def update_impression_in_es(self):
        es.update(index=self.ES_ITEM_INDEX, id=self.id, doc_type=self.ES_ITEM_TYPE, body=self.body)

    @classmethod
    def user_impressions(cls, user_id):
        key = 'user_impressions:' + str(user_id)
        impressions = redis.zrange(key, 0, -1, True, withscores=True)
        return impressions

    @classmethod
    def predict_context_cluster(cls, impression):
        context_vec = ContextEncoder.encode_context_to_dense_vec(impression.extracted_content)
        cluster = ClusteringModel.predict_cluster(context_vec)
        return cluster
