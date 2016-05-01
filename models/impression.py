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
        self.cluster_id = None
        self.context_vec = None
        self.id = None
        self.body = {}
        self.content = content
        self.extracted_content = Context(content).extract_to_json()
        self.parse_body()
        self.persist()

    def parse_body(self):
        for impression_property in self.PROPERTIES_TO_EXTRACT_AND_STORE:
            self.body[impression_property] = self.extracted_content[Context.MAPPINGS_INV[impression_property]]

        self.add_domain_id()
        self.add_timestamp()
        self.predict_context_cluster()
        self.body['cluster_id'] = self.cluster_id

    def add_domain_id(self):
        if self.body.get('item_id', None) is not None:
            domain_id = redis.get('item_domain_pairs:' + str(self.body['item_id']))
            if domain_id:
                self.body['domain_id'] = domain_id

    def add_timestamp(self):
        self.body['timestamp'] = int(self.extracted_content['timestamp'] / 1000)

    def persist(self):
        self.store_impression_to_es()
        self.store_user_impression_to_redis()

    def store_user_impression_to_redis(self):
        if self.body.get('item_id', None) is not None and self.body.get('user_id', None) is not None and str(
                self.body['user_id']) != '0':  # dont store unknown user visits
            item_id = str(self.body['item_id'])
            user_id = str(self.body['user_id'])

            key_user_visits = 'user_impressions:' + user_id
            redis.zadd(key_user_visits, item_id, int(time.time()))
            # user_id:item_id pair, last hour
            self.store_windowed_visit_to_redis(user_id, item_id)

    def store_impression_to_es(self):
        res = es.index(index=self.ES_ITEM_INDEX, doc_type=self.ES_ITEM_TYPE, body=self.body)
        if res['created']:
            self.id = res['_id']

    def update_impression_in_es(self):
        es.update(index=self.ES_ITEM_INDEX, id=self.id, doc_type=self.ES_ITEM_TYPE, body=self.body)

    def predict_context_cluster(self):
        self.context_vec = ContextEncoder.encode_context_to_dense_vec(self.extracted_content)
        self.cluster_id = ClusteringModel.predict_cluster(self.context_vec)
        self.body['encoded_context'] = [i for i in range(0, len(self.context_vec)) if self.context_vec[i] == 1]

    @classmethod
    def user_impressions(cls, user_id):
        key = 'user_impressions:' + str(user_id)
        impressions = redis.zrange(key, 0, -1, True)
        return impressions

    @staticmethod
    def store_windowed_visit_to_redis(user_id, item_id):
        key_visits_in_last_hour = 'windowed_visits:' + str(
            Utils.round_time_to_last_hour_as_epoch())
        enc_user_id = Utils.encode_attribute('user_id', user_id)
        enc_item_id = Utils.encode_attribute('item_id', item_id)
        redis.sadd(key_visits_in_last_hour, ':'.join([str(enc_user_id), str(enc_item_id)]))
