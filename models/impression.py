import time

from elasticsearcher import es
from rediser import redis
from contextual.context import Context
from contextual.context_encoder import ContextEncoder
from clustering.clustering_model import ClusteringModel
from utils import Utils
from models.item import Item
from pyspark.mllib.linalg import Vectors, SparseVector, DenseVector


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
        impression = Impression(content)
        return impression

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
        self.item_id = self.body['item_id']

    def add_domain_id(self):
        if self.body.get('item_id', None) is not None:
            domain_id = redis.get('item_domain_pairs:' + str(self.body['item_id']))
            if domain_id:
                self.body['domain_id'] = int(domain_id.decode('utf-8'))

    def add_timestamp(self):
        self.body['timestamp'] = int(self.extracted_content['timestamp'])

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

    # TODO: this should be extracted somewhere else, background processing - actually hell of a lot of work
    # ignoring domain and publisher for now I think
    # we should probably warm the cache of model per clusters, takes couple ofseconds on first hit
    def predict_new_item_for_clusters(self):
        if Item.is_predictable(self.item_id):
            kws = self.content['context']['clusters'].get('33', {})
            item_int_content = {int(k): int(v) for k, v in kws.items()}
            item_vector = SparseVector(300, item_int_content)
            Item.predict_for_clusters(item_vector, self.item_id)

    @classmethod
    def user_impressions(cls, user_id):
        key = 'user_impressions:' + str(user_id)
        impressions = redis.zrange(key, 0, -1, True)
        return impressions

    @staticmethod  # windowed visits for more efficient als model re-learning
    def store_windowed_visit_to_redis(user_id, item_id):
        key_visits_in_last_hour = 'windowed_visits:' + str(
            Utils.round_time_to_last_hour_as_epoch())
        enc_user_id = Utils.encode_attribute('user_id', user_id)
        enc_item_id = Utils.encode_attribute('item_id', item_id)
        redis.sadd(key_visits_in_last_hour, ':'.join([str(enc_user_id), str(enc_item_id)]))
