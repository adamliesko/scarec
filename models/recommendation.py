import time

from elasticsearcher import es
from rediser import redis
from contextual.context import Context
from contextual.context_encoder import ContextEncoder
from clustering.clustering_model import ClusteringModel


class Recommendation:
    ES_ITEM_INDEX = 'recommendations'
    ES_ITEM_TYPE = 'recommendation'
    PROPERTIES_TO_EXTRACT_AND_STORE = ['user_id', 'publisher_id', 'channel_id', 'category_id', 'item_id']
    PROPERTIES = PROPERTIES_TO_EXTRACT_AND_STORE + ['timestamp']

    @classmethod
    def index_properties(cls):
        index = {
            "mappings": {
                "recommendation": {
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
                        "limit": {
                            "type": "integer", "index": "not_analyzed", "store": "true"
                        },
                        "encoded_context": {
                            "type": "integer", "store": 'true', "index": "not_analyzed"
                        }
                    }
                }
            }
        }
        return index

    # this is way too slow in prod: TODO: async processing - what about sc (SparkContext) this will hurt ?!
    def __init__(self, content):
        self.id = None
        self.cluster_id = None
        self.body = None
        self.extracted_content = None
        self.context_vec = None
        self.content = content
        self.parse_body()
        self.user_id = self.body['user_id']
        self.limit = self.content['limit']
        self.publisher_id = self.body['publisher_id']
        self.predict_context_cluster()

    def parse_body(self):
        self.body = {}
        self.extracted_content = Context(self.content).extract_to_json()
        for rec_property in self.PROPERTIES_TO_EXTRACT_AND_STORE:
            self.body[rec_property] = self.extracted_content[Context.MAPPINGS_INV[rec_property]]

        self.limit = self.content['limit']
        self.add_domain_id()
        self.add_timestamp()

    def add_domain_id(self):
        domain_id = redis.get('item_domain_pairs:' + str(self.body['item_id']))
        if domain_id:
            self.body['domain_id'] = int(domain_id.decode('utf-8'))
        else:  # we don't yet have the id of the domain from the create/update stream in db, what shall we do?
            raise Exception("not a good choice")

    def add_timestamp(self):
        self.body['timestamp'] = int(time.time() / 1000)

    def persist(self):
        self.store_to_es()

    def store_to_es(self):
        res = es.index(index=self.ES_ITEM_INDEX, doc_type=self.ES_ITEM_TYPE, body=self.body)
        if res['created']:
            self.id = res['_id']

    def predict_context_cluster(self):
        self.context_vec = ContextEncoder.encode_context_to_dense_vec(self.extracted_content)
        self.cluster_id = ClusteringModel.predict_cluster(self.context_vec)
        self.body['cluster_id'] = self.cluster_id
        return self.cluster_id

        # self.body['encoded_context'] = [i for i in range(0, len(self.context_vec)) if self.context_vec[i] == 1]
        # TODO if we are storing it to es, probably not ever needed, do it only for impression
