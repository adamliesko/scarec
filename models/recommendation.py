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
                        "limit": {
                            "type": "integer", "index": "not_analyzed", "store": "true"
                        }
                    }
                }
            }
        }
        return index

    def __init__(self, content):
        self.body = {}
        self.content = content
        self.extracted_content = Context(content).extract_to_json()
        self.parse_body()
        self.persist()
        self.user_id = self.body['user_id']
        self.limit = self.content['limit']

    def parse_body(self):
        for rec_property in self.PROPERTIES_TO_EXTRACT_AND_STORE:
            self.body[rec_property] = self.extracted_content[Context.MAPPINGS_INV[rec_property]]

        self.limit = self.body['limit']
        self.add_domain_id()
        self.add_timestamp()

    def add_domain_id(self):
        domain_id = redis.get('item_domain_pairs:' + str(self.body['item_id']))
        if domain_id:
            self.body['domain_id'] = domain_id

    def add_timestamp(self):
        self.body['timestamp'] = int(self.extracted_content['timestamp'] / 1000)

    def persist(self):
        self.store_to_es()

    def store_to_es(self):
        res = es.index(index=self.ES_ITEM_INDEX, doc_type=self.ES_ITEM_TYPE, body=self.body)
        if res['created']:
            self.id = res['_id']

    @classmethod
    def predict_context_cluster(cls, rec_req):
        context_vec = ContextEncoder.encode_context_to_dense_vec(rec_req.extracted_content)
        cluster = ClusteringModel.predict_cluster(context_vec)
        return cluster
