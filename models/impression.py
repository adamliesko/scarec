from elasticsearcher import es
from rediser import redis
from context.context import Context
import time


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
        self.persist_impression()

    def parse_body(self):
        for impr_property in self.PROPERTIES_TO_EXTRACT_AND_STORE:
            self.body[impr_property] = self.extracted_content[Context.MAPPINGS_INV[impr_property]]

        self.add_domain_id()
        self.add_timestamp()

    def add_domain_id(self):
        domain_id = redis.get('item_domain_pairs:' + str(self.body['item_id']))
        if domain_id:
            self.body['domain_id'] = domain_id

    def add_timestamp(self):
        self.body['timestamp'] = int(self.extracted_content['timestamp'] / 1000)

    def persist_impression(self):
        self.store_impression_to_es()
        self.store_user_impression_to_redis()

    def store_user_impression_to_redis(self):
        key = 'user_impressions:' + str(self.body['user_id'])
        redis.zadd(key, self.body['item_id'], int(time.time()))

    def store_impression_to_es(self):
        res = es.index(index=self.ES_ITEM_INDEX, doc_type=self.ES_ITEM_TYPE, body=self.body)
        if res['created']:
            self.id = res['_id']

    @classmethod
    def predict_context_cluster(cls, impression):
        impression.extracted_content


print(es.index(index=Impression.ES_ITEM_INDEX, doc_type=Impression.ES_ITEM_TYPE,
               body={'item_id': 1283210663, 'channel_id': [6, 13], 'timestamp': 1370296799, 'category_id': [1201425],
                     'publisher_id': 59436, 'user_id': 18741214652}))
