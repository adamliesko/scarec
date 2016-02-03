from elasticsearcher import es
from rediser import redis
from context.context import Context
import time


class Impression:
    ES_ITEM_INDEX = 'impressions'
    ES_ITEM_TYPE = 'impression'

    def __init__(self, content):
        self.content = content
        self.extracted_content = Context(content).extract_to_json()

        self.user_id = self.extracted_content[Context.MAPPINGS_INV['user_id']]
        self.item_id = self.extracted_content[Context.MAPPINGS_INV['item_id']]
        self.publisher_id = self.extracted_content[Context.MAPPINGS_INV['publisher_id']]

    def persist_impression(self):
        self.store_impression_to_es()
        self.store_user_impression_to_redis()

    def store_user_impression_to_redis(self):
        key = 'user_impressions:'+ str(self.user_id)
        redis.zadd(key, self.item_id, int(time.time()))

    def store_impression_to_es(self):
        body = {'user_id': self.user_id, 'item_id': self.item_id, 'publisher_id': self.publisher_id}
        es.index(index=self.ES_ITEM_INDEX, doc_type=self.ES_ITEM_TYPE, body=body)
