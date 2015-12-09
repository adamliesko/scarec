from elasticsearch import Elasticsearch
import elasticsearch


class EsClient(object):
    client = None

    def __new__(self):
        if self.client is None:
            self.client = Elasticsearch()
        else:
            return self.client

    def __init__(self, **kwargs):
        self.client = Elasticsearch()




elasticsearch.client