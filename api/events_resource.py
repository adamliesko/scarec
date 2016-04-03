import falcon
import json
from models.impression import Impression
from redis import Redis
from rq import Queue


class EventsResource:
    queue_impression_clustering = Queue(connection=Redis())

    def on_post(self, req, resp):
        body = req.stream.read()
        if not body:
            raise falcon.HTTPBadRequest('Empty request body',
                                        'A valid JSON document is required.')
        data = json.loads(body.decode('utf-8'))
        impression = Impression(data)
        impression.persist_impression()
        print(impression.extracted_content)
        Impression.predict_context_cluster(impression.extracted_content)
        # async processing to be self.queue_impression_clustering.enqueue(Impression.predict_context_cluster(impression))
        resp.status = falcon.HTTP_200
