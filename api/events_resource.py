import json

import falcon
from redis import Redis
from rq import Queue

from models.impression import Impression


class EventsResource:
    queue_impression_clustering = Queue(connection=Redis())

    def on_post(self, req, resp):
        body = req.stream.read()
        if not body:
            raise falcon.HTTPBadRequest('Empty request body',
                                        'A valid JSON document is required.')
        data = json.loads(body.decode('utf-8'))
        impression = Impression(data)
        Impression.predict_context_cluster(impression)
        resp.status = falcon.HTTP_200
