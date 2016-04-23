import json

import falcon

from models.impression import Impression


class EventsResource:
    #  queue_impression_clustering = Queue('impression_processing', connection=Redis(db=13))

    def on_post(self, req, resp):
        body = req.stream.read()
        if not body:
            raise falcon.HTTPBadRequest('Empty request body',
                                        'A valid JSON document is required.')
        data = json.loads(body.decode('utf-8'))
        # self.queue_impression_clustering.enqueue(Impression.process_new, data)
        Impression.process_new(data)
        resp.status = falcon.HTTP_200
