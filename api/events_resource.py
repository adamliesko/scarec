import falcon
import json
from models.impression import Impression


class EventsResource:
    def on_post(self, req, resp):
        body = req.stream.read()
        if not body:
            raise falcon.HTTPBadRequest('Empty request body',
                                        'A valid JSON document is required.')
        data = json.loads(body.decode('utf-8'))
        impression = Impression(data)
        impression.persist_impression()
        resp.status = falcon.HTTP_200
