import json
import falcon
from models.item import Item


class ItemsResource:
    def on_post(self, req, resp):
        body = req.stream.read()

        if not body:
            raise falcon.HTTPBadRequest('Empty request body',
                                        'A valid JSON document is required.')
        data = json.loads(body.decode('utf-8'))
        item = Item(data)
        item.process_item_change_event()
        item.predict_for_clusters()  # TODO: get back to async processing with this
        resp.status = falcon.HTTP_200
