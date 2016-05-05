import json
import logging

import falcon

from models.item import Item


class ItemsResource:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def on_post(self, req, resp):
        body = req.stream.read()

        if not body:
            raise falcon.HTTPBadRequest('Empty request body',
                                        'A valid JSON document is required.')
        data = json.loads(body.decode('utf-8'))
        article = Item(data)
        article.process_item_change_event()
        resp.status = falcon.HTTP_200
