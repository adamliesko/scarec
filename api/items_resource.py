# Let's get this party started
import falcon
import json
import logging
from models.item import Item


# Falcon follows the REST architectural style, meaning (among
# other things) that you think in terms of resources and state
# transitions, which map to HTTP verbs.
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
        resp.status = falcon.HTTP_200  # This is the default status
