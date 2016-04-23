import logging
import os
import sys

import falcon

from api.items_resource import ItemsResource

sys.path.append(os.environ.get('PYTHONPATH'))

from api.recommendations_resource import RecommendationsResource
from api.events_resource import EventsResource
from api.errors_resource import ErrorsResource
from pyspark import SparkContext


logger = logging.getLogger(__name__)
logger.addHandler(logging.FileHandler('scarec.log'))
logger.setLevel(logging.INFO)

app = falcon.API()

items = ItemsResource()
recommendations = RecommendationsResource()
events = EventsResource()
errors = ErrorsResource()

app.add_route('/item', items)
app.add_route('/recommendation/{algorithm}/{time_interval}', recommendations)
app.add_route('/event', events)
app.add_route('/error', errors)
