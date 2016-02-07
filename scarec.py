import falcon
from api.items_resource import ItemsResource
from api.recommendations_resource import RecommendationsResource
from api.events_resource import EventsResource
from api.errors_resource import ErrorsResource

app = falcon.API()

items = ItemsResource()
recommendations = RecommendationsResource()
events = EventsResource()
errors = ErrorsResource()

app.add_route('/item', items)
app.add_route('/recommendation', recommendations)
app.add_route('/event', events)
app.add_route('/error', errors)
