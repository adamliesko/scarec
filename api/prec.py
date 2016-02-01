import falcon
from items_resource import ItemsResource
from recommendations_resource import RecommendationsResource
from events_resource import EventsResource
from errors_resource import ErrorsResource


app = falcon.API()

items = ItemsResource()
recommendations = RecommendationsResource()
events = EventsResource()
errors = ErrorsResource()

app.add_route('/item', items)
app.add_route('/recommendation', recommendations)
app.add_route('/event', events)
app.add_route('/error', errors)
