from models.impression import Impression
from models.item import Item
from models.recommendation import Recommendation
from elasticsearcher import es

# Creates Elasticsearch indices.
# Intentionally does not throw an error if indices already exist in the Elasticsearch database.

es.indices.create(index=Impression.ES_ITEM_INDEX, body=Impression.index_properties(), ignore=400)
es.indices.create(index=Item.ES_ITEM_INDEX, body=Item.index_properties(), ignore=400)
es.indices.create(index=Recommendation.ES_ITEM_INDEX, body=Recommendation.index_properties(), ignore=400)
