from elasticsearcher import es
from rediser import redis
from recommenders.abstract_recommender import AbstractRecommender


class ContentBasedRecommender(AbstractRecommender):
    MAX_ARTICLES_RETRIEVED_FROM_REDIS = 50
    DEFAULT_POPULARITY_ATTRIBUTES = ['publisher_id', 'domain_id', 'category_id', 'channel_id', 'cluster_id']

   @classmethod
   def get_mlt_recs_for_items(item_ids):

