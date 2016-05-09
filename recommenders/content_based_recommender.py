from elasticsearcher import es
from recommenders.abstract_recommender import AbstractRecommender


class ContentBasedRecommender(AbstractRecommender):
    MAX_ARTICLES_RETRIEVED_FROM_REDIS = 50
    DEFAULT_POPULARITY_ATTRIBUTES = ['publisher_id', 'domain_id', 'category_id', 'channel_id', 'cluster_id']

   @classmethod
   def get_mlt_recs_for_items(item_ids, count=30):
     body = {
        "query": { "more_like_this": {
                "fields": [
                  'title', 'text'
        ],
        "docs": item_ids, "min_term_freq": 2, "max_query_terms": 40,
        "min_word_length": 2 }
        },
        "size": count }

        return es.search(index='items', body=body)