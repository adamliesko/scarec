from elasticsearcher import es
from recommenders.abstract_recommender import AbstractRecommender

# to do boost current item to max
class ContentBasedRecommender(AbstractRecommender):
    @classmethod
    def get_mlt_recs_for_items(cls, item_ids, count=30):
        docs = []
        for item_id in item_ids:
            doc = {"_index": 'items', "_type": "article", "_id": item_id}
            docs.append(doc)

        body = {
            "query": {"more_like_this": {
                "fields": [
                    'title', 'text'
                ],
                "like": docs, "min_term_freq": 1, "max_query_terms": 30,
                "min_word_length": 1}
            },
            "size": count}

        ret = es.search(index='items', body=body)
        return ret['hits']['hits']

print(ContentBasedRecommender.get_mlt_recs_for_items([370438112], 1))