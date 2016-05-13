from elasticsearcher import es
from recommenders.abstract_recommender import AbstractRecommender
from rediser import redis


class ContextualRecommender(AbstractRecommender):
    @classmethod
    def redis_key(cls, cluster_id):
        return 'contextual_predictions:' + str(cluster_id)

    @classmethod
    def get_recommendations(cls, cluster_id, count=50):
        recs_dictie = {}
        key = cls.redis_key(cluster_id)
        rec_articles = redis.zrange(key, 0, count, withscores=True, desc=True)

        if len(rec_articles) == 0:
            return {}

        max_score = rec_articles[0][1]
        for article_id, score in rec_articles:
            recs_dictie[int(article_id.decode('utf-8'))] = (float(score) / max_score) + 1

        return recs_dictie

    @classmethod
    def add_prediction(cls, item_id, score, cluster_id):
        key = cls.redis_key(cluster_id)
        redis.zadd(key, item_id, score)
