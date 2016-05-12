from elasticsearcher import es
from recommenders.abstract_recommender import AbstractRecommender
from rediser import redis


class ContextualRecommender(AbstractRecommender):
    @classmethod
    def redis_key(cls, cluster_id):
        return 'contextual_predictions:' + str(cluster_id)

    @classmethod
    def get_most_popular_articles_global(cls, time_interval, count=50):
        pop_dictie = {}
        key = cls.redis_key_for_global(time_interval)

        pop_articles = redis.zrange(key, 0, count, withscores=True, desc=True)
        if len(pop_articles) == 0:
            return {}

        max_score = pop_articles[0][1]
        for article_id, score in pop_articles:
            pop_dictie[int(article_id.decode('utf-8'))] = (float(score) / max_score) + 1

        return pop_dictie

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
