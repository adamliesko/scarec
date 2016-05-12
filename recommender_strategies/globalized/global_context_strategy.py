from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.contextual_recommender import ContextualRecommender


class GlobalContextStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(recommendation_req, user_id, user_visits, time_interval):
        ctx_recs = ContextualRecommender.get_recommendations(recommendation_req.cluster_id)
        recommendations = [r for r, score in ctx_recs.items() if r not in set(user_visits)]

        return recommendations
