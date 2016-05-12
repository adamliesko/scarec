from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.collaborative_recommender import CollaborativeRecommender


class GlobalCollaborativeStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(recommendation_req, user_id, user_visits, time_interval):
        recommendations = CollaborativeRecommender.recommend_to_user(user_id, 10)
        recommendations = [r for r in recommendations if r not in set(user_visits)]
        return recommendations
