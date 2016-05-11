from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.collaborative_recommender import CollaborativeRecommender


class GlobalCollaborativeStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(user_id, user_visits, time_interval):
        recommendations = CollaborativeRecommender.recommend_to_user(user_id, 20)
        recommendations = [r for r in recommendations if r not in user_visits]
        return recommendations
