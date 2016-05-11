import random

from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.collaborative_recommender import CollaborativeRecommender
from recommenders.recency_recommender import RecencyRecommender


class GlobalCollaborativeRecencyStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(user_id, user_visits, _time_interval):
        user_visits = RecommenderStrategy.user_impressions(user_id)
        collab_recommendations = CollaborativeRecommender.recommend_to_user(user_id, 20)
        recency_recommendations = RecencyRecommender.get_most_recent_articles_global()
        recommendations = [r for r in collab_recommendations if r in recency_recommendations and r not in user_visits]
        random.shuffle(recommendations)
        return recommendations
