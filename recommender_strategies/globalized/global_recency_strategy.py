from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.recency_recommender import RecencyRecommender


class GlobalRecencyStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(recommendation_req, user_id, user_visits, time_interval='4h'):
        recommendations = RecencyRecommender.get_most_recent_articles_global()
        recommendations = [r for r, score in recommendations.items() if r not in set(user_visits)]
        return recommendations
