from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.recency_recommender import RecencyRecommender


class GlobalRecencyStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(user_id, time_interval):
        recommendations = RecencyRecommender.get_most_recent_articles_global()
        user_visits = RecommenderStrategy.user_impressions(user_id)
        recommendations = [r for r, score in recommendations.items() if r not in user_visits]
        return recommendations
