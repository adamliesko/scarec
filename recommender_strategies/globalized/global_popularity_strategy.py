from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.popularity_recommender import PopularityRecommender


class GlobalPopularityStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(user_id, time_interval='4h'):
        pop_recommendations = PopularityRecommender.get_most_popular_articles_global(time_interval)
        user_visits = RecommenderStrategy.user_impressions(user_id)
        recommendations = [r for r, score in pop_recommendations.items() if r not in user_visits]
        return recommendations
