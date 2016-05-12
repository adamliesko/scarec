from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.popularity_recommender import PopularityRecommender


class GlobalPopularityStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(recommendation_req, user_id, user_visits, time_interval='4h'):
        pop_recommendations = PopularityRecommender.get_most_popular_articles_global(time_interval)
        recommendations = [r for r, score in pop_recommendations.items() if r not in set(user_visits)]
        return recommendations
