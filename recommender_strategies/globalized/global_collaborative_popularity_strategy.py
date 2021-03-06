from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.collaborative_recommender import CollaborativeRecommender
from recommenders.popularity_recommender import PopularityRecommender

from recommender_strategies.aggregators.product_aggregator import ProductAggregator


class GlobalCollaborativePopularityStrategy(RecommenderStrategy):
    POP_WEIGHT = 1
    ALS_WEIGHT = 2

    @classmethod
    def recommend_to_user(cls, recommendation_req, user_id, user_visits, time_interval='4h'):
        pop_recommendations = PopularityRecommender.get_most_popular_articles_global(time_interval)
        als_recommendations = CollaborativeRecommender.recommend_to_user(user_id)

        recommendations = ProductAggregator.merge_recommendations(pop_recommendations, als_recommendations,
                                                                  weights=[cls.POP_WEIGHT, cls.ALS_WEIGHT])
        final_recommendations = [r for r in recommendations if r not in set(user_visits)]
        return final_recommendations
