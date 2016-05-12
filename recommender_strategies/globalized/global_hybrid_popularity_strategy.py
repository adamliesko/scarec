from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.collaborative_recommender import CollaborativeRecommender
from recommenders.contextual_recommender import ContextualRecommender
from recommenders.popularity_recommender import PopularityRecommender

from recommender_strategies.aggregators.product_aggregator import ProductAggregator


class GlobalHybridPopularityStrategy(RecommenderStrategy):
    POP_WEIGHT = 1
    CTX_WEIGHT = 2
    ALS_WEIGHT = 4

    @classmethod
    def recommend_to_user(cls, recommendation_req, user_id, user_visits, time_interval='4h'):
        pop_recommendations = PopularityRecommender.get_most_popular_articles_global(time_interval)
        ctx_recommendations = ContextualRecommender.get_recommendations(recommendation_req.cluster_id)
        als_recommendations = CollaborativeRecommender.recommend_to_user(user_id)

        recommendations = ProductAggregator.merge_recommendations(pop_recommendations, ctx_recommendations,
                                                                  als_recommendations,
                                                                  weights=[cls.POP_WEIGHT, cls.CTX_WEIGHT,
                                                                           cls.ALS_WEIGHT])

        final_recommendations = [r for r in recommendations if r not in set(user_visits)]
        return final_recommendations
