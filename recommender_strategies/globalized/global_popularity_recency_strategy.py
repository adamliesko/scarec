from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.popularity_recommender import PopularityRecommender
from recommenders.recency_recommender import RecencyRecommender
from recommender_strategies.aggregators.product_aggregator import ProductAggregator


class GlobalPopularityRecencyStrategy(RecommenderStrategy):
    POP_WEIGHT = 3
    RECENCY_WEIGHT = 1

    @classmethod
    def recommend_to_user(cls, user_id, user_visits,time_interval='4h'):
        pop_recommendations = PopularityRecommender.get_most_popular_articles_global(time_interval)
        recency_recommendations = RecencyRecommender.get_most_recent_articles_global()

        recommendations = ProductAggregator.merge_recommendations([cls.POP_WEIGHT, cls.RECENCY_WEIGHT],
                                                                  [pop_recommendations, recency_recommendations])
        final_recommendations = sorted([r for r, score in recommendations.items() if r not in user_visits])
        return final_recommendations
