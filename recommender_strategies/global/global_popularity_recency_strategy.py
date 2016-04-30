from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.popularity_recommender import PopularityRecommender
from recommenders.recency_recommender import RecencyRecommender
from recommender_strategies.aggregators.product_aggregator import ProductAggregator


class GlobalPopularityRecencyStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(user_id, time_interval='4h'):
        pop_recommendations = PopularityRecommender.get_most_popular_articles_global(time_interval)
        recency_recommendations = RecencyRecommender.get_most_recent_articles_global()

        recommendations = ProductAggregator.merge_recommendations([2, 1],
                                                                  [pop_recommendations, recency_recommendations])
        user_visits = RecommenderStrategy.user_impressions(user_id)
        final_recommendations = [r for r, score in recommendations if r not in user_visits]
        return final_recommendations
