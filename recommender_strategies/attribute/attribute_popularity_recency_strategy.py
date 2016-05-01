from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.popularity_recommender import PopularityRecommender
from recommenders.recency_recommender import RecencyRecommender
from recommender_strategies.aggregators.product_aggregator import ProductAggregator


class AttributePopularityRecencyStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(user_id, attribute, attribute_value, time_interval='4h'):
        user_visits = RecommenderStrategy.user_impressions(user_id)
        pop_recommendations = PopularityRecommender.get_most_popular_articles_attribute(time_interval, attribute,
                                                                                        attribute_value)
        recency_recommendations = RecencyRecommender.get_most_recent_articles_global(20)
        recommendations = ProductAggregator.merge_recommendations([3, 1],
                                                                  [pop_recommendations, recency_recommendations])
        recommendations = [r for r, score in recommendations if r not in user_visits]
        return recommendations
