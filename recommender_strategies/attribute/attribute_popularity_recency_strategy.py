from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.popularity_recommender import PopularityRecommender
from recommenders.recency_recommender import RecencyRecommender
from recommender_strategies.aggregators.product_aggregator import ProductAggregator


class AttributePopularityRecencyStrategy(RecommenderStrategy):
    POP_WEIGHT = 3
    RECENCY_WEIGHT = 1

    @classmethod
    def recommend_to_user(cls, recommendation_req, user_id, user_visits, attribute, attribute_value,
                          time_interval='4h'):
        pop_recommendations = PopularityRecommender.get_most_popular_articles_attribute(time_interval, attribute,
                                                                                        attribute_value)
        recency_recommendations = RecencyRecommender.get_most_recent_articles_global()  # default is 100
        recommendations = ProductAggregator.merge_recommendations(pop_recommendations, recency_recommendations,
                                                                  weights=[cls.POP_WEIGHT, cls.RECENCY_WEIGHT])
        recommendations = [r for r in recommendations if r not in set(user_visits)]
        return recommendations
