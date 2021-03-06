from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.popularity_recommender import PopularityRecommender


class AttributePopularityStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(recommendation_req, user_id, user_visits, attribute, attribute_value, time_interval='4h'):
        pop_recommendations = PopularityRecommender.get_most_popular_articles_attribute(time_interval, attribute,
                                                                                        attribute_value)
        recommendations = [r for r in pop_recommendations if r not in set(user_visits)]
        return recommendations
