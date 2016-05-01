from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.popularity_recommender import PopularityRecommender


class AttributePopularityStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(user_id, attribute, attribute_value, time_interval='4h'):
        user_visits = RecommenderStrategy.user_impressions(user_id)
        pop_recommendations = PopularityRecommender.get_most_popular_articles_attribute(time_interval, attribute,
                                                                                        attribute_value)
        recommendations = [r for r, score in pop_recommendations if r not in user_visits]
        return recommendations
