import random

from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.collaborative_recommender import CollaborativeRecommender
from recommenders.popularity_recommender import PopularityRecommender


class AttributeCollaborativePopularityRecencyStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(user_id, attribute, attribute_value, time_interval='4h'):
        user_visits = RecommenderStrategy.user_impressions(user_id)
        pop_recommendations = PopularityRecommender.get_most_popular_articles_attribute(time_interval, attribute,
                                                                                        attribute_value)
        collab_recommendations = CollaborativeRecommender.recommend_to_user(user_id, 20)
        recommendations = [r for r in collab_recommendations if r in pop_recommendations and r not in user_visits]
        random.shuffle(recommendations)
        return recommendations
