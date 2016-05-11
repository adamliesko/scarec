import random

from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.collaborative_recommender import CollaborativeRecommender
from recommenders.recency_recommender import RecencyRecommender
from recommenders.popularity_recommender import PopularityRecommender


class GlobalCollaborativePopularityRecencyStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(user_id, user_visits, time_interval='4h'):
        pop_recommendations = PopularityRecommender.get_most_popular_articles_global(time_interval)
        collab_recommendations = CollaborativeRecommender.recommend_to_user(user_id, 20)
        recency_recommendations = RecencyRecommender.get_most_recent_articles_global()
        recommendations = [r for r in collab_recommendations if
                           r in recency_recommendations and r in pop_recommendations and r not in user_visits]
        random.shuffle(recommendations)
        return recommendations
