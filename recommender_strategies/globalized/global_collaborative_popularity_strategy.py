from recommender_strategies.recommender_strategy import RecommenderStrategy
from recommenders.collaborative_recommender import CollaborativeRecommender
from recommenders.popularity_recommender import PopularityRecommender


class GlobalCollaborativePopularityStrategy(RecommenderStrategy):
    @staticmethod
    def recommend_to_user(user_id, user_visits, time_interval='4h'):
        collab_recommendations = CollaborativeRecommender.recommend_to_user(user_id, 20)
        pop_recommendations = PopularityRecommender.get_most_popular_articles_global(time_interval)
        recommendations = [r for r in collab_recommendations if r in pop_recommendations and r not in user_visits]
        return recommendations
