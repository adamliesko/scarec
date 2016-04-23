from rediser import redis
from recommenders.popularity_recommender import PopularityRecommender


class RecommenderFacade:
    @classmethod
    def recommend_to_user(cls, recommendation_rec, algorithm, time_interval):
        user_id = recommendation_rec.user_id
        data = recommendation_rec.content
        limit = recommendation_rec.limit

        if algorithm == 'popular_global':
            recommendations = cls.recommend_popular_global(time_interval)
        else:
            attribute = algorithm.split('_')[-1]
            attribute_value = data[attribute]
            recommendations = cls.recommend_popular_attribute(attribute, attribute_value, time_interval)

        if user_id:
            user_visits = redis.zrange('user_impressions:' + str(user_id), 0, -1)
            user_visits = [int(visit) for visit in user_visits]
            final_recommendations = [int(rec) for rec in recommendations if rec not in set(user_visits)]
        else:
            final_recommendations = [int(rec) for rec in recommendations]

        if len(final_recommendations) > 0:
            recommended_articles = final_recommendations[0:limit]
        else:
            global_most_popular = cls.recommend_popular_global(time_interval)
            recommended_articles = [int(r) for r in global_most_popular.keys()[0:limit]]

        return recommended_articles

    @classmethod
    def recommend_popular_global(cls, ti):
        return PopularityRecommender.current_most_popular_global(ti)

    @classmethod
    def recommend_popular_attribute(cls, attribute, attribute_value, ti):
        return PopularityRecommender.current_most_popular_attribute(ti, attribute, attribute_value)

    # TODO: finish collaborative filtering
    @classmethod
    def recommend_collaborative_to_user(cls, user_id):
        pass

    # TODO: finish collaborative filtering
    @classmethod
    def recommend_collaborative_from_set(cls, user_id, articles):
        pass
