from recommenders.popularity_recommender import PopularityRecommender
from recommender_strategies import recommender_strategies

class RecommenderFacade:
    ALGORITHM_STRATEGY_MAP = {
        '1': {'class': 'AttributeCollaborativePopularityRecencyStrategy', 'attr': 'cluster_id'},
        '2': {'class': 'AttributeCollaborativePopularityRecencyStrategy', 'attr': 'domain_id'},
        '3': {'class': 'AttributeCollaborativePopularityRecencyStrategy', 'attr': 'publisher_id'},
        '4': {'class': 'AttributeCollaborativePopularityStrategy', 'attr': 'cluster_id'},
        '5': {'class': 'AttributeCollaborativePopularityStrategy', 'attr': 'domain_id'},
        '6': {'class': 'AttributeCollaborativePopularityStrategy', 'attr': 'publisher_id'},

        '7': {'class': 'AttributePopularityRecencyStrategy', 'attr': 'cluster_id'},
        '8': {'class': 'AttributePopularityRecencyStrategy', 'attr': 'domain_id'},
        '9': {'class': 'AttributePopularityRecencyStrategy', 'attr': 'publisher_id'},

        '10': {'class': 'AttributePopularityStrategy', 'attr': 'cluster_id'},
        '11': {'class': 'AttributePopularityStrategy', 'attr': 'domain_id'},
        '12': {'class': 'AttributePopularityStrategy', 'attr': 'publisher_id'},
        '20': {'class': 'AttributePopularityStrategy', 'attr': 'channel_id'},
        '21': {'class': 'AttributePopularityStrategy', 'attr': 'category_id'},

        '13': 'GlobalCollaborativePopularityRecencyStrategy',
        '14': 'GlobalCollaborativePopularityStrategy',
        '15': 'GlobalCollaborativeRecencyStrategy',
        '16': 'GlobalCollaborativeStrategy',
        '17': 'GlobalPopularityRecencyStrategy',
        '18': 'GlobalPopularityStrategy',
        '19': 'GlobalRecencyStrategy'
    }

    BEST_PERFORMING_TIME_INTERVAL = '4'

    @classmethod
    def recommend_to_user(cls, recommendation_req, algorithm_no, time_interval=BEST_PERFORMING_TIME_INTERVAL):
        user_id = recommendation_req.user_id
        limit = recommendation_req.limit

        if int(user_id) != 0:
            algorithm = cls.ALGORITHM_STRATEGY_MAP[str(algorithm_no)]
            if isinstance(algorithm, str):
                recommendations = cls.global_based_recommendations(algorithm, time_interval, user_id)
            else:
                recommendations = cls.attribute_based_recommendations(algorithm, recommendation_req, time_interval, user_id)

            if len(recommendations) >= limit:
                recommended_articles = [int(r) for r in recommendations[0:limit].keys()]
            else:
                global_most_popular = PopularityRecommender.get_most_popular_articles_global('4h', limit)
                recommended_articles = [int(r) for r in list(global_most_popular.keys())]
        else:
            global_most_popular = PopularityRecommender.get_most_popular_articles_global('4h', limit)
            recommended_articles = [int(r) for r in list(global_most_popular.keys())]
        return recommended_articles

    @classmethod
    def attribute_based_recommendations(cls, algorithm, recommendation_req, time_interval, user_id):
        data = recommendation_req.content
        attribute = algorithm['attr']
        attribute_value = data[attribute]
        klazz = algorithm['class']
        StrategyCls = globals()[klazz]
        rec_func = getattr(StrategyCls, 'recommend_to_user')
        recommendations = rec_func(user_id, attribute, attribute_value, time_interval)
        return recommendations

    @classmethod
    def global_based_recommendations(cls, algorithm, time_interval, user_id):
        StrategyCls = globals()[algorithm]
        rec_func = getattr(StrategyCls, "recommend_to_user")
        recommendations = rec_func(user_id, time_interval)
        return recommendations
