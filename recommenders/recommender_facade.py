from models.impression import Impression
import random
# classes are dynamically / meta-programatically used in recommender strategies/facade - tag it?

# UNUSED, MISSING IMPLEMENTATION
# from recommender_strategies.attribute.attribute_collaborative_popularity_strategy import \
#     AttributeCollaborativePopularityRecencyStrategy
# from recommender_strategies.attribute.attribute_collaborative_popularity_recency_strategy import \
#     AttributeCollaborativePopularityRecencyStrategy
# from recommender_strategies.globalized.global_collaborative_popularity_recency_strategy import \
#     GlobalCollaborativePopularityRecencyStrategy
# from recommender_strategies.globalized.global_collaborative_recency_strategy import GlobalCollaborativeRecencyStrategy

# ATTRIBUTE BASED

from recommender_strategies.attribute.attribute_popularity_recency_strategy import AttributePopularityRecencyStrategy
from recommender_strategies.attribute.attribute_popularity_strategy import AttributePopularityStrategy

# GLOBAL BASED


from recommender_strategies.globalized.global_collaborative_popularity_strategy import \
    GlobalCollaborativePopularityStrategy
from recommender_strategies.globalized.global_collaborative_strategy import GlobalCollaborativeStrategy
from recommender_strategies.globalized.global_popularity_recency_strategy import GlobalPopularityRecencyStrategy
from recommender_strategies.globalized.global_popularity_strategy import GlobalPopularityStrategy
from recommender_strategies.globalized.global_recency_strategy import GlobalRecencyStrategy
from recommender_strategies.globalized.global_popularity_recency_strategy import GlobalPopularityRecencyStrategy
from recommender_strategies.globalized.global_hybrid_popularity_strategy import GlobalHybridPopularityStrategy
from recommender_strategies.globalized.global_context_strategy import GlobalContextStrategy
from recommender_strategies.globalized.global_context_popularity_strategy import GlobalContextPopularityStrategy
from recommender_strategies.globalized.global_hybrid_strategy import GlobalHybridStrategy


class RecommenderFacade:
    # TODO: move this to db storage, make it more dynamic, possibly easier to re-deploy
    ALGORITHM_STRATEGY_MAP = {
        # DISABLE THESE FOR NOW TODO implement them later
        # '1': {'class': 'AttributeCollaborativePopularityRecencyStrategy', 'attr': 'cluster_id'},
        # '2': {'class': 'AttributeCollaborativePopularityRecencyStrategy', 'attr': 'domain_id'},
        # '3': {'class': 'AttributeCollaborativePopularityRecencyStrategy', 'attr': 'publisher_id'},
        # '4': {'class': 'AttributeCollaborativePopularityStrategy', 'attr': 'cluster_id'},
        # '5': {'class': 'AttributeCollaborativePopularityStrategy', 'attr': 'domain_id'},
        # '6': {'class': 'AttributeCollaborativePopularityStrategy', 'attr': 'publisher_id'},

        '30': {'class': 'AttributePopularityRecencyStrategy', 'attr': 'cluster_id'},
        '31': {'class': 'AttributePopularityRecencyStrategy', 'attr': 'domain_id'},
        '32': {'class': 'AttributePopularityRecencyStrategy', 'attr': 'publisher_id'},

        '33': {'class': 'AttributePopularityStrategy', 'attr': 'cluster_id'},
        '34': {'class': 'AttributePopularityStrategy', 'attr': 'domain_id'},
        '35': {'class': 'AttributePopularityStrategy', 'attr': 'publisher_id'},
        '36': {'class': 'AttributePopularityStrategy', 'attr': 'channel_id'},
        '37': {'class': 'AttributePopularityStrategy', 'attr': 'category_id'},

        # '13': 'GlobalCollaborativePopularityRecencyStrategy',
        '14': 'GlobalCollaborativePopularityStrategy',
        # '15': 'GlobalCollaborativeRecencyStrategy',
        '16': 'GlobalCollaborativeStrategy',
        '38': 'GlobalPopularityRecencyStrategy',
        '39': 'GlobalPopularityStrategy',
        '40': 'GlobalRecencyStrategy',
        '7': 'GlobalHybridStrategy',
        '8': 'GlobalHybridPopularityStrategy',
        '9': 'GlobalContextStrategy',
        '10': 'GlobalContextPopularityStrategy'
    }

    BEST_PERFORMING_TIME_INTERVAL = '4h'
    DEFAULT_ALGORITHM_NO = '38'
    ARRAY_ATTRS = ['channel_id', 'category_id']

    @classmethod
    def recommend_to_user(cls, recommendation_req, algorithm_no, time_interval=BEST_PERFORMING_TIME_INTERVAL):
        user_id = recommendation_req.user_id
        limit = recommendation_req.limit

        if int(user_id) != 0 or int(algorithm_no) > 30:  # these are not dependant on user_id
            algorithm = cls.ALGORITHM_STRATEGY_MAP[str(algorithm_no)]
            if isinstance(algorithm, str):
                recommendations = cls.global_based_recommendations(algorithm, recommendation_req, time_interval,
                                                                   user_id)
            else:
                recommendations = cls.attribute_based_recommendations(algorithm, recommendation_req, time_interval,
                                                                      user_id)
            recs_len = len(recommendations)
            recommended_articles = recommendations[0:limit]
            if recs_len < limit:
                recommended_articles = recommendations[0:limit]
                global_most_popular = cls.recommend_to_unknown_user(recommendation_req)[:(limit - recs_len)]
                ([recommended_articles.append(r) for r in list(global_most_popular)])
        else:
            recommended_articles = cls.recommend_to_unknown_user(recommendation_req)
        return recommended_articles[:limit]

    @classmethod
    def attribute_based_recommendations(cls, algorithm, recommendation_req, time_interval, user_id):
        data = recommendation_req.body
        attribute = algorithm['attr']

        if attribute in cls.ARRAY_ATTRS:
            attribute_value = random.choice(data[
                                                attribute])  # pick a random value, TODO: we should probably aggregate this over all the attr vals available
        else:
            attribute_value = data[attribute]
        klazz = algorithm['class']
        StrategyCls = globals()[klazz]  # this is a class ref.
        rec_func = getattr(StrategyCls, 'recommend_to_user')  # this is a closure
        recommendations = rec_func(recommendation_req, user_id, cls.user_visits(user_id), attribute, attribute_value,
                                   time_interval)
        return recommendations

    @classmethod
    def global_based_recommendations(cls, algorithm, recommendation_req, time_interval, user_id):
        StrategyCls = globals()[algorithm]  # class ref.
        rec_func = getattr(StrategyCls, "recommend_to_user")  # closure
        recommendations = rec_func(recommendation_req, user_id, cls.user_visits(user_id), time_interval)
        return recommendations

    @classmethod
    def user_visits(cls, user_id):
        return Impression.user_impressions(user_id)

    @classmethod
    def recommend_to_unknown_user(cls, recommendation_req):
        return GlobalPopularityRecencyStrategy.recommend_to_user(recommendation_req, 0, [],
                                                                 cls.BEST_PERFORMING_TIME_INTERVAL)
