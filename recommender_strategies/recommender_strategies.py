# classes are dynamically / meta-programatically used in recommender strategies/facade - tag it?
# ATRIBUTE BASED

from recommender_strategies.attribute.attribute_collaborative_popularity_strategy import \
    AttributeCollaborativePopularityRecencyStrategy
from recommender_strategies.attribute.attribute_collaborative_popularity_recency_strategy import \
    AttributeCollaborativePopularityRecencyStrategy
from recommender_strategies.attribute.attribute_popularity_recency_strategy import AttributePopularityRecencyStrategy
from recommender_strategies.attribute.attribute_popularity_strategy import AttributePopularityStrategy

# GLOBAL BASED

from recommender_strategies.globalized.global_collaborative_popularity_recency_strategy import \
    GlobalCollaborativePopularityRecencyStrategy
from recommender_strategies.globalized.global_collaborative_recency_strategy import GlobalCollaborativeRecencyStrategy
from recommender_strategies.globalized.global_collaborative_popularity_strategy import \
    GlobalCollaborativePopularityStrategy
from recommender_strategies.globalized.global_collaborative_strategy import GlobalCollaborativeStrategy
from recommender_strategies.globalized.global_popularity_recency_strategy import GlobalPopularityRecencyStrategy
from recommender_strategies.globalized.global_popularity_strategy import GlobalPopularityStrategy
from recommender_strategies.globalized.global_recency_strategy import GlobalRecencyStrategy
