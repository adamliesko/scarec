from clustering.clustering_model import ClusteringModel
from classification.random_forester import RandomForester
from recommenders.collaborative_recommender import CollaborativeRecommender


class MLInitializer:
    @staticmethod
    def init():
        ClusteringModel.load_model()
        RandomForester.load_models()
        CollaborativeRecommender.load_model()
